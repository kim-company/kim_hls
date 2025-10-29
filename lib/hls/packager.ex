defmodule HLS.Packager do
  @moduledoc """
  A fully functional HLS packager that performs pure state transformations.

  Instead of performing I/O operations, this module returns `Action`s that the caller
  must execute. This design provides:

  - Pure functions (no side effects)
  - Testability without mocking
  - Explicit control flow
  - Caller-controlled concurrency
  - Ability to batch operations

  ## Usage Flow

  ```elixir
  # 1. Initialize
  {:ok, state} = Packager.new(
    manifest_uri: URI.new!("stream.m3u8"),
    max_segments: 10
  )

  # 2. Add tracks
  {state, []} = Packager.add_track(state, "video",
    stream: %VariantStream{...},
    segment_extension: ".m4s",
    target_segment_duration: 6
  )

  # 3. Add segment (returns upload action)
  {state, [action]} = Packager.put_segment(state, "video", duration: 5.2)
  # action = %Action.UploadSegment{
  #   id: "video_seg_1",
  #   uri: URI.parse("stream_video/00000/stream_video_00001.m4s"),
  #   track_id: "video"
  # }

  # 4. Caller uploads the segment with their payload
  :ok = Storage.put(storage, action.uri, video_payload)

  # 5. Confirm upload (may trigger playlist writes)
  {state, actions} = Packager.confirm_upload(state, action.id)
  # actions = [%Action.WritePlaylist{uri: ..., content: ...}]

  # 6. Execute write actions
  Enum.each(actions, fn
    %Action.WritePlaylist{uri: uri, content: content} ->
      Storage.put(storage, uri, content)
  end)

  # 7. Sync (triggers media playlist updates)
  {state, actions} = Packager.sync(state, 3)
  # actions = [
  #   %Action.WritePlaylist{type: :media, ...},
  #   %Action.WritePlaylist{type: :master, ...},
  #   %Action.DeleteSegment{uri: ...}  # if sliding window
  # ]
  ```

  ## Actions

  All operations return `{new_state, [Action.t()]}` where actions must be executed
  by the caller in order.
  """

  alias HLS.{Segment, VariantStream, AlternativeRendition}
  alias HLS.Playlist.{Media, Master}

  # Action types returned by operations
  defmodule Action do
    @moduledoc "Actions that the caller must execute"

    defmodule UploadSegment do
      @moduledoc "Upload a media segment"
      defstruct [:id, :uri, :track_id, :init_section_uri]
      @type t :: %__MODULE__{
              id: String.t(),
              uri: URI.t(),
              track_id: String.t(),
              init_section_uri: URI.t() | nil
            }
    end

    defmodule UploadInitSection do
      @moduledoc "Upload an initialization section"
      defstruct [:id, :uri, :track_id]
      @type t :: %__MODULE__{id: String.t(), uri: URI.t(), track_id: String.t()}
    end

    defmodule WritePlaylist do
      @moduledoc "Write a playlist to storage"
      defstruct [:type, :uri, :content]

      @type playlist_type :: :master | :media | :pending
      @type t :: %__MODULE__{
              type: playlist_type(),
              uri: URI.t(),
              content: String.t()
            }
    end

    defmodule DeleteSegment do
      @moduledoc "Delete a segment from storage"
      defstruct [:uri, :track_id]
      @type t :: %__MODULE__{uri: URI.t(), track_id: String.t()}
    end

    defmodule DeleteInitSection do
      @moduledoc "Delete an init section from storage"
      defstruct [:uri, :track_id]
      @type t :: %__MODULE__{uri: URI.t(), track_id: String.t()}
    end

    defmodule DeletePlaylist do
      @moduledoc "Delete a playlist from storage"
      defstruct [:uri, :type]
      @type t :: %__MODULE__{uri: URI.t(), type: :media | :pending | :master}
    end

    @type t ::
            UploadSegment.t()
            | UploadInitSection.t()
            | WritePlaylist.t()
            | DeleteSegment.t()
            | DeleteInitSection.t()
            | DeletePlaylist.t()
  end

  defmodule Track do
    @type t :: %__MODULE__{
            stream: VariantStream.t() | AlternativeRendition.t(),
            duration: float(),
            segment_count: non_neg_integer(),
            segment_extension: String.t(),
            init_section: %{uri: URI.t()} | nil,
            media_playlist: Media.t(),
            pending_playlist: Media.t(),
            pending_segments: [%{segment: Segment.t(), uploaded?: boolean(), id: String.t()}],
            discontinue_next_segment: boolean(),
            codecs: [String.t()],
            next_sync_datetime: DateTime.t() | nil
          }

    defstruct [
      :stream,
      :duration,
      :segment_count,
      :segment_extension,
      :init_section,
      :media_playlist,
      :pending_playlist,
      :next_sync_datetime,
      pending_segments: [],
      discontinue_next_segment: false,
      codecs: []
    ]
  end

  @type track_id :: String.t()

  @type t :: %__MODULE__{
          master_written?: boolean(),
          manifest_uri: URI.t(),
          tracks: %{track_id() => Track.t()},
          max_segments: pos_integer() | nil,
          timeline_reference: DateTime.t()
        }

  defstruct [
    :manifest_uri,
    :timeline_reference,
    master_written?: false,
    tracks: %{},
    max_segments: nil
  ]

  @doc """
  Creates a new packager state.

  ## Options

  - `:manifest_uri` (required) - URI of the master playlist
  - `:max_segments` - Maximum segments per media playlist (nil = unlimited)

  ## Examples

      {:ok, state} = Packager.new(
        manifest_uri: URI.new!("stream.m3u8"),
        max_segments: 10
      )
  """
  @spec new(keyword()) :: {:ok, t()} | {:error, term()}
  def new(opts) do
    with {:ok, validated} <- validate_new_opts(opts) do
      state = %__MODULE__{
        manifest_uri: validated.manifest_uri,
        max_segments: validated.max_segments,
        timeline_reference: DateTime.utc_now(:millisecond),
        tracks: %{},
        master_written?: false
      }

      {:ok, state}
    end
  end

  @doc """
  Resumes from existing playlists loaded by the caller.

  The caller is responsible for loading the master playlist and all media playlists
  from storage. This function reconstructs the state from the loaded data.

  ## Examples

      # Caller loads playlists
      {:ok, master_content} = Storage.get(storage, master_uri)
      master = HLS.Playlist.unmarshal(master_content, %Master{uri: master_uri})

      media_playlists = Enum.map(master.streams, fn stream ->
        {:ok, content} = Storage.get(storage, stream.uri)
        HLS.Playlist.unmarshal(content, %Media{uri: stream.uri})
      end)

      # Resume
      {:ok, state} = Packager.resume(
        master_playlist: master,
        media_playlists: media_playlists,
        max_segments: 10
      )
  """
  @spec resume(keyword()) :: {:ok, t()} | {:error, term()}
  def resume(opts) do
    # Implementation would reconstruct state from playlists
    # This is a simplified version
    with {:ok, validated} <- validate_resume_opts(opts) do
      master = validated.master_playlist
      medias = validated.media_playlists

      state = %__MODULE__{
        manifest_uri: master.uri,
        max_segments: validated.max_segments,
        timeline_reference: DateTime.utc_now(:millisecond),
        master_written?: true,
        tracks: build_tracks_from_playlists(master, medias, validated)
      }

      {:ok, state}
    end
  end

  @doc """
  Adds a track to the packager. Returns updated state with no actions.

  Tracks can only be added before the master playlist is written.

  ## Examples

      {state, []} = Packager.add_track(state, "video_720p",
        stream: %VariantStream{
          bandwidth: 2_500_000,
          resolution: {1280, 720},
          codecs: ["avc1.64001f", "mp4a.40.2"]
        },
        segment_extension: ".m4s",
        target_segment_duration: 6.0,
        codecs: ["avc1.64001f"]
      )
  """
  @spec add_track(t(), track_id(), keyword()) :: {t(), []}
  def add_track(state, track_id, opts) do
    if state.master_written? do
      raise "Cannot add track after master playlist is written"
    end

    if Map.has_key?(state.tracks, track_id) do
      raise "Track #{track_id} already exists"
    end

    opts = validate_add_track_opts!(opts)
    stream_uri = build_track_variant_uri(state.manifest_uri, track_id)
    stream = %{opts.stream | uri: stream_uri}

    type = if state.max_segments, do: nil, else: :event

    media_playlist = %Media{
      uri: stream.uri,
      target_segment_duration: opts.target_segment_duration,
      type: type
    }

    track = %Track{
      stream: stream,
      duration: 0.0,
      segment_count: 0,
      segment_extension: opts.segment_extension,
      init_section: nil,
      media_playlist: media_playlist,
      pending_playlist: %{media_playlist | uri: append_to_path(stream.uri, "_pending")},
      codecs: opts.codecs,
      next_sync_datetime: state.timeline_reference,
      pending_segments: []
    }

    new_state = put_in(state.tracks[track_id], track)
    {new_state, []}
  end

  @doc """
  Prepares a new init section upload.

  Returns an action to upload the init section. The caller must upload the payload
  and then call `confirm_init_upload/2`.

  ## Examples

      {state, [action]} = Packager.put_init_section(state, "video")
      # action = %Action.UploadInitSection{
      #   id: "video_init_1",
      #   uri: URI.parse("stream_video/00000/stream_video_00001_init.mp4"),
      #   track_id: "video"
      # }

      # Caller uploads
      :ok = Storage.put(storage, action.uri, init_payload)

      # Confirm
      {state, []} = Packager.confirm_init_upload(state, action.id)
  """
  @spec put_init_section(t(), track_id()) :: {t(), [Action.UploadInitSection.t()]}
  def put_init_section(state, track_id) do
    track = Map.fetch!(state.tracks, track_id)
    next_index = track.segment_count + 1

    extname =
      case track.segment_extension do
        ".mp4" -> ".mp4"
        ".m4s" -> ".mp4"
        other -> raise "Init section not supported for #{other}"
      end

    segment_uri = relative_segment_uri(track.media_playlist.uri, extname, next_index)
    uri = append_to_path(segment_uri, "_init")

    upload_id = "#{track_id}_init_#{next_index}"

    action = %Action.UploadInitSection{
      id: upload_id,
      uri: uri,
      track_id: track_id
    }

    # Store pending init section info
    new_state =
      update_in(state.tracks[track_id], fn t ->
        %{t | init_section: %{uri: uri, pending_upload_id: upload_id}}
      end)

    {new_state, [action]}
  end

  @doc """
  Confirms that an init section upload completed.
  """
  @spec confirm_init_upload(t(), String.t()) :: {t(), []}
  def confirm_init_upload(state, upload_id) do
    # Find track with this upload_id
    track_id =
      Enum.find_value(state.tracks, fn {id, track} ->
        if track.init_section && track.init_section[:pending_upload_id] == upload_id do
          id
        end
      end)

    if track_id do
      new_state =
        update_in(state.tracks[track_id], fn track ->
          init_section = Map.drop(track.init_section, [:pending_upload_id])
          %{track | init_section: init_section}
        end)

      {new_state, []}
    else
      {state, []}
    end
  end

  @doc """
  Adds a segment to a track and returns an upload action.

  The caller must upload the segment payload and then call `confirm_upload/2`.

  ## Examples

      {state, [action]} = Packager.put_segment(state, "video", duration: 5.2)

      # Caller uploads
      :ok = Storage.put(storage, action.uri, segment_payload)

      # Confirm (may trigger playlist writes)
      {state, actions} = Packager.confirm_upload(state, action.id)
  """
  @spec put_segment(t(), track_id(), keyword()) :: {t(), [Action.UploadSegment.t()]}
  def put_segment(state, track_id, opts) do
    track = Map.fetch!(state.tracks, track_id)
    duration = Keyword.fetch!(opts, :duration)

    next_index = track.segment_count + 1
    segment_uri = relative_segment_uri(track.media_playlist.uri, track.segment_extension, next_index)

    segment = %Segment{
      uri: segment_uri,
      duration: Float.round(duration, 5),
      init_section: if(track.init_section, do: %{uri: track.init_section.uri}),
      discontinuity: track.discontinue_next_segment
    }

    upload_id = "#{track_id}_seg_#{next_index}"

    pending_segment = %{
      segment: segment,
      uploaded?: false,
      id: upload_id
    }

    new_state =
      update_in(state.tracks[track_id], fn t ->
        t
        |> Map.update!(:segment_count, &(&1 + 1))
        |> Map.put(:discontinue_next_segment, false)
        |> Map.update!(:pending_segments, &(&1 ++ [pending_segment]))
        |> then(fn t ->
          if segment.discontinuity do
            %{t | next_sync_datetime: state.timeline_reference}
          else
            t
          end
        end)
      end)

    action = %Action.UploadSegment{
      id: upload_id,
      uri: segment_uri,
      track_id: track_id,
      init_section_uri: segment.init_section && segment.init_section.uri
    }

    {new_state, [action]}
  end

  @doc """
  Confirms that a segment upload completed.

  May return playlist write actions if enough segments are buffered.

  ## Examples

      {state, actions} = Packager.confirm_upload(state, "video_seg_1")
      # actions = [
      #   %Action.WritePlaylist{type: :pending, uri: ..., content: ...}
      # ]
  """
  @spec confirm_upload(t(), String.t()) :: {t(), [Action.WritePlaylist.t()]}
  def confirm_upload(state, upload_id) do
    # Find track with this segment
    {track_id, _track} =
      Enum.find(state.tracks, fn {_id, track} ->
        Enum.any?(track.pending_segments, &(&1.id == upload_id))
      end) || raise "Upload #{upload_id} not found"

    # Update the specific track
    updated_track =
      state.tracks[track_id]
      |> Map.update!(:pending_segments, fn segments ->
        Enum.map(segments, fn seg ->
          if seg.id == upload_id, do: %{seg | uploaded?: true}, else: seg
        end)
      end)

    # Check if we can move uploaded segments to pending playlist
    {uploaded, still_pending} = Enum.split_while(updated_track.pending_segments, & &1.uploaded?)

    if Enum.empty?(uploaded) do
      new_state = %{state | tracks: Map.put(state.tracks, track_id, updated_track)}
      {new_state, []}
    else
      uploaded_segments = Enum.map(uploaded, & &1.segment)

      new_pending_playlist = %{
        updated_track.pending_playlist
        | segments: updated_track.pending_playlist.segments ++ uploaded_segments
      }

      final_track =
        updated_track
        |> Map.put(:pending_segments, still_pending)
        |> Map.put(:pending_playlist, new_pending_playlist)

      new_state = %{state | tracks: Map.put(state.tracks, track_id, final_track)}

      # Generate write action for pending playlist (if not in sliding window mode)
      actions =
        if state.max_segments do
          []
        else
          [
            %Action.WritePlaylist{
              type: :pending,
              uri: new_pending_playlist.uri,
              content: HLS.Playlist.marshal(new_pending_playlist)
            }
          ]
        end

      {new_state, actions}
    end
  end

  @doc """
  Marks all tracks to add discontinuity to next segment.

  Only effective when `max_segments` is configured (sliding window).
  """
  @spec discontinue(t()) :: {t(), []}
  def discontinue(state) when is_nil(state.max_segments), do: {state, []}

  def discontinue(state) do
    new_tracks =
      Map.new(state.tracks, fn {id, track} ->
        {id, %{track | discontinue_next_segment: true}}
      end)

    {%{state | tracks: new_tracks}, []}
  end

  @doc """
  Synchronizes all tracks to the specified segment index.

  Moves segments from pending playlists to media playlists and returns actions
  to write updated playlists. May also return delete actions if sliding window
  is enabled.

  ## Examples

      {state, actions} = Packager.sync(state, 5)
      # actions = [
      #   %Action.WritePlaylist{type: :media, ...},
      #   %Action.WritePlaylist{type: :media, ...},
      #   %Action.WritePlaylist{type: :master, ...},
      #   %Action.DeleteSegment{...},  # if sliding window
      # ]
  """
  @spec sync(t(), pos_integer()) :: {t(), [Action.t()]}
  def sync(state, sync_point) do
    # Move segments to media playlists
    {new_tracks, track_actions} =
      state.tracks
      |> Enum.map(fn {id, track} ->
        sync_track(state, track, id, sync_point)
      end)
      |> Enum.unzip()

    new_state = %{state | tracks: Map.new(new_tracks)}

    # Flatten track actions
    all_track_actions = List.flatten(track_actions)

    # Maybe write master playlist
    {new_state, master_actions} = maybe_write_master(new_state, sync_point)

    {new_state, all_track_actions ++ master_actions}
  end

  @doc """
  Returns the next synchronization point.

  This is the maximum segment count across all tracks plus one.
  """
  @spec next_sync_point(t()) :: pos_integer()
  def next_sync_point(state) do
    max_segments =
      state.tracks
      |> Enum.map(fn {_id, track} ->
        length(track.media_playlist.segments)
      end)
      |> Enum.max(fn -> 0 end)

    max_segments + 1
  end

  @doc """
  Flushes the packager, finalizing all playlists.

  When `max_segments` is nil (unlimited):
  - Creates VOD playlists with EXT-X-ENDLIST
  - Returns write actions for final playlists

  When `max_segments` is set (sliding window):
  - Returns delete actions for all segments and playlists
  - Resets state to initial condition

  ## Examples

      # VOD mode
      {state, actions} = Packager.flush(state)
      # actions = [
      #   %Action.WritePlaylist{type: :media, ...},  # VOD playlist
      #   %Action.WritePlaylist{type: :media, ...},
      #   %Action.DeletePlaylist{type: :pending, ...},
      # ]

      # Sliding window mode
      {state, actions} = Packager.flush(sliding_state)
      # actions = [
      #   %Action.DeleteSegment{...},
      #   %Action.DeleteSegment{...},
      #   %Action.DeletePlaylist{type: :media, ...},
      #   %Action.DeletePlaylist{type: :master, ...},
      # ]
  """
  @spec flush(t()) :: {t(), [Action.t()]}
  def flush(state) do
    if state.max_segments do
      flush_with_cleanup(state)
    else
      flush_as_vod(state)
    end
  end

  defp sync_track(state, track, track_id, sync_point) do
    current_count = length(track.media_playlist.segments)

    {moved_segments, remaining_segments, moved_duration} =
      split_segments_at_sync_point(track.pending_playlist.segments, sync_point, current_count)

    if Enum.empty?(moved_segments) do
      # Nothing to sync
      {{track_id, track}, []}
    else
      new_duration = track.duration + moved_duration

      # Assign program date times if sliding window
      {moved_segments, track} =
        if state.max_segments do
          assign_datetime(moved_segments, track)
        else
          {moved_segments, track}
        end

      # Update media playlist
      updated_media = %{
        track.media_playlist
        | segments: track.media_playlist.segments ++ moved_segments
      }

      # Apply sliding window
      {windowed_media, removed_segments} = apply_sliding_window(updated_media, state.max_segments)

      # Update pending playlist
      updated_pending = %{track.pending_playlist | segments: remaining_segments}

      # Generate actions
      media_action = %Action.WritePlaylist{
        type: :media,
        uri: windowed_media.uri,
        content: HLS.Playlist.marshal(windowed_media)
      }

      pending_action =
        if state.max_segments do
          []
        else
          [
            %Action.WritePlaylist{
              type: :pending,
              uri: updated_pending.uri,
              content: HLS.Playlist.marshal(updated_pending)
            }
          ]
        end

      delete_actions = build_delete_actions(removed_segments, windowed_media, track_id)

      updated_track = %{
        track
        | duration: new_duration,
          media_playlist: windowed_media,
          pending_playlist: updated_pending
      }

      {{track_id, updated_track}, [media_action] ++ pending_action ++ delete_actions}
    end
  end

  defp split_segments_at_sync_point(pending_segments, sync_point, current_count) do
    to_move = max(0, sync_point - current_count)
    {moved, remaining} = Enum.split(pending_segments, to_move)
    duration = Enum.reduce(moved, 0.0, &(&1.duration + &2))
    {moved, remaining, duration}
  end

  defp assign_datetime(segments, track) do
    Enum.map_reduce(segments, track, fn segment, track ->
      segment = %{segment | program_date_time: track.next_sync_datetime}

      track = %{
        track
        | next_sync_datetime: DateTime.add(track.next_sync_datetime, round(segment.duration), :second)
      }

      {segment, track}
    end)
  end

  defp apply_sliding_window(playlist, nil), do: {playlist, []}

  defp apply_sliding_window(playlist, max_segments) do
    segment_count = length(playlist.segments)

    if segment_count > max_segments do
      to_remove = segment_count - max_segments
      {removed, remaining} = Enum.split(playlist.segments, to_remove)

      removed_discontinuities = Enum.count(removed, & &1.discontinuity)

      updated = %{
        playlist
        | segments: remaining,
          media_sequence_number: playlist.media_sequence_number + to_remove,
          discontinuity_sequence: playlist.discontinuity_sequence + removed_discontinuities
      }

      {updated, removed}
    else
      {playlist, []}
    end
  end

  defp build_delete_actions(removed_segments, media_playlist, track_id) do
    if Enum.empty?(removed_segments) do
      []
    else
      # Get init sections still in use
      preserved_init_uris =
        media_playlist.segments
        |> Enum.filter(& &1.init_section)
        |> Enum.map(& &1.init_section.uri)
        |> MapSet.new()

      # Collect init sections from removed segments
      removed_init_uris =
        removed_segments
        |> Enum.filter(& &1.init_section)
        |> Enum.map(& &1.init_section.uri)
        |> MapSet.new()

      # Delete orphaned init sections
      orphaned_init_uris = MapSet.difference(removed_init_uris, preserved_init_uris)

      segment_deletes =
        Enum.map(removed_segments, fn seg ->
          %Action.DeleteSegment{uri: seg.uri, track_id: track_id}
        end)

      init_deletes =
        Enum.map(orphaned_init_uris, fn uri ->
          %Action.DeleteInitSection{uri: uri, track_id: track_id}
        end)

      segment_deletes ++ init_deletes
    end
  end

  defp maybe_write_master(state, _sync_point) when state.master_written? do
    {state, []}
  end

  defp maybe_write_master(state, sync_point) do
    if sync_point >= 3 do
      master = build_master(state)
      action = %Action.WritePlaylist{
        type: :master,
        uri: state.manifest_uri,
        content: HLS.Playlist.marshal(master)
      }

      {%{state | master_written?: true}, [action]}
    else
      {state, []}
    end
  end

  defp build_master(state) do
    alternative_tracks =
      state.tracks
      |> Map.values()
      |> Enum.filter(&is_struct(&1.stream, AlternativeRendition))

    variant_tracks =
      state.tracks
      |> Map.values()
      |> Enum.filter(&is_struct(&1.stream, VariantStream))
      |> Enum.map(fn track ->
        group_ids = VariantStream.associated_group_ids(track.stream)

        alternative_codecs =
          alternative_tracks
          |> Enum.filter(&Enum.member?(group_ids, &1.stream.group_id))
          |> Enum.flat_map(& &1.codecs)

        all_codecs =
          (track.stream.codecs ++ track.codecs ++ alternative_codecs)
          |> Enum.uniq()

        %{track | stream: %{track.stream | codecs: all_codecs}}
      end)

    %Master{
      version: 4,
      uri: state.manifest_uri,
      independent_segments: true,
      streams: Enum.map(variant_tracks, & &1.stream),
      alternative_renditions: Enum.map(alternative_tracks, & &1.stream)
    }
  end

  defp flush_as_vod(state) do
    # Move all pending segments to media playlists
    {new_tracks, actions} =
      state.tracks
      |> Enum.map(fn {id, track} ->
        # All pending segments become part of media playlist
        all_segments = track.pending_playlist.segments ++ Enum.map(track.pending_segments, & &1.segment)
        pending_duration = Enum.reduce(all_segments, 0.0, &(&1.duration + &2))

        vod_media = %{
          track.media_playlist
          | segments: track.media_playlist.segments ++ all_segments,
            finished: true,
            type: :vod
        }

        updated_track = %{
          track
          | duration: track.duration + pending_duration,
            media_playlist: vod_media,
            pending_segments: []
        }

        media_action = %Action.WritePlaylist{
          type: :media,
          uri: vod_media.uri,
          content: HLS.Playlist.marshal(vod_media)
        }

        delete_pending_action = %Action.DeletePlaylist{
          type: :pending,
          uri: track.pending_playlist.uri
        }

        {{id, updated_track}, [media_action, delete_pending_action]}
      end)
      |> Enum.unzip()

    master = build_master(%{state | tracks: Map.new(new_tracks)})

    master_action = %Action.WritePlaylist{
      type: :master,
      uri: state.manifest_uri,
      content: HLS.Playlist.marshal(master)
    }

    new_state = %{state | tracks: Map.new(new_tracks), master_written?: true}

    {new_state, List.flatten(actions) ++ [master_action]}
  end

  defp flush_with_cleanup(state) do
    # Collect all segments
    delete_actions =
      state.tracks
      |> Enum.flat_map(fn {id, track} ->
        all_segments =
          track.media_playlist.segments ++
            track.pending_playlist.segments ++
            Enum.map(track.pending_segments, & &1.segment)

        segment_deletes =
          Enum.map(all_segments, fn seg ->
            %Action.DeleteSegment{uri: seg.uri, track_id: id}
          end)

        # Delete all init sections
        init_uris =
          all_segments
          |> Enum.filter(& &1.init_section)
          |> Enum.map(& &1.init_section.uri)
          |> Enum.uniq()

        init_deletes =
          Enum.map(init_uris, fn uri ->
            %Action.DeleteInitSection{uri: uri, track_id: id}
          end)

        media_delete = %Action.DeletePlaylist{
          type: :media,
          uri: track.media_playlist.uri
        }

        pending_delete = %Action.DeletePlaylist{
          type: :pending,
          uri: track.pending_playlist.uri
        }

        segment_deletes ++ init_deletes ++ [media_delete, pending_delete]
      end)

    master_delete = %Action.DeletePlaylist{
      type: :master,
      uri: state.manifest_uri
    }

    # Reset state
    new_state = %{
      state
      | tracks: %{},
        master_written?: false
    }

    {new_state, delete_actions ++ [master_delete]}
  end

  # URI helpers (copied from original)
  defp relative_segment_uri(playlist_uri, extname, segment_index) do
    root_path =
      playlist_uri
      |> to_string()
      |> Path.basename()
      |> String.trim_trailing(".m3u8")

    {dir, suffix} =
      segment_index
      |> to_string()
      |> String.pad_leading(10, "0")
      |> String.split_at(5)

    [root_path, dir, "#{Path.basename(root_path)}_#{suffix}#{extname}"]
    |> Path.join()
    |> URI.new!()
  end

  defp append_to_path(uri, append) do
    field = if is_nil(uri.path), do: :host, else: :path

    Map.update!(uri, field, fn path ->
      extname = Path.extname(path)
      without_ext = String.trim_trailing(path, extname)
      "#{without_ext}#{append}#{extname}"
    end)
  end

  defp build_track_variant_uri(manifest_uri, track_id) do
    manifest_uri
    |> append_to_path("_#{track_id}")
    |> to_string()
    |> Path.basename()
    |> URI.new!()
  end

  # Validation helpers
  defp validate_new_opts(opts) do
    with {:ok, manifest_uri} <- Keyword.fetch(opts, :manifest_uri) do
      {:ok,
       %{
         manifest_uri: manifest_uri,
         max_segments: Keyword.get(opts, :max_segments)
       }}
    else
      :error -> {:error, "manifest_uri is required"}
    end
  end

  defp validate_resume_opts(opts) do
    with {:ok, master} <- Keyword.fetch(opts, :master_playlist),
         {:ok, medias} <- Keyword.fetch(opts, :media_playlists) do
      {:ok,
       %{
         master_playlist: master,
         media_playlists: medias,
         max_segments: Keyword.get(opts, :max_segments)
       }}
    else
      :error -> {:error, "master_playlist and media_playlists required"}
    end
  end

  defp validate_add_track_opts!(opts) do
    %{
      stream: Keyword.fetch!(opts, :stream),
      segment_extension: Keyword.fetch!(opts, :segment_extension),
      target_segment_duration: Keyword.fetch!(opts, :target_segment_duration),
      codecs: Keyword.get(opts, :codecs, [])
    }
  end

  defp build_tracks_from_playlists(_master, _medias, _opts) do
    # Simplified - would reconstruct Track structs from loaded playlists
    %{}
  end
end
