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
  {state, [action]} = Packager.put_segment(state, "video", duration: 5.2, pts: 0)
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
      @moduledoc "Write a playlist"
      defstruct [:type, :uri, :content]

      @type playlist_type :: :master | :media | :pending
      @type t :: %__MODULE__{
              type: playlist_type(),
              uri: URI.t(),
              content: String.t()
            }
    end

    defmodule DeleteSegment do
      @moduledoc "Delete a segment"
      defstruct [:uri, :track_id]
      @type t :: %__MODULE__{uri: URI.t(), track_id: String.t()}
    end

    defmodule DeleteInitSection do
      @moduledoc "Delete an init section"
      defstruct [:uri, :track_id]
      @type t :: %__MODULE__{uri: URI.t(), track_id: String.t()}
    end

    defmodule DeletePlaylist do
      @moduledoc "Delete a playlist"
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

  defmodule Error do
    @moduledoc """
    RFC 8216 compliance error or stall signal.

    These errors are returned instead of producing non-compliant playlists.
    """

    defexception [:code, :message, :details]

    @type code ::
            :segment_duration_over_target
            | :timing_drift
            | :discontinuity_point_missed
            | :track_timing_mismatch_at_sync
            | :mandatory_track_missing_segment_at_sync
            | :sync_point_skipped
            | :upload_id_not_found
            | :track_conflict
            | :resume_missing_playlist
            | :resume_unexpected_playlist
            | :resume_unknown_segment_extension
            | :resume_inconsistent_playlist
            | :resume_track_not_ready

    @type t :: %__MODULE__{code: code(), message: String.t(), details: map()}
  end

  defmodule Track do
    @type t :: %__MODULE__{
            stream: VariantStream.t() | AlternativeRendition.t(),
            duration: float(),
            segment_count: non_neg_integer(),
            segment_extension: String.t() | nil,
            init_section: %{uri: URI.t()} | nil,
            media_playlist: Media.t(),
            pending_playlist: Media.t(),
            pending_segments: [%{segment: Segment.t(), uploaded?: boolean(), id: String.t()}],
            codecs: [String.t()],
            mandatory?: boolean(),
            last_timestamp_ns: non_neg_integer() | nil,
            last_duration_ns: non_neg_integer() | nil,
            base_pdt: DateTime.t() | nil,
            base_timestamp_ns: non_neg_integer() | nil,
            applied_discontinuities: MapSet.t(),
            resume_incomplete?: boolean()
          }

    defstruct [
      :stream,
      :duration,
      :segment_count,
      :segment_extension,
      :init_section,
      :media_playlist,
      :pending_playlist,
      :base_pdt,
      :base_timestamp_ns,
      :mandatory?,
      :last_timestamp_ns,
      :last_duration_ns,
      :applied_discontinuities,
      resume_incomplete?: false,
      pending_segments: [],
      codecs: []
    ]
  end

  @type track_id :: String.t()

  @type t :: %__MODULE__{
          master_written?: boolean(),
          manifest_uri: URI.t(),
          tracks: %{track_id() => Track.t()},
          max_segments: pos_integer() | nil,
          timeline_reference: DateTime.t(),
          timing_tolerance_ns: non_neg_integer(),
          pending_discontinuities: list(),
          skipped_sync_points: %{pos_integer() => MapSet.t(track_id())}
        }

  defstruct [
    :manifest_uri,
    :timeline_reference,
    :timing_tolerance_ns,
    master_written?: false,
    tracks: %{},
    max_segments: nil,
    pending_discontinuities: [],
    skipped_sync_points: %{}
  ]

  @doc """
  Creates a new packager state.

  ## Options

  - `:manifest_uri` (required) - URI of the master playlist
  - `:max_segments` - Maximum segments per media playlist (nil = unlimited)
  - `:timing_tolerance_ms` - Allowed timing drift in milliseconds (default: 200)

  ## Examples

      {:ok, state} = Packager.new(
        manifest_uri: URI.new!("stream.m3u8"),
        max_segments: 10
      )
  """
  @spec new(keyword()) :: {:ok, t()} | {:error, term()}
  def new(opts) do
    with {:ok, validated} <- validate_new_opts(opts) do
      timeline_reference = DateTime.utc_now(:millisecond)

      state = %__MODULE__{
        manifest_uri: validated.manifest_uri,
        max_segments: validated.max_segments,
        timeline_reference: timeline_reference,
        timing_tolerance_ns: validated.timing_tolerance_ns,
        tracks: %{},
        master_written?: false,
        pending_discontinuities: [],
        skipped_sync_points: %{}
      }

      {:ok, state}
    end
  end

  @doc """
  Resumes from existing playlists loaded by the caller.

  The caller is responsible for loading the master playlist and all media playlists.
  This function reconstructs the state from the loaded data.

  ## Examples

      # Caller loads playlists
      master = load_master_playlist(master_uri)

      media_playlists =
        Enum.map(master.streams, fn stream ->
          load_media_playlist(stream.uri)
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
    timeline_reference = DateTime.utc_now(:millisecond)

    with {:ok, validated} <- validate_resume_opts(opts),
         {:ok, {tracks, common_sync_point}} <-
           build_tracks_from_playlists(Map.put(validated, :timeline_reference, timeline_reference)) do
      master = validated.master_playlist

      state = %__MODULE__{
        manifest_uri: master.uri,
        max_segments: validated.max_segments,
        timeline_reference: timeline_reference,
        timing_tolerance_ns: validated.timing_tolerance_ns,
        master_written?: true,
        tracks: tracks,
        pending_discontinuities: [],
        skipped_sync_points: %{}
      }

      case schedule_discontinuity(state, common_sync_point + 1, :resume, timeline_reference) do
        {:ok, new_state} ->
          {:ok, %{new_state | timeline_reference: timeline_reference}}

        {:error, error, _state} ->
          {:error, error}
      end
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
        codecs: ["avc1.64001f"],
        mandatory?: true
      )
  """
  @spec add_track(t(), track_id(), keyword()) :: {t(), []}
  def add_track(state, track_id, opts) do
    if Map.has_key?(state.tracks, track_id) do
      opts = validate_add_track_opts!(opts)
      {stream, media_playlist_uri} = build_track_stream(state, track_id, opts.stream)
      mandatory? = resolve_mandatory(opts.mandatory?, stream)
      track = state.tracks[track_id]

      cond do
        track.resume_incomplete? ->
          case reconcile_incomplete_track(
                 track,
                 track_id,
                 stream,
                 media_playlist_uri,
                 opts,
                 mandatory?
               ) do
            {:ok, updated_track} ->
              new_state = put_in(state.tracks[track_id], updated_track)
              {new_state, []}

            {:error, error} ->
              raise error
          end

        track_spec_matches?(track, stream, media_playlist_uri, opts, mandatory?) ->
          {state, []}

        true ->
          raise Error,
            code: :track_conflict,
            message: "Track #{track_id} already exists with a different specification",
            details: %{track_id: track_id}
      end
    else
      if state.master_written? do
        raise "Cannot add track after master playlist is written"
      end

      opts = validate_add_track_opts!(opts)

      {stream, media_playlist_uri} = build_track_stream(state, track_id, opts.stream)

      type = if state.max_segments, do: nil, else: :event

      media_playlist = %Media{
        uri: media_playlist_uri,
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
        pending_playlist: %{media_playlist | uri: append_to_path(media_playlist.uri, "_pending")},
        codecs: opts.codecs,
        mandatory?: resolve_mandatory(opts.mandatory?, stream),
        last_timestamp_ns: nil,
        last_duration_ns: nil,
        base_pdt: state.timeline_reference,
        base_timestamp_ns: nil,
        pending_segments: [],
        applied_discontinuities: MapSet.new(),
        resume_incomplete?: false
      }

      new_state = put_in(state.tracks[track_id], track)
      {new_state, []}
    end
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
  The caller must provide PTS in nanoseconds; DTS may be provided for video and
  is used instead of PTS when present.

  Returns an error when RFC 8216 compliance issues are detected.

  ## Examples

      {state, [action]} = Packager.put_segment(state, "video", duration: 5.2, pts: 0)

      # Caller uploads
      :ok = Storage.put(storage, action.uri, segment_payload)

      # Confirm (may trigger playlist writes)
      {state, actions} = Packager.confirm_upload(state, action.id)
  """
  @spec put_segment(t(), track_id(), keyword()) ::
          {t(), [Action.UploadSegment.t()]}
          | {:warning, Error.t(), t()}
          | {:error, Error.t(), t()}
  def put_segment(state, track_id, opts) do
    track = Map.fetch!(state.tracks, track_id)
    duration = Keyword.fetch!(opts, :duration)
    pts = Keyword.fetch!(opts, :pts)
    dts = Keyword.get(opts, :dts)
    timestamp_ns = dts || pts

    case resume_track_ready?(track, track_id) do
      {:error, error} ->
        {:error, error, state}

      :ok ->
        case skip_sync_point_for_track(state, track_id, track.segment_count + 1) do
          {:skip, new_state} ->
            warning = %Error{
              code: :sync_point_skipped,
              message: "Sync point #{track.segment_count + 1} was skipped for track '#{track_id}'",
              details: %{
                track_id: track_id,
                sync_point: track.segment_count + 1
              }
            }

            {:warning, warning, new_state}

          :ok ->
            # RFC 8216: EXTINF duration rounded to nearest integer MUST be <= target duration
            if round(duration) > track.media_playlist.target_segment_duration do
              error = %Error{
                code: :segment_duration_over_target,
                message:
                  "Segment duration #{duration}s exceeds target #{track.media_playlist.target_segment_duration}s (RFC 8216 violation)",
                details: %{
                  track_id: track_id,
                  segment_index: track.segment_count + 1,
                  duration: duration,
                  target_duration: track.media_playlist.target_segment_duration
                }
              }

              {:error, error, state}
            else
              duration_ns = duration_to_ns(duration)

              if track.last_timestamp_ns && track.last_duration_ns do
                expected = track.last_timestamp_ns + track.last_duration_ns

                if abs(timestamp_ns - expected) > state.timing_tolerance_ns do
                  error = %Error{
                    code: :timing_drift,
                    message:
                      "Timing drift detected on track '#{track_id}' at segment #{track.segment_count + 1}",
                    details: %{
                      track_id: track_id,
                      segment_index: track.segment_count + 1,
                      timestamp_ns: timestamp_ns,
                      expected_ns: expected,
                      tolerance_ns: state.timing_tolerance_ns
                    }
                  }

                  {:error, error, state}
                else
                  do_put_segment(
                    state,
                    track_id,
                    track,
                    duration,
                    duration_ns,
                    pts,
                    dts,
                    timestamp_ns
                  )
                end
              else
                do_put_segment(state, track_id, track, duration, duration_ns, pts, dts, timestamp_ns)
              end
            end
        end
    end
  end

  defp do_put_segment(state, track_id, track, duration, duration_ns, pts, dts, timestamp_ns) do
    next_index = track.segment_count + 1

    segment_uri =
      relative_segment_uri(track.media_playlist.uri, track.segment_extension, next_index)

    {discontinuity, state, segment_pdt, base_pdt, base_timestamp_ns} =
      apply_discontinuity_if_needed(state, track_id, next_index, timestamp_ns, track)

    segment = %Segment{
      uri: segment_uri,
      duration: Float.round(duration, 5),
      pts: pts,
      dts: dts,
      init_section: if(track.init_section, do: %{uri: track.init_section.uri}),
      discontinuity: discontinuity,
      program_date_time: segment_pdt
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
        |> Map.put(:base_pdt, base_pdt)
        |> Map.put(:base_timestamp_ns, base_timestamp_ns)
        |> Map.put(:last_timestamp_ns, timestamp_ns)
        |> Map.put(:last_duration_ns, duration_ns)
        |> Map.update!(:pending_segments, &(&1 ++ [pending_segment]))
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
  @spec confirm_upload(t(), String.t()) ::
          {t(), [Action.WritePlaylist.t()]} | {:warning, Error.t(), t()}
  def confirm_upload(state, upload_id) do
    # Find track with this segment
    case Enum.find(state.tracks, fn {_id, track} ->
           Enum.any?(track.pending_segments, &(&1.id == upload_id))
         end) do
      nil ->
        warning = %Error{
          code: :upload_id_not_found,
          message: "Upload #{upload_id} not found",
          details: %{upload_id: upload_id}
        }

        {:warning, warning, state}

      {track_id, _track} ->
        # Update the specific track
        updated_track =
          state.tracks[track_id]
          |> Map.update!(:pending_segments, fn segments ->
            Enum.map(segments, fn seg ->
              if seg.id == upload_id, do: %{seg | uploaded?: true}, else: seg
            end)
          end)

        # Check if we can move uploaded segments to pending playlist
        {uploaded, still_pending} =
          Enum.split_while(updated_track.pending_segments, & &1.uploaded?)

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
  end

  @doc """
  Marks all tracks to add discontinuity to next segment.
  """
  @spec discontinue(t()) :: {t(), []} | {:error, Error.t(), t()}
  def discontinue(state) do
    timeline_reference = DateTime.utc_now(:millisecond)
    sync_point = next_discontinuity_sync_point(state)

    case schedule_discontinuity(state, sync_point, :manual, timeline_reference) do
      {:ok, new_state} ->
        {%{new_state | timeline_reference: timeline_reference}, []}

      {:error, error, _state} ->
        {:error, error, state}
    end
  end

  @doc """
  Skips a sync point across all tracks and schedules a discontinuity.

  Callers should invoke this when a segment is rejected for RFC compliance,
  so all tracks drop the same segment index before continuing.
  """
  @spec skip_sync_point(t(), pos_integer()) :: {t(), []} | {:error, Error.t(), t()}
  def skip_sync_point(state, sync_point) when sync_point > 0 do
    state =
      Map.update(
        state,
        :skipped_sync_points,
        %{sync_point => MapSet.new(Map.keys(state.tracks))},
        fn skips ->
          Map.put_new(skips, sync_point, MapSet.new(Map.keys(state.tracks)))
        end
      )

    timeline_reference = DateTime.utc_now(:millisecond)

    case schedule_discontinuity(state, sync_point, :skip, timeline_reference) do
      {:ok, new_state} ->
        {%{new_state | timeline_reference: timeline_reference}, []}

      {:error, error, _state} ->
        {:error, error, state}
    end
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
  @spec sync(t(), pos_integer()) ::
          {t(), [Action.t()]} | {:warning, [Error.t()], t()} | {:error, Error.t(), t()}
  def sync(state, sync_point) do
    case sync_if_ready(state, sync_point) do
      {:warning, warnings} ->
        {:warning, warnings, state}

      {:error, error} ->
        {:error, error, state}

      :ready ->
        do_sync(state, sync_point)
    end
  end

  defp do_sync(state, sync_point) do
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
  Checks whether selected tracks have enough available segments to sync.

  By default, only variant (video) tracks are checked.
  Returns `{ready?, lagging_track_ids}`.
  """
  @spec sync_ready?(t(), pos_integer(), (track_id(), Track.t() -> boolean())) ::
          {boolean(), [track_id()]}
  def sync_ready?(state, sync_point, track_filter \\ &variant_track?/2) do
    lagging =
      state.tracks
      |> Enum.filter(fn {id, track} -> track_filter.(id, track) end)
      |> Enum.reduce([], fn {id, track}, acc ->
        available =
          length(track.pending_playlist.segments) + length(track.media_playlist.segments)

        if available < sync_point, do: [id | acc], else: acc
      end)
      |> Enum.reverse()

    {lagging == [], lagging}
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

      # Update media playlist
      updated_media = %{
        track.media_playlist
        | segments: track.media_playlist.segments ++ moved_segments
      }

      # Apply sliding window
      {windowed_media, removed_segments, updated_track} =
        apply_sliding_window(
          updated_media,
          state.max_segments,
          track,
          state.pending_discontinuities
        )

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
        updated_track
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

  defp apply_sliding_window(playlist, nil, track, _pending_discontinuities),
    do: {playlist, [], track}

  defp apply_sliding_window(playlist, max_segments, track, pending_discontinuities) do
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

      {updated_track, extra_discontinuities} =
        advance_discontinuities_past_window(
          track,
          pending_discontinuities,
          updated.media_sequence_number
        )

      updated = %{
        updated
        | discontinuity_sequence: updated.discontinuity_sequence + extra_discontinuities
      }

      {updated, removed, updated_track}
    else
      {playlist, [], track}
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
      independent_segments: false,
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
        all_segments =
          track.pending_playlist.segments ++ Enum.map(track.pending_segments, & &1.segment)

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

  defp sync_if_ready(state, sync_point) do
    mandatory_tracks =
      state.tracks
      |> Enum.filter(fn {_id, track} -> track.mandatory? end)

    warnings =
      mandatory_tracks
      |> Enum.flat_map(fn {id, track} ->
        available =
          length(track.pending_playlist.segments) + length(track.media_playlist.segments)

        if available < sync_point do
          [
            %Error{
              code: :mandatory_track_missing_segment_at_sync,
              message:
                "Mandatory track '#{id}' only has #{available} segments but sync point is #{sync_point}",
              details: %{
                track_id: id,
                available_segments: available,
                sync_point: sync_point,
                missing_segments: sync_point - available
              }
            }
          ]
        else
          []
        end
      end)

    if warnings != [] do
      {:warning, warnings}
    else
      case check_track_alignment(state.tracks, mandatory_tracks, sync_point, state) do
        :ok -> :ready
        {:error, error} -> {:error, error}
      end
    end
  end

  defp check_track_alignment(tracks, mandatory_tracks, sync_point, state) do
    mandatory_ids = MapSet.new(Enum.map(mandatory_tracks, &elem(&1, 0)))

    {timestamps, missing} =
      Enum.reduce(tracks, {[], []}, fn {id, track}, {acc, missing_acc} ->
        segment = segment_at_sync_point(track, sync_point)

        cond do
          is_nil(segment) ->
            {acc, missing_acc}

          segment_timestamp_ns(segment) == nil ->
            {acc, [id | missing_acc]}

          true ->
            ts = segment_timestamp_ns(segment)
            {[{id, ts} | acc], missing_acc}
        end
      end)

    case timestamps do
      [] ->
        {:error,
         %Error{
           code: :track_timing_mismatch_at_sync,
           message: "No mandatory tracks have timing information at sync point #{sync_point}",
           details: %{sync_point: sync_point, missing_tracks: missing}
         }}

      [{reference_id, reference_ts} | rest] ->
        mismatches =
          Enum.reduce(rest, [], fn {id, ts}, acc ->
            if abs(ts - reference_ts) > state.timing_tolerance_ns do
              [%{track_id: id, timestamp_ns: ts} | acc]
            else
              acc
            end
          end)

        missing_mandatory =
          missing
          |> Enum.filter(&MapSet.member?(mandatory_ids, &1))

        if mismatches == [] and missing == [] do
          :ok
        else
          {:error,
           %Error{
             code: :track_timing_mismatch_at_sync,
             message: "Tracks are misaligned at sync point #{sync_point}",
             details: %{
               sync_point: sync_point,
               reference_track_id: reference_id,
               reference_timestamp_ns: reference_ts,
               mismatched_tracks: mismatches,
               missing_tracks: missing,
               missing_mandatory_tracks: missing_mandatory,
               tolerance_ns: state.timing_tolerance_ns
             }
           }}
        end
    end
  end

  defp segment_at_sync_point(track, sync_point) do
    segments = track.media_playlist.segments ++ track.pending_playlist.segments
    Enum.at(segments, sync_point - 1)
  end

  defp skip_sync_point_for_track(state, track_id, sync_point) do
    case Map.get(state.skipped_sync_points, sync_point) do
      nil ->
        :ok

      track_ids ->
        if MapSet.member?(track_ids, track_id) do
          remaining = MapSet.delete(track_ids, track_id)

          updated =
            if MapSet.size(remaining) == 0 do
              Map.delete(state.skipped_sync_points, sync_point)
            else
              Map.put(state.skipped_sync_points, sync_point, remaining)
            end

          {:skip, %{state | skipped_sync_points: updated}}
        else
          :ok
        end
    end
  end

  defp apply_discontinuity_if_needed(state, track_id, segment_index, timestamp_ns, track) do
    case find_discontinuity(state.pending_discontinuities, segment_index) do
      nil ->
        {segment_pdt, base_pdt, base_timestamp_ns} =
          compute_segment_pdt(track.base_pdt, track.base_timestamp_ns, timestamp_ns)

        {false, state, segment_pdt, base_pdt, base_timestamp_ns}

      discontinuity ->
        segment_pdt = discontinuity.pdt_reference
        base_pdt = discontinuity.pdt_reference
        base_timestamp_ns = timestamp_ns

        updated_state =
          update_in(state.tracks[track_id].applied_discontinuities, fn applied ->
            MapSet.put(applied, discontinuity.id)
          end)
          |> maybe_pop_discontinuities()

        {true, updated_state, segment_pdt, base_pdt, base_timestamp_ns}
    end
  end

  defp schedule_discontinuity(
         state,
         sync_point,
         reason,
         pdt_reference,
         timestamp_reference_ns \\ nil
       ) do
    if discontinuity_exists?(state.pending_discontinuities, sync_point) do
      {:ok, state}
    else
      passed_tracks =
        state.tracks
        |> Enum.filter(fn {_id, track} -> track.segment_count >= sync_point end)
        |> Enum.map(&elem(&1, 0))

      if passed_tracks != [] do
        error = %Error{
          code: :discontinuity_point_missed,
          message:
            "Discontinuity at segment #{sync_point} cannot be synchronized across all tracks",
          details: %{
            sync_point: sync_point,
            reason: reason,
            tracks_ahead: passed_tracks
          }
        }

        {:error, error, state}
      else
        entry = %{
          id: make_ref(),
          sync_point: sync_point,
          pdt_reference: pdt_reference,
          timestamp_reference_ns: timestamp_reference_ns,
          reason: reason
        }

        {:ok, %{state | pending_discontinuities: state.pending_discontinuities ++ [entry]}}
      end
    end
  end

  defp discontinuity_exists?(discontinuities, sync_point) do
    Enum.any?(discontinuities, fn disc -> disc.sync_point == sync_point end)
  end

  defp find_discontinuity(discontinuities, sync_point) do
    Enum.find(discontinuities, fn disc -> disc.sync_point == sync_point end)
  end

  defp maybe_pop_discontinuities(state) do
    case state.pending_discontinuities do
      [] ->
        state

      [head | rest] ->
        all_applied? =
          Enum.all?(state.tracks, fn {_id, track} ->
            MapSet.member?(track.applied_discontinuities, head.id)
          end)

        if all_applied? do
          maybe_pop_discontinuities(%{state | pending_discontinuities: rest})
        else
          state
        end
    end
  end

  defp advance_discontinuities_past_window(track, pending_discontinuities, media_sequence_number) do
    {applied_ids, count, updated_track} =
      pending_discontinuities
      |> Enum.reduce({track.applied_discontinuities, 0, track}, fn disc,
                                                                   {applied, acc, acc_track} ->
        if MapSet.member?(applied, disc.id) do
          {applied, acc, acc_track}
        else
          if disc.sync_point <= media_sequence_number do
            updated_track =
              acc_track
              |> Map.put(:base_pdt, disc.pdt_reference)
              |> Map.put(:base_timestamp_ns, disc.timestamp_reference_ns)

            {MapSet.put(applied, disc.id), acc + 1, updated_track}
          else
            {applied, acc, acc_track}
          end
        end
      end)

    {%{updated_track | applied_discontinuities: applied_ids}, count}
  end

  defp next_discontinuity_sync_point(state) do
    state.tracks
    |> Enum.map(fn {_id, track} -> track.segment_count end)
    |> Enum.max(fn -> 0 end)
    |> Kernel.+(1)
  end

  defp segment_timestamp_ns(%Segment{dts: dts, pts: pts}) do
    dts || pts
  end

  defp compute_segment_pdt(base_pdt, base_timestamp_ns, timestamp_ns) when is_nil(base_pdt) do
    {nil, nil, base_timestamp_ns || timestamp_ns}
  end

  defp compute_segment_pdt(base_pdt, base_timestamp_ns, timestamp_ns)
       when is_nil(base_timestamp_ns) do
    {base_pdt, base_pdt, timestamp_ns}
  end

  defp compute_segment_pdt(base_pdt, base_timestamp_ns, timestamp_ns) do
    offset_ns = timestamp_ns - base_timestamp_ns
    {DateTime.add(base_pdt, offset_ns, :nanosecond), base_pdt, base_timestamp_ns}
  end

  defp duration_to_ns(duration) do
    round(duration * 1_000_000_000)
  end

  defp timing_tolerance_to_ns(tolerance_ms) when is_integer(tolerance_ms) do
    tolerance_ms * 1_000_000
  end

  defp resolve_mandatory(nil, stream), do: is_struct(stream, VariantStream)
  defp resolve_mandatory(mandatory?, _stream), do: mandatory?

  defp variant_track?(_track_id, track) do
    is_struct(track.stream, VariantStream)
  end

  # Validation helpers
  defp validate_new_opts(opts) do
    with {:ok, manifest_uri} <- Keyword.fetch(opts, :manifest_uri) do
      {:ok,
       %{
         manifest_uri: manifest_uri,
         max_segments: Keyword.get(opts, :max_segments),
         timing_tolerance_ns:
           opts
           |> Keyword.get(:timing_tolerance_ms, 200)
           |> timing_tolerance_to_ns()
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
         max_segments: Keyword.get(opts, :max_segments),
         timing_tolerance_ns:
           opts
           |> Keyword.get(:timing_tolerance_ms, 200)
           |> timing_tolerance_to_ns()
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
      codecs: Keyword.get(opts, :codecs, []),
      mandatory?: Keyword.get(opts, :mandatory?, nil)
    }
  end

  defp build_tracks_from_playlists(%{
         master_playlist: master,
         media_playlists: medias,
         max_segments: max_segments,
         timeline_reference: timeline_reference
       }) do
    media_by_uri =
      Enum.reduce(medias, %{}, fn %Media{uri: uri} = media, acc ->
        Map.put(acc, to_string(uri), media)
      end)

    streams = master.streams ++ master.alternative_renditions

    with {:ok, entries, referenced_uris} <-
           build_resume_entries(streams, media_by_uri, master.uri),
         :ok <- validate_no_extra_playlists(media_by_uri, referenced_uris),
         {:ok, common_sync_point} <- compute_common_sync_point(entries),
         {:ok, trimmed_entries} <- trim_entries_to_sync_point(entries, common_sync_point),
         {:ok, tracks} <-
           build_tracks_map(trimmed_entries, max_segments, timeline_reference) do
      {:ok, {tracks, common_sync_point}}
    end
  end

  defp build_resume_entries(streams, media_by_uri, manifest_uri) do
    Enum.reduce_while(
      streams,
      {:ok, [], MapSet.new(), MapSet.new()},
      fn stream, {:ok, acc, referenced_uris, track_ids} ->
        case Map.fetch(stream, :uri) do
          :error ->
            error = %Error{
              code: :resume_missing_playlist,
              message: "Stream is missing a media playlist URI",
              details: %{stream: stream}
            }

            {:halt, {:error, error}}

          {:ok, nil} ->
            error = %Error{
              code: :resume_missing_playlist,
              message: "Stream is missing a media playlist URI",
              details: %{stream: stream}
            }

            {:halt, {:error, error}}

          {:ok, uri} ->
            track_id = derive_track_id_from_uri(manifest_uri, uri)

            if MapSet.member?(track_ids, track_id) do
              error = %Error{
                code: :resume_inconsistent_playlist,
                message: "Duplicate track id derived from media playlist URI",
                details: %{track_id: track_id, uri: uri}
              }

              {:halt, {:error, error}}
            else
              {media, missing?} =
                case Map.get(media_by_uri, to_string(uri)) do
                  nil ->
                    {%Media{uri: uri, media_sequence_number: 0, segments: []}, true}

                  %Media{} = media ->
                    {media, false}
                end

              entry = %{track_id: track_id, stream: stream, media: media, missing_media?: missing?}

              {:cont,
               {:ok, [entry | acc], MapSet.put(referenced_uris, to_string(uri)),
                MapSet.put(track_ids, track_id)}}
            end
        end
      end
    )
    |> case do
      {:ok, entries, referenced_uris, _track_ids} ->
        {:ok, Enum.reverse(entries), referenced_uris}

      {:error, _} = error ->
        error
    end
  end

  defp validate_no_extra_playlists(media_by_uri, referenced_uris) do
    extra =
      media_by_uri
      |> Map.keys()
      |> Enum.reject(&MapSet.member?(referenced_uris, &1))

    if extra == [] do
      :ok
    else
      {:error,
       %Error{
         code: :resume_unexpected_playlist,
         message: "Media playlists not referenced by the master playlist",
         details: %{uris: extra}
       }}
    end
  end

  defp compute_common_sync_point([]) do
    {:error,
     %Error{
       code: :resume_inconsistent_playlist,
       message: "No tracks available to resume",
       details: %{}
     }}
  end

  defp compute_common_sync_point(entries) do
    counts =
      Enum.map(entries, fn entry ->
        entry.media.media_sequence_number + length(entry.media.segments)
      end)

    {:ok, Enum.min(counts)}
  end

  defp trim_entries_to_sync_point(entries, common_sync_point) do
    Enum.reduce_while(entries, {:ok, []}, fn entry, {:ok, acc} ->
      media = entry.media
      media_sequence = media.media_sequence_number
      segment_count = media_sequence + length(media.segments)

      cond do
        media_sequence > common_sync_point ->
          error = %Error{
            code: :resume_inconsistent_playlist,
            message: "Media playlist starts after the common sync point",
            details: %{track_id: entry.track_id, sync_point: common_sync_point}
          }

          {:halt, {:error, error}}

        segment_count <= common_sync_point ->
          {:cont, {:ok, [entry | acc]}}

        true ->
          excess = segment_count - common_sync_point
          trimmed_segments = Enum.drop(media.segments, -excess)
          trimmed_media = %{media | segments: trimmed_segments}
          trimmed_entry = %{entry | media: trimmed_media}
          {:cont, {:ok, [trimmed_entry | acc]}}
      end
    end)
    |> case do
      {:ok, trimmed} -> {:ok, Enum.reverse(trimmed)}
      {:error, _} = error -> error
    end
  end

  defp build_tracks_map(entries, max_segments, timeline_reference) do
    tracks =
      Enum.reduce(entries, %{}, fn entry, acc ->
        {:ok, track} = build_track_from_entry(entry, max_segments, timeline_reference)
        Map.put(acc, entry.track_id, track)
      end)

    {:ok, tracks}
  end

  defp build_track_from_entry(
         %{stream: stream, media: media, missing_media?: missing_media?},
         max_segments,
         timeline_reference
       ) do
    segment_extension =
      case infer_segment_extension(media.segments) do
        {:ok, ext} -> ext
        {:error, _error} -> nil
      end

    init_section = last_init_section(media.segments)

    type = if max_segments, do: nil, else: media.type || :event

    duration = Media.compute_playlist_duration(media)
    segment_count = media.media_sequence_number + length(media.segments)

    resume_incomplete? =
      missing_media? or is_nil(segment_extension) or is_nil(media.target_segment_duration)

    {:ok,
     %Track{
       stream: stream,
       duration: duration,
       segment_count: segment_count,
       segment_extension: segment_extension,
       init_section: init_section,
       media_playlist: %{media | type: type},
       pending_playlist: %{media | uri: append_to_path(media.uri, "_pending"), segments: []},
       codecs: [],
       mandatory?: resolve_mandatory(nil, stream),
       last_timestamp_ns: nil,
       last_duration_ns: nil,
       base_pdt: timeline_reference,
       base_timestamp_ns: nil,
       pending_segments: [],
       applied_discontinuities: MapSet.new(),
       resume_incomplete?: resume_incomplete?
     }}
  end

  defp infer_segment_extension([]) do
    {:error,
     %Error{
       code: :resume_unknown_segment_extension,
       message: "Unable to infer segment extension from an empty media playlist",
       details: %{}
     }}
  end

  defp infer_segment_extension(segments) do
    ext =
      segments
      |> List.last()
      |> Map.get(:uri)
      |> to_string()
      |> Path.extname()

    if ext == "" do
      {:error,
       %Error{
         code: :resume_unknown_segment_extension,
         message: "Unable to infer segment extension from media playlist",
         details: %{}
       }}
    else
      {:ok, ext}
    end
  end

  defp last_init_section(segments) do
    Enum.reduce(segments, nil, fn seg, acc ->
      if seg.init_section, do: seg.init_section, else: acc
    end)
  end

  defp derive_track_id_from_uri(manifest_uri, media_uri) do
    manifest_base =
      manifest_uri
      |> to_string()
      |> Path.basename()
      |> Path.rootname()

    media_base =
      media_uri
      |> to_string()
      |> Path.basename()
      |> Path.rootname()

    prefix = manifest_base <> "_"

    if String.starts_with?(media_base, prefix) do
      String.replace_prefix(media_base, prefix, "")
    else
      media_base
    end
  end

  defp build_track_stream(state, track_id, stream) do
    case stream do
      %AlternativeRendition{type: :closed_captions} = alt ->
        {%{alt | uri: nil}, build_track_variant_uri(state.manifest_uri, track_id)}

      stream ->
        stream_uri = build_track_variant_uri(state.manifest_uri, track_id)
        {%{stream | uri: stream_uri}, stream_uri}
    end
  end

  defp resume_track_ready?(track, track_id) do
    missing =
      []
      |> maybe_add_missing(:segment_extension, is_nil(track.segment_extension))
      |> maybe_add_missing(
        :target_segment_duration,
        is_nil(track.media_playlist.target_segment_duration)
      )

    if missing == [] do
      :ok
    else
      {:error,
       %Error{
         code: :resume_track_not_ready,
         message: "Track '#{track_id}' is not ready after resume",
         details: %{track_id: track_id, missing: missing}
       }}
    end
  end

  defp maybe_add_missing(list, _field, false), do: list
  defp maybe_add_missing(list, field, true), do: [field | list]

  defp reconcile_incomplete_track(track, track_id, stream, media_playlist_uri, opts, mandatory?) do
    cond do
      track.stream != stream ->
        {:error,
         %Error{
           code: :track_conflict,
           message: "Track #{track_id} already exists with a different stream",
           details: %{track_id: track_id}
         }}

      track.media_playlist.uri != media_playlist_uri ->
        {:error,
         %Error{
           code: :track_conflict,
           message: "Track #{track_id} already exists with a different playlist URI",
           details: %{track_id: track_id}
         }}

      not is_nil(track.media_playlist.target_segment_duration) and
          track.media_playlist.target_segment_duration != opts.target_segment_duration ->
        {:error,
         %Error{
           code: :track_conflict,
           message: "Track #{track_id} already exists with a different target duration",
           details: %{track_id: track_id}
         }}

      not is_nil(track.segment_extension) and track.segment_extension != opts.segment_extension ->
        {:error,
         %Error{
           code: :track_conflict,
           message: "Track #{track_id} already exists with a different extension",
           details: %{track_id: track_id}
         }}

      track.codecs != [] and track.codecs != opts.codecs ->
        {:error,
         %Error{
           code: :track_conflict,
           message: "Track #{track_id} already exists with different codecs",
           details: %{track_id: track_id}
         }}

      track.mandatory? != mandatory? ->
        {:error,
         %Error{
           code: :track_conflict,
           message: "Track #{track_id} already exists with a different mandatory flag",
           details: %{track_id: track_id}
         }}

      true ->
        target_duration =
          track.media_playlist.target_segment_duration || opts.target_segment_duration

        updated_media = %{track.media_playlist | target_segment_duration: target_duration}
        updated_pending = %{track.pending_playlist | target_segment_duration: target_duration}

        updated_track = %{
          track
          | segment_extension: track.segment_extension || opts.segment_extension,
            media_playlist: updated_media,
            pending_playlist: updated_pending,
            codecs: if(track.codecs == [], do: opts.codecs, else: track.codecs),
            mandatory?: mandatory?,
            resume_incomplete?: false
        }

        {:ok, updated_track}
    end
  end

  defp track_spec_matches?(track, stream, media_playlist_uri, opts, mandatory?) do
    codecs_match =
      cond do
        track.codecs == opts.codecs ->
          true

        track.codecs == [] and is_struct(stream, VariantStream) ->
          Enum.all?(opts.codecs, &(&1 in stream.codecs))

        track.codecs == [] and is_struct(stream, AlternativeRendition) ->
          true

        true ->
          false
      end

    track.stream == stream and
      track.segment_extension == opts.segment_extension and
      track.media_playlist.target_segment_duration == opts.target_segment_duration and
      track.media_playlist.uri == media_playlist_uri and
      codecs_match and
      track.mandatory? == mandatory?
  end
end
