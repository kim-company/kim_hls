defmodule HLS.Packager do
  @moduledoc """
  The `HLS.Packager` module is responsible for managing and generating media and master playlists
  for HTTP Live Streaming (HLS). It handles various tasks such as loading and saving playlists,
  inserting new streams, adding segments, and maintaining synchronization points for different streams.

  ## Usage

  ### Initializing a Packager

  The `new/1` function initializes a new `HLS.Packager` with a storage backend and manifest URI. 
  It either loads an existing master playlist (if it exists) or creates a new one if no playlist is found.

  Example:
  ```elixir
  HLS.Packager.new(
    storage: HLS.Storage.File.new(),
    manifest_uri: URI.new!("file://path/to/stream.m3u8")
  )
  ```

  ### Managing tracks

  You can insert a new track using the `add_track/3` function, which allows adding variant streams
  or alternative renditions to the packager. Tracks can only be inserted before the master playlist has been written.

  Example:
  ```elixir
  {packager, stream_id} = Packager.add_track(
    packager,
    "416x234",
    stream: %HLS.VariantStream{
      uri: URI.new!("stream_416x234.m3u8"),
      bandwidth: 341_276,
      resolution: {416, 234},
      codecs: ["avc1.64000c", "mp4a.40.2"]
    },
    segment_extension: ".m4s",
    target_segment_duration: 7
  )
  ```

  ### Adding the init section

  The `put_init_section/3` function adds or updates the initialization section (such as an MPEG-4 ‘init’ section)
  for a stream. This section will be used for all upcoming segments and is essential for media formats like fragmented
  MP4 where an initial header is required before media segments can be played.

  If the init section has changed, it is uploaded and associated with future segments. If no payload is provided,
  the init section is removed.

  Example:
  ```elixir
  HLS.Packager.put_init_section(packager, track_id, init_segment_data)
  ```

  ### Adding Segments

  The `put_segment/4` function allows adding a new segment to a track. It will update the playlist with the new segment and write it to storage.

  Example:
  ```elixir
  HLS.Packager.put_segment(packager, track_id, segment_data, 10.0)
  ```

  ### Adding Segments Asynchronously

  The `put_segment_async/4` function allows you to add a new segment to a track asynchronously. This function returns a reference to the segment upload task along with a function to perform the upload. After invoking the upload function, you must call the `ack_segment/3` function to acknowledge the segment upload and update the playlist.

  This method is useful when dealing with concurrent uploads of segments while keeping the playlist updates consistent.

  Example:
  ```elixir
  {packager, {ref, upload_fun}} = HLS.Packager.put_segment_async(packager, track_id, segment_data, 10.0)
  :ok = upload_fun.()
  HLS.Packager.ack_segment(packager, track_id, ref)
  ```

  ### Discontinuing Tracks

  The `discontinue_track/2` function forces the next segment to be added to the track with an EXT-X-DISCONTINUITY tag. This tag is used in HLS playlists to indicate that there is a discontinuity in the media timeline, such as a change in codec, resolution, or other major differences between segments.

  This is useful for signaling transitions between different media formats or changes in the stream, making sure that the HLS client can handle the change smoothly.

  Example:
  ```elixir
  packager = HLS.Packager.discontinue_track(packager, track_id)
  ```

  ### Synchronization and Flushing

  The sync/2 function synchronizes media playlists by moving segments from the pending playlist to the main playlist, ensuring that all trackss are properly aligned.
  The flush/1 function writes any remaining segments and marks all playlists as finished.

  Example:
  ```elixir
  HLS.Packager.sync(packager, 14)
  HLS.Packager.flush(packager)
  ```
  """

  alias HLS.Storage
  require Logger

  @type t() :: %__MODULE__{
          master_written?: boolean(),
          storage: HLS.Storage.t(),
          manifest_uri: URI.t(),
          tracks: %{String.t() => Track.t()}
        }

  defstruct [
    :master_written?,
    :storage,
    :manifest_uri,
    tracks: %{}
  ]

  defmodule Track do
    @type t() :: %__MODULE__{
            stream: HLS.VariantStream.t() | HLS.AlternativeRendition.t(),
            duration: float(),
            segment_count: non_neg_integer(),
            segment_extension: String.t(),
            init_section: nil | %{uri: URI.t(), payload: binary()},
            media_playlist: HLS.Playlist.Media.t(),
            pending_playlist: HLS.Playlist.Media.t(),
            discontinue_next_segment: boolean(),
            codecs: [String.t()],
            upload_tasks: [%{ref: reference(), segment: HLS.Segment.t(), uploaded: boolean()}]
          }

    @enforce_keys [
      :init_section,
      :stream,
      :duration,
      :segment_count,
      :segment_extension,
      :media_playlist,
      :pending_playlist
    ]

    defstruct [
                discontinue_next_segment: false,
                upload_tasks: [],
                codecs: []
              ] ++ @enforce_keys
  end

  @doc """
  Initializes a new packager with a storage and its root manifest uri.
  By default, the packager will raise an exception when trying to resume a
  finished track. This behaviour can be controlled with the `resume_finished_tracks` option.

  ## Examples

      iex> HLS.Packager.new(
      ...>   storage: HLS.Storage.File.new(),
      ...>   manifest_uri: URI.new!("file://stream.m3u8"),
      ...>   resume_finished_tracks: false
      ...> )
  """
  def new(opts) do
    opts = Keyword.validate!(opts, [:storage, :manifest_uri, resume_finished_tracks: false])
    manifest_uri = opts[:manifest_uri]
    storage = opts[:storage]

    case Storage.get(storage, manifest_uri) do
      {:ok, data} ->
        master = HLS.Playlist.unmarshal(data, %HLS.Playlist.Master{uri: opts[:manifest_uri]})

        %__MODULE__{
          master_written?: true,
          storage: storage,
          manifest_uri: manifest_uri,
          tracks: load_tracks(storage, master, opts[:resume_finished_tracks])
        }

      {:error, :not_found} ->
        %__MODULE__{
          master_written?: false,
          storage: storage,
          manifest_uri: manifest_uri,
          tracks: %{}
        }
    end
  end

  @doc """
  Will force that the next added segment has an `EXT-X-DISCONTINUITY` tag.
  """
  def discontinue_track(packager, track_id) do
    update_track(
      packager,
      track_id,
      fn track -> %{track | discontinue_next_segment: true} end
    )
  end

  @doc """
  Finds a track from the packager.
  """
  def get_track(packager, track_id) do
    Map.get(packager.tracks, track_id)
  end

  @doc """
  Checks if the given track already exists in the packager.
  """
  def has_track?(packager, track_id) do
    Map.has_key?(packager.tracks, track_id)
  end

  @doc """
  Returns the maximum track duration.
  """
  def max_track_duration(packager) do
    packager.tracks
    |> Enum.map(fn {_track_id, track} ->
      track.duration + HLS.Playlist.Media.compute_playlist_duration(track.pending_playlist)
    end)
    |> Enum.max(&>=/2, fn -> 0 end)
  end

  @doc """
  Returns the duration of the given track.
  """
  def track_duration(packager, track_id) do
    case get_track(packager, track_id) do
      nil ->
        raise HLS.Packager.TrackNotFoundError, "Track with id track_id does not exist."

      track ->
        track.duration + HLS.Playlist.Media.compute_playlist_duration(track.pending_playlist)
    end
  end

  @doc """
  Adds a new track to the packager.

  Tracks can only be added as long as the master playlist has not been written yet.
  """
  def add_track(packager, track_id, opts) do
    opts =
      Keyword.validate!(opts, [
        :stream,
        :segment_extension,
        :target_segment_duration,
        codecs: []
      ])

    stream = opts[:stream]

    cond do
      Map.has_key?(packager.tracks, track_id) ->
        raise HLS.Packager.AddTrackError,
          message: "The track already exists."

      packager.master_written? ->
        raise HLS.Packager.AddTrackError,
          message: "Cannot add a new track if the master playlist was already written."

      true ->
        media_playlist = %HLS.Playlist.Media{
          uri: stream.uri,
          target_segment_duration: opts[:target_segment_duration]
        }

        track = %Track{
          stream: stream,
          duration: 0.0,
          init_section: nil,
          segment_count: 0,
          media_playlist: media_playlist,
          segment_extension: opts[:segment_extension],
          pending_playlist: %{media_playlist | uri: to_pending_uri(stream.uri)},
          codecs: opts[:codecs]
        }

        put_in(packager, [Access.key!(:tracks), track_id], track)
    end
  end

  @doc """
  Puts a new init section that will be used for all upcoming segments.
  """
  def put_init_section(packager, track_id, payload) do
    track = Map.fetch!(packager.tracks, track_id)

    extname =
      case track.segment_extension do
        ".mp4" -> ".mp4"
        ".m4s" -> ".mp4"
        other -> raise "Init section is not supported for #{other} segments."
      end

    init_section =
      cond do
        is_nil(payload) ->
          nil

        is_nil(track.init_section) or track.init_section.payload != payload ->
          next_index = track.segment_count + 1
          segment_uri = relative_segment_uri(track.media_playlist.uri, extname, next_index)
          uri = append_to_path(segment_uri, "_init")

          :ok =
            Storage.put(
              packager.storage,
              HLS.Playlist.Media.build_segment_uri(packager.manifest_uri, uri),
              payload
            )

          %{uri: uri, payload: payload}

        true ->
          track.init_section
      end

    update_track(packager, track_id, fn track -> %{track | init_section: init_section} end)
  end

  @doc """
  Adds a new segment asynchronously to the playlist.
  """
  def put_segment_async(packager, track_id, payload, duration) do
    track = Map.fetch!(packager.tracks, track_id)
    stream_uri = track.media_playlist.uri
    next_index = track.segment_count + 1
    segment_uri = relative_segment_uri(stream_uri, track.segment_extension, next_index)
    init_section = if track.init_section, do: %{uri: track.init_section[:uri]}
    ref = make_ref()

    segment =
      %HLS.Segment{
        uri: segment_uri,
        duration: duration,
        init_section: init_section,
        discontinuity: track.discontinue_next_segment
      }

    upload_fun = fn ->
      :ok =
        Storage.put(
          packager.storage,
          HLS.Playlist.Media.build_segment_uri(packager.manifest_uri, segment.uri),
          payload
        )
    end

    packager =
      update_track(packager, track_id, fn track ->
        track
        |> Map.update!(:segment_count, &(&1 + 1))
        |> Map.replace!(:discontinue_next_segment, false)
        |> Map.update!(:upload_tasks, fn tasks ->
          tasks ++ [%{ref: ref, segment: segment, uploaded: false}]
        end)
      end)

    {packager, {ref, upload_fun}}
  end

  @doc """
  Confirms a successful segment upload.
  """
  def ack_segment(packager, track_id, ref) do
    update_track(packager, track_id, fn track ->
      upload_tasks =
        Enum.map(track.upload_tasks, fn upload_task ->
          if upload_task.ref == ref do
            %{upload_task | uploaded: true}
          else
            upload_task
          end
        end)

      {finished, unfinished} = Enum.split_while(upload_tasks, & &1.uploaded)
      finished_segments = Enum.map(finished, & &1.segment)

      pending_playlist = %{
        track.pending_playlist
        | segments: track.pending_playlist.segments ++ finished_segments
      }

      :ok = write_playlist(packager, pending_playlist)

      %{track | upload_tasks: unfinished, pending_playlist: pending_playlist}
    end)
  end

  @doc """
  Adds a new segment into the playlist.
  """
  def put_segment(packager, track_id, payload, duration) do
    {packager, {ref, upload_fun}} = put_segment_async(packager, track_id, payload, duration)
    :ok = upload_fun.()
    ack_segment(packager, track_id, ref)
  end

  @doc """
  Returns the next synchronization point which
  can then be passed to the `sync/2` function.
  """
  def next_sync_point(%{tracks: []}, _target_duration), do: 0

  def next_sync_point(packager, target_duration) do
    max_duration =
      packager.tracks
      |> Enum.map(fn {_id, track} -> track.duration end)
      |> Enum.max()

    ceil(max_duration / target_duration) * target_duration
  end

  @doc """
  Synchornizes all media playlists and writes down the master playlist as soon as needed.
  """
  def sync(packager, sync_point) do
    packager
    |> sync_playlists(sync_point)
    |> maybe_write_master()
  end

  @doc """
  Writes down the remaining segments and marks all playlists as finished (EXT-X-ENDLIST).
  Deletes pending playlists.
  """
  def flush(packager) do
    tracks =
      Map.new(
        packager.tracks,
        fn {id, track} ->
          pending_duration = HLS.Playlist.Media.compute_playlist_duration(track.pending_playlist)

          track =
            track
            |> Map.update!(:duration, &(&1 + pending_duration))
            |> Map.update!(:media_playlist, fn playlist ->
              %{
                playlist
                | segments: playlist.segments ++ track.pending_playlist.segments,
                  finished: true,
                  type: :vod
              }
            end)
            |> Map.update!(:pending_playlist, fn playlist ->
              %{playlist | segments: [], finished: true, type: :vod}
            end)

          :ok = write_playlist(packager, track.media_playlist)
          :ok = delete_playlist(packager, track.pending_playlist)

          {id, track}
        end
      )

    packager
    |> Map.replace!(:tracks, tracks)
    |> maybe_write_master(force: true)
  end

  @doc """
  Builds the master playlist of the given packager.
  """
  def build_master(packager) do
    alternative_tracks =
      packager.tracks
      |> Map.values()
      |> Enum.filter(&is_struct(&1.stream, HLS.AlternativeRendition))

    variant_tracks =
      packager.tracks
      |> Map.values()
      |> Enum.filter(&is_struct(&1.stream, HLS.VariantStream))
      |> Enum.map(fn track ->
        group_ids = HLS.VariantStream.associated_group_ids(track.stream)

        alternative_codecs =
          alternative_tracks
          |> Enum.filter(fn track -> Enum.member?(group_ids, track.stream.group_id) end)
          |> Enum.flat_map(& &1.codecs)

        update_in(track, [Access.key!(:stream), Access.key!(:codecs)], fn prev_codecs ->
          prev_codecs
          |> Enum.concat(track.codecs)
          |> Enum.concat(alternative_codecs)
          |> Enum.uniq()
        end)
      end)

    %HLS.Playlist.Master{
      version: 4,
      uri: packager.manifest_uri,
      independent_segments: true,
      streams: Enum.map(variant_tracks, & &1.stream),
      alternative_renditions: Enum.map(alternative_tracks, & &1.stream)
    }
  end

  @doc """
  Generates a new variant URI from an existing packager.

  ## Examples

      iex> packager = HLS.Packager.new(
      ...>   storage: HLS.Storage.File.new(),
      ...>   manifest_uri: URI.new!("file://stream.m3u8")
      ...> )
      iex> HLS.Packager.new_variant_uri(packager, "video_480p")
      URI.new!("stream_video_480p.m3u8")
  """
  def new_variant_uri(packager, suffix) do
    packager.manifest_uri
    |> append_to_path("_" <> suffix)
    |> to_string()
    |> Path.basename()
    |> URI.new!()
  end

  @doc """
  Generates a relative segment uri for the given playlist and segment index.

  ## Examples

      iex> HLS.Packager.relative_segment_uri(
      ...>   URI.new!("file://x/stream_video_480p.m3u8"),
      ...>   ".aac",
      ...>   48
      ...> )
      URI.new!("stream_video_480p/00000/stream_video_480p_00048.aac")
  """
  def relative_segment_uri(playlist_uri, extname, segment_index) do
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

    [
      root_path,
      dir,
      "#{Path.basename(root_path)}_#{suffix}#{extname}"
    ]
    |> Path.join()
    |> URI.new!()
  end

  @doc """
  Allows to append something to an URIs path.

  ## Examples

      iex> HLS.Packager.append_to_path(URI.new!("file://a.m3u8"), "_480p")
      URI.new!("file://a_480p.m3u8")

      iex> HLS.Packager.append_to_path(URI.new!("file://a/b.m3u8"), "_480p")
      URI.new!("file://a/b_480p.m3u8")
  """
  def append_to_path(uri, append) do
    field = if is_nil(uri.path), do: :host, else: :path

    Map.update!(uri, field, fn path ->
      extname = Path.extname(path)
      without_ext = String.trim_trailing(path, extname)

      "#{without_ext}#{append}#{extname}"
    end)
  end

  defp sync_playlists(packager, sync_point) do
    tracks =
      Map.new(packager.tracks, fn {id, track} ->
        track = move_segments_until_sync_point(packager, track, sync_point)
        {id, track}
      end)

    Logger.debug(fn ->
      track_info =
        Enum.map(tracks, fn {id, track} ->
          media_playlist_segments =
            track.segment_count - length(track.pending_playlist.segments) -
              length(track.upload_tasks)

          "#{id}: #{media_playlist_segments}/#{track.segment_count} segment published (#{track.duration}s)"
        end)

      """
      #{__MODULE__}.sync/2 synchronized tracks:
        - #{Enum.join(track_info, "\n  - ")}
      """
    end)

    %{packager | tracks: tracks}
  end

  defp move_segments_until_sync_point(packager, track, sync_point) do
    {moved_segments, remaining_segments, new_duration} =
      split_segments_at_sync_point(
        track.pending_playlist.segments,
        sync_point,
        track.duration
      )

    track =
      track
      |> Map.replace!(:duration, new_duration)
      |> Map.update!(:media_playlist, fn playlist ->
        %{playlist | segments: playlist.segments ++ moved_segments}
      end)
      |> Map.update!(:pending_playlist, fn playlist ->
        %{playlist | segments: remaining_segments}
      end)

    if Enum.any?(moved_segments) do
      :ok = write_playlist(packager, track.media_playlist)
      :ok = write_playlist(packager, track.pending_playlist)
    end

    track
  end

  defp split_segments_at_sync_point(
         pending_segments,
         sync_point,
         acc_duration,
         moved_segs \\ []
       )

  defp split_segments_at_sync_point([], _sync_point, acc_duration, moved_segs) do
    {Enum.reverse(moved_segs), [], acc_duration}
  end

  defp split_segments_at_sync_point([segment | rest], sync_point, acc_duration, moved_segs) do
    new_duration = acc_duration + segment.duration

    if new_duration <= sync_point do
      split_segments_at_sync_point(
        rest,
        sync_point,
        new_duration,
        [segment | moved_segs]
      )
    else
      {Enum.reverse(moved_segs), [segment | rest], acc_duration}
    end
  end

  defp maybe_write_master(packager, opts \\ []) do
    opts = Keyword.validate!(opts, force: false)

    if packager.master_written? do
      packager
    else
      all_playlists_ready? =
        Enum.all?(packager.tracks, fn {_uri, track} ->
          track.duration >= track.media_playlist.target_segment_duration * 3
        end)

      if opts[:force] or all_playlists_ready? do
        master_playlist = build_master(packager)
        :ok = write_playlist(packager, master_playlist)
        Logger.debug(fn -> "#{__MODULE__}.maybe_write_master/2 master playlist written." end)
        %{packager | master_written?: true}
      else
        Logger.debug(fn ->
          track_info =
            Enum.map(packager.tracks, fn {id, track} ->
              "#{id}: #{track.duration}s (expected: #{track.media_playlist.target_segment_duration * 3}s)"
            end)

          """
          #{__MODULE__}.maybe_write_master/2 not all tracks are ready yet.
            - #{Enum.join(track_info, "\n  - ")}
          """
        end)

        packager
      end
    end
  end

  defp load_tracks(storage, master, resume_finished_tracks) do
    all_streams = Enum.concat(master.streams, master.alternative_renditions)

    Enum.reduce(all_streams, %{}, fn stream, acc ->
      media_playlist =
        case Storage.get(storage, HLS.Playlist.build_absolute_uri(master.uri, stream.uri)) do
          {:ok, data} ->
            media = HLS.Playlist.unmarshal(data, %HLS.Playlist.Media{uri: stream.uri})

            cond do
              media.finished and not resume_finished_tracks ->
                raise HLS.Packager.PlaylistFinishedError,
                  message: "Cannot resume a finished media playlist: #{to_string(stream.uri)}"

              resume_finished_tracks ->
                %{media | finished: false, type: nil}

              true ->
                media
            end

          {:error, :not_found} ->
            raise HLS.Packager.PlaylistNotFoundError,
              message: "Cannot load media playlist: #{to_string(stream.uri)}"
        end

      pending_uri = to_pending_uri(stream.uri)

      pending_playlist =
        case Storage.get(storage, HLS.Playlist.build_absolute_uri(master.uri, pending_uri)) do
          {:ok, data} ->
            data
            |> HLS.Playlist.unmarshal(%HLS.Playlist.Media{uri: pending_uri})
            |> Map.replace!(:finished, false)
            |> Map.replace!(:type, nil)

          {:error, _error} ->
            %HLS.Playlist.Media{
              uri: pending_uri,
              target_segment_duration: media_playlist.target_segment_duration,
              version: media_playlist.version
            }
        end

      all_segments = Enum.reverse(media_playlist.segments ++ pending_playlist.segments)

      if Enum.empty?(all_segments) do
        raise HLS.Packager.ResumeError, message: "Cannot resume a playlist without segments."
      end

      last_segment = hd(all_segments)

      segment_extension =
        last_segment.uri
        |> to_string()
        |> Path.extname()

      init_section =
        if last_segment.init_section do
          {:ok, payload} =
            Storage.get(
              storage,
              HLS.Playlist.Media.build_segment_uri(master.uri, last_segment.init_section.uri)
            )

          %{uri: last_segment.init_section.uri, payload: payload}
        end

      track_id = uri_to_track_id(master.uri, stream.uri)

      track = %Track{
        stream: stream,
        segment_extension: segment_extension,
        segment_count: length(all_segments),
        init_section: init_section,
        duration: HLS.Playlist.Media.compute_playlist_duration(media_playlist),
        media_playlist: media_playlist,
        pending_playlist: pending_playlist
      }

      Map.put(acc, track_id, track)
    end)
  end

  defp to_pending_uri(uri), do: append_to_path(uri, "_pending")

  defp write_playlist(packager, playlist) do
    Storage.put(
      packager.storage,
      HLS.Playlist.build_absolute_uri(packager.manifest_uri, playlist.uri),
      HLS.Playlist.marshal(playlist)
    )
  end

  defp delete_playlist(packager, playlist) do
    Storage.delete(
      packager.storage,
      HLS.Playlist.build_absolute_uri(packager.manifest_uri, playlist.uri)
    )
  end

  defp uri_to_track_id(master_uri, stream_uri) do
    master_uri = to_string(master_uri)
    extname = Path.extname(master_uri)

    leading =
      master_uri
      |> to_string()
      |> Path.basename()
      |> String.trim_trailing(extname)

    stream_uri
    |> to_string()
    |> String.trim_leading(leading <> "_")
    |> String.trim_trailing(extname)
  end

  defp update_track(packager, track_id, callback) do
    update_in(packager, [Access.key!(:tracks), track_id], callback)
  end
end
