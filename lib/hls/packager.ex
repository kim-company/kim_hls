defmodule HLS.Packager do
  @moduledoc """
  The `HLS.Packager` module is responsible for managing and generating media and master playlists
  for HTTP Live Streaming (HLS). It handles various tasks such as loading and saving playlists,
  inserting new streams, uploading segments and maintaining index-based synchronization for different streams.

  Synchronization is now index-based rather than time-based, ensuring consistent behavior across
  audio and video tracks regardless of exact segment durations.
  """

  use GenServer
  alias HLS.Storage
  require Logger

  @type track_id :: String.t()

  @type t() :: %__MODULE__{
          master_written?: boolean(),
          storage: HLS.Storage.t(),
          manifest_uri: URI.t(),
          tracks: %{track_id() => Track.t()},
          upload_tasks_to_track: %{Task.ref() => track_id()},
          master_written_callback: nil | (-> term()),
          max_segments: nil | pos_integer(),
          # RFC-compliant: Common timeline reference point for all tracks
          timeline_reference: DateTime.t()
        }

  defstruct [
    :master_written?,
    :storage,
    :manifest_uri,
    :master_written_callback,
    :max_segments,
    :timeline_reference,
    tracks: %{},
    upload_tasks_to_track: %{}
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
            upload_tasks: [%{ref: reference(), segment: HLS.Segment.t(), uploaded: boolean()}],
            # RFC-compliant: next datetime for program date time assignment (track-level timeline)
            next_sync_datetime: nil | DateTime.t()
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
                codecs: [],
                next_sync_datetime: nil
              ] ++ @enforce_keys
  end

  @doc """
  Initializes a new packager with a storage and its root manifest uri.
  By default, the packager will raise an exception when trying to resume a
  finished track. This behaviour can be controlled with the `resume_finished_tracks` option.

  ## Options

  * `:max_segments` - Maximum number of segments to keep in each media playlist. When exceeded, 
    old segments are removed from playlists and deleted from storage. `nil` (default) means unlimited.

  ## Examples

  ```elixir
  HLS.Packager.start_link(
    storage: HLS.Storage.File.new(),
    manifest_uri: URI.new!("file://stream.m3u8"),
    resume_finished_tracks: false,
    restore_pending_segments: true,
    master_written_callback: nil,
    max_segments: 10
  )
  ```
  """

  def start_link(opts) do
    opts =
      Keyword.validate!(opts, [
        :storage,
        :manifest_uri,
        resume_finished_tracks: false,
        restore_pending_segments: true,
        name: nil,
        master_written_callback: nil,
        max_segments: nil
      ])

    {server_opts, opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, opts, server_opts)
  end

  @doc """
  Checks if the given track already exists in the packager.
  """
  @spec has_track?(GenServer.server(), track_id()) :: boolean()
  def has_track?(packager, track_id) do
    GenServer.call(packager, {:has_track?, track_id})
  end

  @doc """
  Returns the tracks managed by the packager.
  """
  @spec tracks(GenServer.server()) :: %{track_id() => Track.t()}
  def tracks(packager) do
    GenServer.call(packager, :tracks)
  end

  @doc """
  Returns true if the packager is configured to produce a live playlist with the
  sliding window feature enabled.
  """
  @spec sliding_window_enabled?(GenServer.server()) :: boolean()
  def sliding_window_enabled?(packager) do
    GenServer.call(packager, :is_sliding_window)
  end

  @doc """
  Adds a new track to the packager.
  Tracks can only be added as long as the master playlist has not been written yet.

  ## Examples

  ```elixir
  Packager.add_track(
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
  """
  @type add_track_opt ::
          {:stream, HLS.VariantStream.t() | HLS.AlternativeRendition.t()}
          | {:segment_extension, String.t()}
          | {:target_segment_duration, float()}
          | {:codecs, [String.t()]}
  @spec add_track(GenServer.server(), track_id(), [add_track_opt()]) :: :ok
  def add_track(packager, track_id, opts) do
    GenServer.call(packager, {:add_track, track_id, opts})
  end

  @doc """
  Will force that the next added segment has an `EXT-X-DISCONTINUITY` tag for ALL tracks.
  It is applied only in case max_segments is not nil.
  According to HLS specification, discontinuity markers should be synchronized across all tracks.
  """
  @spec discontinue(GenServer.server()) :: :ok
  def discontinue(packager) do
    GenServer.cast(packager, :discontinue)
  end

  @doc """
  Returns the maximum track duration.
  """
  @spec max_track_duration(GenServer.server()) :: non_neg_integer()
  def max_track_duration(packager) do
    GenServer.call(packager, :max_track_duration)
  end

  @doc """
  Returns the duration of the given track.
  """
  @spec track_duration(GenServer.server(), track_id()) ::
          {:ok, non_neg_integer()} | {:error, :not_found}
  def track_duration(packager, track_id) do
    GenServer.call(packager, {:track_duration, track_id})
  end

  @doc """
  The `put_init_section/3` function adds or updates the initialization section (such as an MPEG-4 ‘init’ section)
  for the given track. This section will be used for all upcoming segments and is essential for media formats like fragmented
  MP4 where an initial header is required before media segments can be played.

  If the init section has changed, it is uploaded and associated with future segments. If no payload is provided,
  the init section is removed.
  """
  @spec put_init_section(GenServer.server(), track_id(), binary() | nil) :: :ok
  def put_init_section(packager, track_id, payload) do
    GenServer.cast(packager, {:put_init_section, track_id, payload})
  end

  @doc """
  Adds a new segment asynchronously to the given track.
  """
  @spec put_segment(GenServer.server(), track_id(), binary(), float()) :: :ok
  def put_segment(packager, track_id, payload, duration) do
    GenServer.cast(packager, {:put_segment, track_id, payload, duration})
  end

  @doc """
  When `max_segments` is `nil` (default): Writes down the remaining segments and marks all 
  playlists as finished (EXT-X-ENDLIST). Deletes pending playlists.

  When `max_segments` is configured: Cleans up all segments and playlists from storage.
  This is the expected behavior for sliding window scenarios where storage cleanup is desired.
  """
  @spec flush(GenServer.server()) :: :ok
  def flush(packager) do
    GenServer.call(packager, :flush, :infinity)
  end

  @doc """
  Returns the next synchronization index which
  can then be passed to the `sync/2` function.
  """
  @spec next_sync_point(GenServer.server()) :: pos_integer()
  def next_sync_point(packager) do
    GenServer.call(packager, :next_sync_point)
  end

  @doc """
  Synchronizes all media playlists up to the specified segment index and writes down the master playlist as soon as needed.
  For example, `sync(packager, 100)` ensures all tracks have at least 100 segments in their media playlists.
  """
  @spec sync(GenServer.server(), pos_integer()) :: :ok
  def sync(packager, sync_index) do
    GenServer.cast(packager, {:sync, sync_index})
  end

  @doc """
  Returns a relative variant uri for a given track id.
  """
  @spec track_variant_uri(GenServer.server(), track_id()) :: URI.t()
  def track_variant_uri(packager, track_id) do
    GenServer.call(packager, {:track_variant_uri, track_id})
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

  # Callbacks

  @impl true
  def init(opts) do
    manifest_uri = opts[:manifest_uri]
    storage = opts[:storage]
    # RFC-compliant: Establish common timeline reference for all tracks
    timeline_reference = DateTime.utc_now(:millisecond)

    case Storage.get(storage, manifest_uri, max_retries: 5) do
      {:ok, data} ->
        master = HLS.Playlist.unmarshal(data, %HLS.Playlist.Master{uri: opts[:manifest_uri]})

        load_track_opts =
          Keyword.take(opts, [:resume_finished_tracks, :restore_pending_segments, :max_segments])
          |> Keyword.put(:timeline_reference, timeline_reference)

        {:ok,
         %__MODULE__{
           master_written?: true,
           master_written_callback: opts[:master_written_callback],
           storage: storage,
           manifest_uri: manifest_uri,
           max_segments: opts[:max_segments],
           timeline_reference: timeline_reference,
           tracks: load_and_fix_tracks(storage, master, load_track_opts)
         }}

      {:error, :not_found} ->
        {:ok,
         %__MODULE__{
           master_written?: false,
           master_written_callback: opts[:master_written_callback],
           storage: storage,
           manifest_uri: manifest_uri,
           max_segments: opts[:max_segments],
           timeline_reference: timeline_reference,
           tracks: %{}
         }}

      {:error, error} ->
        raise HLS.Packager.ResumeError,
          message: "Cannot check current state on the storage: #{inspect(error)}."
    end
  end

  @impl true
  def handle_cast({:sync, sync_point}, state) do
    state =
      state
      |> sync_playlists(sync_point)
      |> maybe_write_master(sync_point: sync_point)

    {:noreply, state}
  end

  def handle_cast(:discontinue, state = %{max_segments: nil}) do
    {:noreply, state}
  end

  def handle_cast(:discontinue, state) do
    updated_tracks =
      Enum.into(state.tracks, %{}, fn {track_id, track} ->
        {track_id, %{track | discontinue_next_segment: true}}
      end)

    {:noreply, %{state | tracks: updated_tracks}}
  end

  def handle_cast({:put_init_section, track_id, payload}, state) do
    track = Map.fetch!(state.tracks, track_id)

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
              state.storage,
              HLS.Playlist.Media.build_segment_uri(state.manifest_uri, uri),
              payload,
              max_retries: 10
            )

          %{uri: uri, payload: payload}

        true ->
          track.init_section
      end

    {:noreply,
     update_track(
       state,
       track_id,
       fn track -> %{track | init_section: init_section} end
     )}
  end

  def handle_cast({:put_segment, track_id, payload, duration}, state) do
    track = Map.fetch!(state.tracks, track_id)
    stream_uri = track.media_playlist.uri
    next_index = track.segment_count + 1
    segment_uri = relative_segment_uri(stream_uri, track.segment_extension, next_index)
    init_section = if track.init_section, do: %{uri: track.init_section[:uri]}

    segment =
      %HLS.Segment{
        uri: segment_uri,
        duration: Float.round(duration, 5),
        init_section: init_section,
        discontinuity: track.discontinue_next_segment
      }

    task =
      Task.Supervisor.async_nolink(HLS.Task.Supervisor, fn ->
        :ok =
          Storage.put(
            state.storage,
            HLS.Playlist.Media.build_segment_uri(state.manifest_uri, segment.uri),
            payload,
            max_retries: 10
          )
      end)

    state =
      state
      |> put_in([Access.key!(:upload_tasks_to_track), task.ref], track_id)
      |> update_track(track_id, fn track ->
        track
        |> Map.update!(:segment_count, &(&1 + 1))
        |> Map.replace!(:discontinue_next_segment, false)
        |> Map.update!(:upload_tasks, fn tasks ->
          tasks ++ [%{task: task, segment: segment, uploaded: false}]
        end)
        |> then(fn track ->
          # RFC-compliant: Reset timeline at discontinuity boundaries
          if segment.discontinuity do
            %{track | next_sync_datetime: state.timeline_reference}
          else
            track
          end
        end)
      end)

    {:noreply, state}
  end

  @impl true
  def handle_call(:is_sliding_window, _from, state) do
    {:reply, state.max_segments != nil, state}
  end

  def handle_call({:track_variant_uri, track_id}, _from, state) do
    {:reply, build_track_variant_uri(state, track_id), state}
  end

  def handle_call({:has_track?, track_id}, _from, state) do
    {:reply, Map.has_key?(state.tracks, track_id), state}
  end

  def handle_call(:tracks, _from, state) do
    {:reply, state.tracks, state}
  end

  def handle_call(:next_sync_point, _from, state) do
    max_segments =
      state.tracks
      |> Enum.map(fn {_id, track} -> media_playlist_segment_count(track) end)
      |> Enum.max(fn -> 0 end)

    sync_index = max_segments + 1

    {:reply, sync_index, state}
  end

  def handle_call({:track_duration, track_id}, _from, state) do
    case Map.get(state.tracks, track_id) do
      nil ->
        {:reply, {:error, :not_found}, state}

      track ->
        pending_duration = HLS.Playlist.Media.compute_playlist_duration(track.pending_playlist)
        {:reply, {:ok, track.duration + pending_duration}, state}
    end
  end

  def handle_call(:max_track_duration, _from, state) do
    duration =
      state.tracks
      |> Enum.map(fn {_track_id, track} ->
        track.duration + HLS.Playlist.Media.compute_playlist_duration(track.pending_playlist)
      end)
      |> Enum.max(&>=/2, fn -> 0 end)

    {:reply, duration, state}
  end

  def handle_call({:add_track, track_id, opts}, _from, state) do
    opts =
      Keyword.validate!(opts, [
        :stream,
        :segment_extension,
        :target_segment_duration,
        codecs: []
      ])

    cond do
      Map.has_key?(state.tracks, track_id) ->
        {:reply, {:error, :track_already_exists}, state}

      state.master_written? ->
        {:reply, {:error, :master_playlist_written}, state}

      true ->
        stream = Map.replace!(opts[:stream], :uri, build_track_variant_uri(state, track_id))
        type = if is_nil(state.max_segments), do: :event

        media_playlist = %HLS.Playlist.Media{
          # TODO: Version 7 is requires for CMAF playlists.
          version: 4,
          uri: stream.uri,
          target_segment_duration: opts[:target_segment_duration],
          type: type
        }

        track = %Track{
          stream: stream,
          duration: 0.0,
          init_section: nil,
          segment_count: 0,
          media_playlist: media_playlist,
          segment_extension: opts[:segment_extension],
          pending_playlist: %{media_playlist | uri: to_pending_uri(stream.uri)},
          codecs: opts[:codecs],
          # RFC-compliant: Initialize track timeline from packager's reference point
          next_sync_datetime: state.timeline_reference
        }

        {:reply, :ok, put_in(state, [Access.key!(:tracks), track_id], track)}
    end
  end

  def handle_call(:flush, _from, state) do
    if state.max_segments do
      handle_flush_with_cleanup(state)
    else
      handle_flush_as_vod(state)
    end
  end

  # Handles flush operation with storage cleanup for sliding window scenarios
  defp handle_flush_with_cleanup(state) do
    # First, finish pending uploads and collect all segments
    tracks =
      state.tracks
      |> Stream.map(fn {id, track} ->
        # Wait for pending uploads
        track.upload_tasks
        |> Enum.reject(& &1.uploaded)
        |> Enum.map(& &1.task)
        |> Task.await_many(:infinity)

        # Collect all segments (media playlist + pending + upload tasks)
        all_segments =
          track.media_playlist.segments ++
            track.pending_playlist.segments ++
            Enum.map(track.upload_tasks, & &1.segment)

        {id, track, all_segments}
      end)
      |> Enum.to_list()

    # Clean up all segments from storage
    tracks
    |> Task.async_stream(
      fn {id, _track, segments} ->
        # For flush cleanup, we're deleting all segments, so no init sections should be preserved
        cleanup_segments_from_storage(state, segments, id, nil, false)
        {id, :cleaned}
      end,
      concurrency: Enum.count(state.tracks),
      ordered: false,
      timeout: :infinity
    )
    |> Stream.run()

    # Delete all playlists from storage
    tracks
    |> Task.async_stream(
      fn {id, track, _segments} ->
        :ok = delete_playlist(state, track.media_playlist, max_retries: 10)
        :ok = delete_playlist(state, track.pending_playlist, max_retries: 10)

        {id, :deleted}
      end,
      concurrency: Enum.count(state.tracks),
      ordered: false,
      timeout: :infinity
    )
    |> Stream.run()

    # Delete master playlist
    Storage.delete(state.storage, state.manifest_uri, max_retries: 3)

    Logger.debug(fn ->
      track_info =
        Enum.map(tracks, fn {id, _track, segments} ->
          "#{id}: cleaned #{length(segments)} segments"
        end)

      """
      #{__MODULE__}.flush/1 cleaned up all storage for sliding window:
        - #{Enum.join(track_info, "\n  - ")}
      """
    end)

    # Reset state
    state =
      state
      |> Map.replace!(:tracks, %{})
      |> Map.replace!(:upload_tasks_to_track, %{})
      |> Map.replace!(:master_written?, false)

    {:reply, :ok, state}
  end

  # Handles flush operation creating VOD playlists (original behavior)
  defp handle_flush_as_vod(state) do
    tracks =
      state.tracks
      # finish pending uploads
      |> Stream.map(fn {id, track} ->
        track.upload_tasks
        |> Enum.reject(& &1.uploaded)
        |> Enum.map(& &1.task)
        |> Task.await_many(:infinity)

        pending_playlist = %{
          track.pending_playlist
          | segments:
              track.pending_playlist.segments ++ Enum.map(track.upload_tasks, & &1.segment)
        }

        track = %{track | pending_playlist: pending_playlist, upload_tasks: []}
        {id, track}
      end)
      # update playlists
      |> Task.async_stream(
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

          :ok = write_playlist(state, track.media_playlist, max_retries: 10)
          :ok = delete_playlist(state, track.pending_playlist, max_retries: 10)

          {id, track}
        end,
        concurrency: Enum.count(state.tracks),
        ordered: false,
        timeout: :infinity
      )
      |> Map.new(fn {:ok, {id, track}} -> {id, track} end)

    Enum.each(tracks, fn {id, track} -> measure_track_progress(id, track) end)

    Logger.debug(fn ->
      track_info =
        Enum.map(tracks, fn {id, track} ->
          "#{id}: segments: #{track.segment_count}, duration: #{Float.round(track.duration, 2)}s"
        end)

      """
      #{__MODULE__}.flush/1 flushed all playlists and set them to vod:
        - #{Enum.join(track_info, "\n  - ")}
      """
    end)

    state =
      state
      |> Map.replace!(:tracks, tracks)
      |> Map.replace!(:upload_tasks_to_track, %{})
      |> maybe_write_master(force: true)

    {:reply, :ok, state}
  end

  defp measure_track_progress(track_id, track) do
    metadata = %{
      track_id: track_id,
      pid: self()
    }

    if Code.ensure_loaded?(:telemetry) do
      :telemetry.execute([:hls, :packager, :track], %{duration: round(track.duration)}, metadata)

      :telemetry.execute(
        [:hls, :packager, :track, :segment],
        %{
          count: track.segment_count,
          published:
            track.segment_count - length(track.pending_playlist.segments) -
              length(track.upload_tasks)
        },
        metadata
      )
    end
  end

  @impl true
  def handle_info({ref, :ok}, state) when is_map_key(state.upload_tasks_to_track, ref) do
    Process.demonitor(ref, [:flush])
    {track_id, state} = pop_in(state, [Access.key!(:upload_tasks_to_track), ref])

    state =
      update_track(state, track_id, fn track ->
        upload_tasks =
          Enum.map(track.upload_tasks, fn upload_task ->
            if upload_task.task.ref == ref do
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

        maybe_write_pending_playlist(state, pending_playlist)

        %{track | upload_tasks: unfinished, pending_playlist: pending_playlist}
      end)

    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, ref, _, _, reason}, state)
      when is_map_key(state.upload_tasks_to_track, ref) do
    # TODO: Maybe we should write the segment in the playlist and just continue as nothing has happened?
    raise "Cannot write segment with reason: #{inspect(reason)}."
  end

  defp build_master(packager) do
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

  defp build_track_variant_uri(packager, suffix) do
    packager.manifest_uri
    |> append_to_path("_" <> suffix)
    |> to_string()
    |> Path.basename()
    |> URI.new!()
  end

  defp sync_playlists(packager, sync_point) do
    tracks =
      packager.tracks
      |> async_stream_nolink(
        fn {id, track} ->
          track = move_segments_until_sync_point(packager, track, sync_point)
          {id, track}
        end,
        ordered: false,
        timeout: :infinity,
        concurrency: Enum.count(packager.tracks)
      )
      |> Map.new(fn {:ok, {id, track}} -> {id, track} end)

    Enum.each(tracks, fn {id, track} -> measure_track_progress(id, track) end)

    count = Enum.map(tracks, fn {_, track} -> media_playlist_segment_count(track) end)
    log_level = if Enum.any?(count, &(&1 < sync_point)), do: :warning, else: :debug

    Logger.log(log_level, fn ->
      track_info =
        Enum.map(tracks, fn {id, track} ->
          media_playlist_segments = media_playlist_segment_count(track)

          "#{id}: #{media_playlist_segments}/#{track.segment_count} segment published (#{Float.round(track.duration, 2)}s)"
        end)

      """
      #{__MODULE__}.sync/2 synchronized tracks to #{sync_point}:
        - #{Enum.join(track_info, "\n  - ")}
      """
    end)

    %{packager | tracks: tracks}
  end

  defp media_playlist_segment_count(track) do
    track.segment_count - length(track.pending_playlist.segments) -
      length(track.upload_tasks)
  end

  defp assign_datetime(segments, track) do
    Enum.map_reduce(segments, track, fn segment, track ->
      segment = put_in(segment, [Access.key!(:program_date_time)], track.next_sync_datetime)

      track =
        update_in(track, [Access.key!(:next_sync_datetime)], fn current_time ->
          DateTime.add(current_time, round(segment.duration), :second)
        end)

      {segment, track}
    end)
  end

  defp move_segments_until_sync_point(packager, track, sync_point) do
    current_media_playlist_count = media_playlist_segment_count(track)

    {moved_segments, remaining_segments, moved_duration} =
      split_segments_at_sync_point(
        track.pending_playlist.segments,
        sync_point,
        current_media_playlist_count
      )

    new_duration = track.duration + moved_duration

    # RFC-compliant: Assign program date times using track-level timeline
    # Only assign when sliding window (max_segments) is enabled, matching original behavior
    {moved_segments, track} =
      if packager.max_segments && not Enum.empty?(moved_segments) do
        assign_datetime(moved_segments, track)
      else
        {moved_segments, track}
      end

    track =
      track
      |> Map.replace!(:duration, new_duration)
      |> Map.update!(:media_playlist, fn playlist ->
        updated_playlist = %{playlist | segments: playlist.segments ++ moved_segments}

        # Apply sliding window to media playlist
        {windowed_playlist, removed_segments} =
          apply_sliding_window(updated_playlist, packager.max_segments)

        # Clean up removed segments from storage
        if not Enum.empty?(removed_segments) do
          track_id = uri_to_track_id(packager.manifest_uri, track.stream.uri)
          cleanup_segments_from_storage(packager, removed_segments, track_id, windowed_playlist)
        end

        windowed_playlist
      end)
      |> Map.update!(:pending_playlist, fn playlist ->
        %{playlist | segments: remaining_segments}
      end)

    if Enum.any?(moved_segments) do
      write_playlist(packager, track.media_playlist)
      maybe_write_pending_playlist(packager, track.pending_playlist)
    end

    track
  end

  defp split_segments_at_sync_point(pending_segments, sync_point, current_media_playlist_count) do
    segments_to_move = max(0, sync_point - current_media_playlist_count)

    {moved_segments, remaining_segments} = Enum.split(pending_segments, segments_to_move)

    moved_duration = Enum.reduce(moved_segments, 0.0, fn seg, acc -> acc + seg.duration end)

    {moved_segments, remaining_segments, moved_duration}
  end

  defp maybe_write_master(packager, _opts) when packager.master_written? do
    packager
  end

  defp maybe_write_master(packager, opts) do
    opts = Keyword.validate!(opts, force: false, sync_point: nil)

    if opts[:force] or opts[:sync_point] >= 3 do
      master_playlist = build_master(packager)
      :ok = write_playlist(packager, master_playlist, max_retries: 10)
      if packager.master_written_callback != nil, do: packager.master_written_callback.()

      Logger.info(fn -> "#{__MODULE__}.maybe_write_master/2 master playlist written." end)

      %{packager | master_written?: true}
    else
      Enum.each(packager.tracks, fn {id, track} -> measure_track_progress(id, track) end)

      Logger.debug(fn ->
        track_info =
          Enum.map(packager.tracks, fn {id, track} ->
            "#{id}: #{Float.round(track.duration, 2)}s (expected: #{track.media_playlist.target_segment_duration * 3}s)"
          end)

        """
        #{__MODULE__}.maybe_write_master/2 not all tracks are ready yet and the sync point was not reached.
          - #{Enum.join(track_info, "\n  - ")}
        """
      end)

      packager
    end
  end

  defp load_and_fix_tracks(storage, master, opts) do
    storage
    |> load_tracks(master, opts)
    |> fix_tracks(storage, master, opts)
  end

  defp fix_tracks(tracks, storage, master, opts) do
    max_segments = opts[:max_segments]

    all_synced? =
      tracks
      |> Enum.map(fn {_id, t} -> t.segment_count end)
      |> Enum.uniq()
      |> then(fn x -> length(x) == 1 end)

    if all_synced? do
      tracks
    else
      # Only fix tracks for sliding window livestreams (max_segments != nil)
      if max_segments != nil do
        fix_out_of_sync_tracks(tracks, storage, master, opts)
      else
        tracks
      end
    end
  end

  # Synchronizes out-of-sync tracks using recovery (trim) or reset strategy
  defp fix_out_of_sync_tracks(tracks, storage, master, _opts) do
    # Check if all tracks have the same media_sequence_number (same timeline)
    media_sequences =
      Enum.map(tracks, fn {_id, track} -> track.media_playlist.media_sequence_number end)

    same_timeline = Enum.uniq(media_sequences) |> length() == 1

    if same_timeline do
      # RECOVERY: All tracks on same timeline, can safely trim segments
      recover_by_trimming_segments(tracks, storage, master)
    else
      # RESET: Tracks on different timelines, need complete reset
      reset_to_aligned_position(tracks, storage, master)
    end
  end

  # Recovery strategy: trim segments while maintaining same media_sequence_number
  defp recover_by_trimming_segments(tracks, storage, master) do
    segment_counts = Enum.map(tracks, fn {_id, track} -> track.segment_count end)
    min_segments = Enum.min(segment_counts)

    track_info =
      tracks
      |> Enum.map(fn {id, track} ->
        segments_to_trim = track.segment_count - min_segments
        "#{id}: #{track.segment_count} -> #{min_segments} (trimming #{segments_to_trim})"
      end)
      |> Enum.join(", ")

    Logger.warning(fn ->
      """
      #{__MODULE__}.fix_tracks/4 using RECOVERY strategy (same timeline), trimming to #{min_segments} segments:
        #{track_info}
      Note: Some content will be lost during this synchronization operation.
      """
    end)

    tracks
    |> Enum.map(fn {track_id, track} ->
      if track.segment_count > min_segments do
        trim_track_to_segment_count(track, track_id, min_segments, storage, master)
      else
        {track_id, track}
      end
    end)
    |> Map.new()
  end

  # Reset strategy: align all tracks to furthest logical position with empty playlists
  defp reset_to_aligned_position(tracks, storage, master) do
    # Find the furthest logical position across all tracks
    max_position =
      tracks
      |> Enum.map(fn {_id, track} ->
        track.media_playlist.media_sequence_number + length(track.media_playlist.segments)
      end)
      |> Enum.max()

    track_info =
      tracks
      |> Enum.map(fn {id, track} ->
        current_position =
          track.media_playlist.media_sequence_number + length(track.media_playlist.segments)

        "#{id}: position #{current_position} -> #{max_position} (seq: #{track.media_playlist.media_sequence_number}->#{max_position})"
      end)
      |> Enum.join(", ")

    Logger.warning(fn ->
      """
      #{__MODULE__}.fix_tracks/4 using RESET strategy (different timelines), aligning to position #{max_position}:
        #{track_info}
      Note: All playlist content will be cleared for complete synchronization recovery.
      """
    end)

    tracks
    |> Enum.map(fn {track_id, track} ->
      reset_track_to_position(track, track_id, max_position, storage, master)
    end)
    |> Map.new()
  end

  # Trims a single track to the specified segment count
  defp trim_track_to_segment_count(track, track_id, target_count, storage, master) do
    segments_to_remove = track.segment_count - target_count

    # Split segments to keep the most recent ones
    {segments_to_trim, remaining_segments} =
      Enum.split(track.media_playlist.segments, segments_to_remove)

    # Calculate new duration based on remaining segments
    new_duration =
      remaining_segments
      |> Enum.reduce(0.0, fn segment, acc -> acc + segment.duration end)

    # Update media sequence number to reflect removed segments
    new_media_sequence = track.media_playlist.media_sequence_number + segments_to_remove

    # Count discontinuities in removed segments to update discontinuity_sequence  
    removed_discontinuities =
      Enum.count(segments_to_trim, fn segment -> segment.discontinuity end)

    new_discontinuity_sequence =
      track.media_playlist.discontinuity_sequence + removed_discontinuities

    # Calculate the final segment count after trimming
    final_segment_count = target_count

    # Update the track
    updated_track = %{
      track
      | segment_count: final_segment_count,
        duration: new_duration,
        media_playlist: %{
          track.media_playlist
          | segments: remaining_segments,
            media_sequence_number: new_media_sequence,
            discontinuity_sequence: new_discontinuity_sequence
        }
    }

    # Write the updated playlist to storage
    playlist_uri = HLS.Playlist.build_absolute_uri(master.uri, track.media_playlist.uri)

    case Storage.put(
           storage,
           playlist_uri,
           HLS.Playlist.marshal(updated_track.media_playlist),
           max_retries: 10
         ) do
      :ok ->
        Logger.debug(fn ->
          "#{__MODULE__}.trim_track_to_segment_count/5 updated playlist for track #{track_id} (#{segments_to_remove} segments trimmed)"
        end)

      {:error, error} ->
        Logger.warning(
          "#{__MODULE__}.trim_track_to_segment_count/5 Failed to write updated playlist for track #{track_id}: #{inspect(error)}"
        )
    end

    # Clean up the trimmed segments from storage
    cleanup_segments_from_storage(
      %{storage: storage, manifest_uri: master.uri},
      segments_to_trim,
      track_id,
      updated_track.media_playlist
    )

    {track_id, updated_track}
  end

  # Resets a single track to the specified logical position with empty playlist
  defp reset_track_to_position(track, track_id, target_position, storage, master) do
    # Clean up all existing segments from storage
    cleanup_segments_from_storage(
      %{storage: storage, manifest_uri: master.uri},
      track.media_playlist.segments,
      track_id,
      # no preserved playlist since we're clearing everything
      nil,
      # don't preserve any init sections 
      false
    )

    # Create updated track with reset playlist
    updated_track = %{
      track
      | segment_count: target_position,
        duration: 0.0,
        media_playlist: %{
          track.media_playlist
          | segments: [],
            media_sequence_number: target_position,
            discontinuity_sequence: 0
        }
    }

    # Write the empty playlist to storage
    playlist_uri = HLS.Playlist.build_absolute_uri(master.uri, track.media_playlist.uri)

    case Storage.put(
           storage,
           playlist_uri,
           HLS.Playlist.marshal(updated_track.media_playlist),
           max_retries: 10
         ) do
      :ok ->
        Logger.debug(fn ->
          "#{__MODULE__}.reset_track_to_position/5 reset track #{track_id} to position #{target_position} with empty playlist"
        end)

      {:error, error} ->
        Logger.warning(
          "#{__MODULE__}.reset_track_to_position/5 Failed to write reset playlist for track #{track_id}: #{inspect(error)}"
        )
    end

    {track_id, updated_track}
  end

  defp load_tracks(storage, master, opts) do
    resume_finished_tracks = Keyword.fetch!(opts, :resume_finished_tracks)
    restore_pending_segments = Keyword.fetch!(opts, :restore_pending_segments)
    streams = Enum.concat(master.streams, master.alternative_renditions)
    playlist_type = if is_nil(opts[:max_segments]), do: :event

    streams
    |> Stream.map(fn stream ->
      %Track{
        stream: stream,
        segment_extension: nil,
        segment_count: 0,
        init_section: nil,
        duration: 0,
        media_playlist: nil,
        pending_playlist: nil
      }
    end)
    # Load media playlist
    |> async_stream_nolink(
      fn track ->
        result =
          Storage.get(
            storage,
            HLS.Playlist.build_absolute_uri(master.uri, track.stream.uri)
          )

        case result do
          {:ok, data} ->
            playlist = HLS.Playlist.unmarshal(data, %HLS.Playlist.Media{uri: track.stream.uri})
            {track, {:ok, playlist}}

          other ->
            {track, other}
        end
      end,
      concurrency: length(streams),
      timeout: :infinity,
      ordered: false
    )
    |> Stream.map(fn {:ok, {track, result}} ->
      case result do
        {:ok, media} ->
          cond do
            media.finished and not resume_finished_tracks ->
              raise HLS.Packager.PlaylistFinishedError,
                    "Cannot resume a finished media playlist: #{to_string(track.stream.uri)}"

            resume_finished_tracks ->
              media =
                media
                |> Map.replace!(:finished, false)
                |> Map.replace!(:type, playlist_type)

              %{track | media_playlist: media}

            true ->
              %{track | media_playlist: media}
          end

        {:error, :not_found} ->
          raise HLS.Packager.PlaylistNotFoundError,
            message: "Cannot load media playlist: #{to_string(track.stream.uri)}"

        {:error, error} ->
          raise HLS.Packager.ResumeError,
            message:
              "Cannot initialize current steat of playlist #{to_string(track.stream.uri)} because of error: #{inspect(error)}"
      end
    end)
    # Load pending playlist
    |> async_stream_nolink(
      fn track ->
        pending_uri = to_pending_uri(track.stream.uri)

        pending_playlist =
          with {:restore_pending, true} <- {:restore_pending, restore_pending_segments},
               {:ok, data} <-
                 Storage.get(storage, HLS.Playlist.build_absolute_uri(master.uri, pending_uri)) do
            data
            |> HLS.Playlist.unmarshal(%HLS.Playlist.Media{uri: pending_uri})
            |> Map.replace!(:finished, false)
            |> Map.replace!(:type, playlist_type)
          else
            _error ->
              %HLS.Playlist.Media{
                # TODO: Version 7 is requires for CMAF playlists.
                version: 4,
                uri: pending_uri,
                type: playlist_type,
                target_segment_duration: track.media_playlist.target_segment_duration,
                version: track.media_playlist.version
              }
          end

        %{track | pending_playlist: pending_playlist}
      end,
      concurrency: length(streams),
      timeout: :infinity,
      ordered: false
    )
    # Initializes the rest
    |> async_stream_nolink(
      fn {:ok, track} ->
        all_segments =
          track.media_playlist.segments
          |> Enum.concat(track.pending_playlist.segments)
          |> Enum.reverse()

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

        segment_count = length(all_segments) + track.media_playlist.media_sequence_number

        %{
          track
          | segment_extension: segment_extension,
            segment_count: segment_count,
            init_section: init_section,
            duration: HLS.Playlist.Media.compute_playlist_duration(track.media_playlist),
            next_sync_datetime: opts[:timeline_reference]
        }
      end,
      concurrency: length(streams),
      timeout: :infinity,
      ordered: false
    )
    |> Map.new(fn {:ok, track} ->
      track_id = uri_to_track_id(master.uri, track.stream.uri)
      {track_id, track}
    end)
  end

  defp to_pending_uri(uri), do: append_to_path(uri, "_pending")

  defp maybe_write_pending_playlist(state, playlist, opts \\ []) do
    if state.max_segments != nil do
      :ok
    else
      write_playlist(state, playlist, opts)
    end
  end

  defp write_playlist(packager, playlist, storage_opts \\ []) do
    case Storage.put(
           packager.storage,
           HLS.Playlist.build_absolute_uri(packager.manifest_uri, playlist.uri),
           HLS.Playlist.marshal(playlist),
           storage_opts
         ) do
      :ok ->
        :ok

      {:error, error} ->
        Logger.warning(
          "#{__MODULE__}.write_playlist/3 Failed to write #{to_string(playlist.uri)} because of error: #{inspect(error)}."
        )

        {:error, error}
    end
  end

  defp delete_playlist(packager, playlist, storage_opts) do
    case Storage.delete(
           packager.storage,
           HLS.Playlist.build_absolute_uri(packager.manifest_uri, playlist.uri),
           storage_opts
         ) do
      :ok ->
        :ok

      {:error, error} ->
        Logger.warning(
          "#{__MODULE__}.delete_playlist/3 Failed to delete #{to_string(playlist.uri)} because of error: #{inspect(error)}."
        )

        {:error, error}
    end
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

  # Applies sliding window logic to a playlist, removing old segments when max_segments is exceeded.
  # Returns {updated_playlist, removed_segments}.
  defp apply_sliding_window(playlist, max_segments) when is_nil(max_segments) do
    {playlist, []}
  end

  defp apply_sliding_window(playlist, max_segments) do
    segments = playlist.segments
    segment_count = length(segments)

    if segment_count > max_segments do
      segments_to_remove = segment_count - max_segments
      {removed_segments, remaining_segments} = Enum.split(segments, segments_to_remove)

      # Count discontinuities in removed segments to update discontinuity_sequence
      removed_discontinuities =
        Enum.count(removed_segments, fn segment -> segment.discontinuity end)

      updated_playlist = %{
        playlist
        | segments: remaining_segments,
          media_sequence_number: playlist.media_sequence_number + segments_to_remove,
          discontinuity_sequence: playlist.discontinuity_sequence + removed_discontinuities
      }

      {updated_playlist, removed_segments}
    else
      {playlist, []}
    end
  end

  # Removes segments and their init sections from storage.
  # When preserve_shared_init_sections is true, only deletes init sections that are not referenced by remaining segments.
  # When preserve_shared_init_sections is false, deletes all init sections (used during complete cleanup).
  defp cleanup_segments_from_storage(
         packager,
         segments_to_remove,
         track_id,
         current_media_playlist,
         preserve_shared_init_sections \\ true
       ) do
    preserved_init_uris =
      get_preserved_init_uris(current_media_playlist, preserve_shared_init_sections)

    candidate_init_uris =
      delete_segments_and_collect_init_uris(packager, segments_to_remove, track_id)

    init_uris_to_delete =
      filter_init_uris_to_delete(
        candidate_init_uris,
        preserved_init_uris,
        preserve_shared_init_sections
      )

    delete_init_sections(packager, init_uris_to_delete, track_id)
  end

  # Collects init section URIs that should be preserved from deletion.
  defp get_preserved_init_uris(media_playlist, preserve_shared_init_sections) do
    if preserve_shared_init_sections and media_playlist do
      media_playlist.segments
      |> Enum.filter(& &1.init_section)
      |> Enum.map(& &1.init_section.uri)
      |> MapSet.new()
    else
      MapSet.new()
    end
  end

  # Deletes segments from storage and returns their init section URIs.
  defp delete_segments_and_collect_init_uris(packager, segments, track_id) do
    segments
    |> Enum.reduce(MapSet.new(), fn segment, acc ->
      delete_segment_file(packager, segment, track_id)

      if segment.init_section do
        MapSet.put(acc, segment.init_section.uri)
      else
        acc
      end
    end)
  end

  # Determines which init section URIs should be deleted.
  defp filter_init_uris_to_delete(candidate_uris, preserved_uris, preserve_shared_init_sections) do
    if preserve_shared_init_sections do
      MapSet.difference(candidate_uris, preserved_uris)
    else
      candidate_uris
    end
  end

  # Deletes a single segment file from storage with error handling.
  defp delete_segment_file(packager, segment, track_id) do
    segment_uri = HLS.Playlist.Media.build_segment_uri(packager.manifest_uri, segment.uri)

    case Storage.delete(packager.storage, segment_uri, max_retries: 3) do
      :ok ->
        :ok

      {:error, error} ->
        Logger.warning(
          "#{__MODULE__}.cleanup_segments_from_storage Failed to delete segment #{to_string(segment.uri)} for track #{track_id}: #{inspect(error)}"
        )
    end
  end

  # Deletes init sections from storage with error handling and logging.
  defp delete_init_sections(packager, init_uris, track_id) do
    Enum.each(init_uris, fn init_uri ->
      full_uri = HLS.Playlist.Media.build_segment_uri(packager.manifest_uri, init_uri)

      case Storage.delete(packager.storage, full_uri, max_retries: 3) do
        :ok ->
          Logger.debug(
            "#{__MODULE__}.cleanup_segments_from_storage Deleted orphaned init section #{to_string(init_uri)} for track #{track_id}"
          )

        {:error, error} ->
          Logger.warning(
            "#{__MODULE__}.cleanup_segments_from_storage Failed to delete init section #{to_string(init_uri)} for track #{track_id}: #{inspect(error)}"
          )
      end
    end)
  end

  defp async_stream_nolink(enum, fun, opts) do
    Task.Supervisor.async_stream_nolink(HLS.Task.Supervisor, enum, fun, opts)
  end
end
