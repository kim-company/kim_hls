defmodule HLS.Packager do
  @moduledoc """
  The `HLS.Packager` module is responsible for managing and generating media and master playlists
  for HTTP Live Streaming (HLS). It handles various tasks such as loading and saving playlists,
  inserting new streams, uploading segments and maintaining synchronization points for different streams.
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
          upload_tasks_to_track: %{Task.ref() => track_id()}
        }

  defstruct [
    :master_written?,
    :storage,
    :manifest_uri,
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

  ```elixir
  HLS.Packager.start_link(
    storage: HLS.Storage.File.new(),
    manifest_uri: URI.new!("file://stream.m3u8"),
    resume_finished_tracks: false,
    restore_pending_segments: true
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
        name: nil
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
  Will force that the next added segment has an `EXT-X-DISCONTINUITY` tag.
  """
  @spec discontinue_track(GenServer.server(), track_id()) :: :ok
  def discontinue_track(packager, track_id) do
    GenServer.cast(packager, {:discontinue_track, track_id})
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
  Writes down the remaining segments and marks all playlists as finished (EXT-X-ENDLIST).
  Deletes pending playlists.
  """
  @spec flush(GenServer.server()) :: :ok
  def flush(packager) do
    GenServer.call(packager, :flush, :infinity)
  end

  @doc """
  Returns the next synchronization point which
  can then be passed to the `sync/2` function.
  """
  @spec next_sync_point(GenServer.server(), pos_integer()) :: pos_integer()
  def next_sync_point(packager, target_duration) do
    GenServer.call(packager, {:next_sync_point, target_duration})
  end

  @doc """
  Synchronizes all media playlists and writes down the master playlist as soon as needed.
  """
  @spec sync(GenServer.server(), pos_integer()) :: :ok
  def sync(packager, sync_point) do
    GenServer.cast(packager, {:sync, sync_point})
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

    case Storage.get(storage, manifest_uri, max_retries: 5) do
      {:ok, data} ->
        master = HLS.Playlist.unmarshal(data, %HLS.Playlist.Master{uri: opts[:manifest_uri]})
        load_track_opts = Keyword.take(opts, [:resume_finished_tracks, :restore_pending_segments])

        {:ok,
         %__MODULE__{
           master_written?: true,
           storage: storage,
           manifest_uri: manifest_uri,
           tracks: load_tracks(storage, master, load_track_opts)
         }}

      {:error, :not_found} ->
        {:ok,
         %__MODULE__{
           master_written?: false,
           storage: storage,
           manifest_uri: manifest_uri,
           tracks: %{}
         }}

      {:error, error} ->
        raise HLS.Packager.ResumeError,
          message: "Cannot check current state on the storage: #{inspect(error)}."
    end
  end

  @impl true
  def handle_cast({:sync, sync_point}, state) do
    state
    |> sync_playlists(sync_point)
    |> maybe_write_master(sync_point: sync_point)

    {:noreply, state}
  end

  def handle_cast({:discontinue_track, track_id}, state) do
    {:noreply,
     update_track(
       state,
       track_id,
       fn track -> %{track | discontinue_next_segment: true} end
     )}
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
        duration: duration,
        init_section: init_section,
        discontinuity: track.discontinue_next_segment
      }

    # TODO: Task.Supervisor.async_nolink?
    task =
      Task.async(fn ->
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
      end)

    {:noreply, state}
  end

  @impl true
  def handle_call({:track_variant_uri, track_id}, _from, state) do
    {:reply, build_track_variant_uri(state, track_id), state}
  end

  def handle_call({:has_track?, track_id}, _from, state) do
    {:reply, Map.has_key?(state.tracks, track_id), state}
  end

  def handle_call({:next_sync_point, target_duration}, _from, state) do
    max_duration =
      state.tracks
      |> Enum.map(fn {_id, track} -> track.duration end)
      |> Enum.max(fn -> 0 end)

    sync_point =
      max(
        ceil(max_duration / target_duration) * target_duration,
        target_duration
      )

    {:reply, sync_point, state}
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
        stream = Map.put(opts[:stream], :uri, build_track_variant_uri(state, track_id))

        media_playlist = %HLS.Playlist.Media{
          uri: stream.uri,
          target_segment_duration: opts[:target_segment_duration],
          type: :event
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

        {:reply, :ok, put_in(state, [Access.key!(:tracks), track_id], track)}
    end
  end

  def handle_call(:flush, _from, state) do
    tracks =
      state.tracks
      # finish pending uploads
      |> Stream.map(fn {id, track} ->
        track.upload_tasks
        |> Enum.map(& &1.task)
        |> Task.await_many(:infinity)

        pending_playlist = %{
          track.pending_playlist
          | segments:
              track.pending_playlist.segments ++ Enum.map(track.upload_tasks, & &1.segment)
        }

        track = %{track | pending_playlist: pending_playlist, upload_tasks: %{}}
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

        write_playlist(state, pending_playlist)

        %{track | upload_tasks: unfinished, pending_playlist: pending_playlist}
      end)

    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, ref, _, _, reason}, state)
      when is_map_key(state.upload_tasks_to_track, ref) do
    # TODO: Maybe we should write the segment in the playlist and just continue as nothing has happened?
    raise "Cannot write segment of track #{state.track_id} with reason: #{inspect(reason)}."
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
      |> Task.async_stream(
        fn {id, track} ->
          track = move_segments_until_sync_point(packager, track, sync_point)
          {id, track}
        end,
        ordered: false,
        timeout: :infinity,
        concurrency: Enum.count(packager.tracks)
      )
      |> Map.new(fn {:ok, {id, track}} -> {id, track} end)

    Logger.debug(fn ->
      track_info =
        Enum.map(tracks, fn {id, track} ->
          media_playlist_segments =
            track.segment_count - length(track.pending_playlist.segments) -
              length(track.upload_tasks)

          "#{id}: #{media_playlist_segments}/#{track.segment_count} segment published (#{Float.round(track.duration, 2)}s)"
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
      write_playlist(packager, track.media_playlist)
      write_playlist(packager, track.pending_playlist)
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

  defp maybe_write_master(packager, _opts) when packager.master_written? do
    packager
  end

  defp maybe_write_master(packager, opts) do
    opts = Keyword.validate!(opts, force: false, sync_point: nil)

    sync_point_reached? =
      if opts[:sync_point] do
        max_target_duration =
          packager.tracks
          |> Enum.map(fn {_id, track} ->
            track.media_playlist.target_segment_duration
          end)
          |> Enum.max()

        opts[:sync_point] >= max_target_duration * 3
      else
        false
      end

    all_playlists_ready? =
      Enum.all?(packager.tracks, fn {_id, track} ->
        track.duration >= track.media_playlist.target_segment_duration * 3
      end)

    if opts[:force] or all_playlists_ready? or sync_point_reached? do
      master_playlist = build_master(packager)
      :ok = write_playlist(packager, master_playlist, max_retries: 10)
      Logger.debug(fn -> "#{__MODULE__}.maybe_write_master/2 master playlist written." end)
      %{packager | master_written?: true}
    else
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

  defp load_tracks(storage, master, opts) do
    resume_finished_tracks = Keyword.fetch!(opts, :resume_finished_tracks)
    restore_pending_segments = Keyword.fetch!(opts, :restore_pending_segments)
    streams = Enum.concat(master.streams, master.alternative_renditions)

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
    |> Task.async_stream(
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
                |> Map.replace!(:type, :event)

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
    |> Task.async_stream(
      fn track ->
        pending_uri = to_pending_uri(track.stream.uri)

        pending_playlist =
          with {:restore_pending, true} <- {:restore_pending, restore_pending_segments},
               {:ok, data} <-
                 Storage.get(storage, HLS.Playlist.build_absolute_uri(master.uri, pending_uri)) do
            data
            |> HLS.Playlist.unmarshal(%HLS.Playlist.Media{uri: pending_uri})
            |> Map.replace!(:finished, false)
            |> Map.replace!(:type, :event)
          else
            _error ->
              %HLS.Playlist.Media{
                uri: pending_uri,
                type: :event,
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
    |> Task.async_stream(
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

        %{
          track
          | segment_extension: segment_extension,
            segment_count: length(all_segments),
            init_section: init_section,
            duration: HLS.Playlist.Media.compute_playlist_duration(track.media_playlist)
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
end
