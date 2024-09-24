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

  ### Managing Streams

  You can insert a new stream using the put_stream/2 function, which allows adding variant streams
  or alternative renditions to the packager. Streams can only be inserted before the master playlist has been written.

  Example:
  ```elixir
  {packager, stream_id} = Packager.put_stream(packager,
    stream: %HLS.VariantStream{
      uri: URI.new!("stream_416x234.m3u8"),
      bandwidth: 341_276,
      resolution: {416, 234},
      codecs: ["avc1.64000c", "mp4a.40.2"]
    },
    segment_extension: ".fmp4",
    target_segment_duration: 7
  )
  ```

  ### Adding the init section

  The put_init_section/3 function adds or updates the initialization section (such as an MPEG-4 ‘init’ section)
  for a stream. This section will be used for all upcoming segments and is essential for media formats like fragmented
  MP4 where an initial header is required before media segments can be played.

  If the init section has changed, it is uploaded and associated with future segments. If no payload is provided,
  the init section is removed.

  Example:
  ```elixir
  HLS.Packager.put_init_section(packager, stream_uri, init_segment_data)
  ```

  ### Adding Segments

  The put_segment/4 function allows adding a new segment to a stream. It will update the playlist with the new segment and write it to storage.

  Example:
  ```elixir
  HLS.Packager.put_segment(packager, stream_id, segment_data, 10.0)
  ```

  ### Synchronization and Flushing

  The sync/2 function synchronizes media playlists by moving segments from the pending playlist to the main playlist, ensuring that all streams are properly aligned.
  The flush/1 function writes any remaining segments and marks all playlists as finished.

  Example:
  ```elixir
  HLS.Packager.sync(packager, 14)
  HLS.Packager.flush(packager)
  ```
  """

  alias HLS.Storage

  @type t() :: %__MODULE__{
          master_written?: boolean(),
          storage: HLS.Storage.t(),
          manifest_uri: URI.t(),
          streams: %{URI.t() => StreamState.t()}
        }

  defstruct [
    :master_written?,
    :storage,
    :manifest_uri,
    streams: %{}
  ]

  defmodule StreamState do
    @type t() :: %__MODULE__{
            stream: HLS.VariantStream.t() | HLS.AlternativeRendition.t(),
            duration: float(),
            segment_count: non_neg_integer(),
            segment_extension: String.t(),
            init_section: nil | %{uri: URI.t(), payload: binary()},
            media_playlist: HLS.Playlist.Media.t(),
            pending_playlist: HLS.Playlist.Media.t()
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

    defstruct @enforce_keys
  end

  @doc """
  Initializes a new packager with a storage and its root manifest uri.

  ## Examples

      iex> HLS.Packager.new(
      ...>   storage: HLS.Storage.File.new(),
      ...>   manifest_uri: URI.new!("file://stream.m3u8")
      ...> )
  """
  def new(opts) do
    opts = Keyword.validate!(opts, [:storage, :manifest_uri])
    manifest_uri = opts[:manifest_uri]
    storage = opts[:storage]

    case Storage.get(storage, manifest_uri) do
      {:ok, data} ->
        master = HLS.Playlist.unmarshal(data, %HLS.Playlist.Master{uri: opts[:manifest_uri]})

        %__MODULE__{
          master_written?: true,
          storage: storage,
          manifest_uri: manifest_uri,
          streams: load_streams(storage, master)
        }

      {:error, :not_found} ->
        %__MODULE__{
          master_written?: false,
          storage: storage,
          manifest_uri: manifest_uri,
          streams: %{}
        }
    end
  end

  @doc """
  Inserts a new stream or updates an existing stream.

  Streams can only be updated as long as the master playlist was not written yet.
  Trying to change the resolution or bandwidth in the ready state will lead to a `HLS.Packager.UpsertError`.
  """
  def put_stream(packager, opts) do
    opts = Keyword.validate!(opts, [:stream, :segment_extension, :target_segment_duration])
    stream = opts[:stream]

    cond do
      is_map_key(packager.streams, stream.uri) ->
        # TODO: For now don't do anything.
        # later we want to validate that all the options corrispond.
        {packager, stream.uri}

      not packager.master_written? ->
        media_playlist = %HLS.Playlist.Media{
          uri: stream.uri,
          target_segment_duration: opts[:target_segment_duration]
        }

        strea_state = %StreamState{
          stream: stream,
          duration: 0.0,
          init_section: nil,
          segment_count: 0,
          media_playlist: media_playlist,
          segment_extension: opts[:segment_extension],
          pending_playlist: %{media_playlist | uri: to_pending_uri(stream.uri)}
        }

        packager = put_in(packager, [Access.key!(:streams), stream.uri], strea_state)
        {packager, stream.uri}

      packager.master_written? ->
        raise HLS.Packager.UpsertError, message: "Cannot add a new stream in the ready state."
    end
  end

  @doc """
  Puts a new init section that will be used for all upcoming segments.
  """
  def put_init_section(packager, stream_id, payload) do
    stream_state = Map.fetch!(packager.streams, stream_id)

    extname =
      case stream_state.segment_extension do
        ".mp4" -> ".mp4"
        ".fmp4" -> ".mp4"
        other -> raise "Init section is not supported for #{other} segments."
      end

    init_section =
      cond do
        is_nil(payload) ->
          nil

        is_nil(stream_state.init_section) or stream_state.init_section.payload != payload ->
          next_index = stream_state.segment_count + 1
          segment_uri = relative_segment_uri(stream_state.media_playlist.uri, extname, next_index)
          uri = append_to_path(segment_uri, "_init")

          :ok =
            Storage.put(
              packager.storage,
              HLS.Playlist.Media.build_segment_uri(packager.manifest_uri, uri),
              payload
            )

          %{uri: uri, payload: payload}

        true ->
          stream_state.init_section
      end

    put_in(
      packager,
      [Access.key!(:streams), stream_id, Access.key!(:init_section)],
      init_section
    )
  end

  @doc """
  Adds a new segment into the playlist.
  """
  def put_segment(packager, stream_id, payload, duration) do
    stream_state = Map.fetch!(packager.streams, stream_id)
    stream_uri = stream_state.media_playlist.uri
    next_index = stream_state.segment_count + 1
    segment_uri = relative_segment_uri(stream_uri, stream_state.segment_extension, next_index)

    init_section =
      if stream_state.init_section do
        %{uri: stream_state.init_section[:uri], byterange: nil}
      end

    # Create a new segment
    segment = %HLS.Segment{
      uri: segment_uri,
      duration: duration,
      init_section: init_section
    }

    # Upload the segment
    # TODO: Create an async API for it.
    :ok =
      Storage.put(
        packager.storage,
        HLS.Playlist.Media.build_segment_uri(packager.manifest_uri, segment.uri),
        payload
      )

    # Add segment to the pending playlist and increment the segment count.
    packager =
      packager
      |> update_in(
        [Access.key!(:streams), stream_uri, Access.key!(:pending_playlist)],
        fn playlist -> Map.update!(playlist, :segments, &(&1 ++ [segment])) end
      )
      |> update_in(
        [Access.key!(:streams), stream_uri, Access.key!(:segment_count)],
        &(&1 + 1)
      )

    pending_playlist =
      get_in(packager, [Access.key!(:streams), stream_uri, Access.key!(:pending_playlist)])

    :ok = write_playlist(packager, pending_playlist)

    packager
  end

  @doc """
  Returns the next synchronization point which
  can then be passed to the `sync/2` function.
  """
  def next_sync_point(packager, target_duration) do
    max_duration =
      packager.streams
      |> Enum.map(fn {_id, stream} -> stream.duration end)
      |> Enum.max()

    :math.ceil(max_duration / target_duration) * target_duration
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
  Writes down the remaining segments and marks all playlists as `is_finished`.
  Deletes pending playlists.
  """
  def flush(packager) do
    streams =
      Map.new(packager.streams, fn {id, stream} ->
        if Enum.any?(stream.pending_playlist.segments) do
          pending_duration = HLS.Playlist.Media.compute_playlist_duration(stream.pending_playlist)

          stream = %{
            stream
            | duration: stream.duration + pending_duration,
              media_playlist: %{
                stream.media_playlist
                | segments: stream.media_playlist.segments ++ stream.pending_playlist.segments,
                  finished: true
              },
              pending_playlist: %{stream.pending_playlist | segments: [], finished: true}
          }

          :ok = write_playlist(packager, stream.media_playlist)
          :ok = write_playlist(packager, stream.pending_playlist)

          {id, stream}
        else
          {id, stream}
        end
      end)

    packager
    |> Map.replace!(:streams, streams)
    |> maybe_write_master(force: true)
  end

  @doc """
  Builds the master playlist of the given packager.
  """
  def build_master(packager) do
    streams =
      packager.streams
      |> Map.values()
      |> Enum.map(& &1.stream)

    %HLS.Playlist.Master{
      version: 4,
      uri: packager.manifest_uri,
      independent_segments: true,
      streams: Enum.filter(streams, &is_struct(&1, HLS.VariantStream)),
      alternative_renditions: Enum.filter(streams, &is_struct(&1, HLS.AlternativeRendition))
    }
  end

  @doc """
  Generates a new variant URI from an existing packager.

  ## Examples

      iex> packager = HLS.Packager.new(
      ...>   storage: HLS.Storage.File.new(),
      ...>   manifest_uri: URI.new!("file://stream.m3u8")
      ...> )
      iex> HLS.Packager.new_variant_uri(packager, "_video_480p")
      URI.new!("stream_video_480p.m3u8")
  """
  def new_variant_uri(packager, suffix) do
    packager.manifest_uri
    |> append_to_path(suffix)
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
    streams =
      Map.new(packager.streams, fn {id, stream} ->
        updated_stream = move_segments_until_sync_point(packager, stream, sync_point)
        {id, updated_stream}
      end)

    %{packager | streams: streams}
  end

  defp move_segments_until_sync_point(packager, stream, sync_point) do
    {moved_segments, remaining_segments, new_duration} =
      Enum.reduce_while(
        stream.pending_playlist.segments,
        {[], [], stream.duration},
        fn segment, {moved, remaining, duration} ->
          new_duration = duration + segment.duration

          # TODO: Check if this is the behaviour we want to have.
          if new_duration <= sync_point do
            {:cont, {moved ++ [segment], remaining, new_duration}}
          else
            {:halt, {moved, [segment | remaining], duration}}
          end
        end
      )

    stream = %{
      stream
      | duration: new_duration,
        media_playlist: %{
          stream.media_playlist
          | segments: stream.media_playlist.segments ++ moved_segments
        },
        pending_playlist: %{stream.pending_playlist | segments: remaining_segments}
    }

    if Enum.any?(moved_segments) do
      :ok = write_playlist(packager, stream.media_playlist)
      :ok = write_playlist(packager, stream.pending_playlist)
    end

    stream
  end

  defp maybe_write_master(packager, opts \\ []) do
    opts = Keyword.validate!(opts, force: false)

    if packager.master_written? do
      packager
    else
      all_playlists_ready? =
        Enum.all?(packager.streams, fn {_uri, stream_state} ->
          stream_state.duration >= stream_state.media_playlist.target_segment_duration * 3
        end)

      if opts[:force] or all_playlists_ready? do
        master_playlist = build_master(packager)
        :ok = write_playlist(packager, master_playlist)
        %{packager | master_written?: true}
      else
        packager
      end
    end
  end

  defp load_streams(storage, master) do
    all_streams = Enum.concat(master.streams, master.alternative_renditions)

    Enum.reduce(all_streams, %{}, fn stream, acc ->
      media_playlist =
        case Storage.get(storage, HLS.Playlist.build_absolute_uri(master.uri, stream.uri)) do
          {:ok, data} ->
            media = HLS.Playlist.unmarshal(data, %HLS.Playlist.Media{uri: stream.uri})

            if media.finished do
              raise HLS.Packager.PlaylistFinishedError,
                message: "Cannot resume a finished media playlist: #{to_string(stream.uri)}"
            end

            media

          {:error, :not_found} ->
            raise HLS.Packager.PlaylistNotFoundError,
              message: "Cannot load media playlist: #{to_string(stream.uri)}"
        end

      pending_uri = to_pending_uri(stream.uri)

      pending_playlist =
        case Storage.get(storage, HLS.Playlist.build_absolute_uri(master.uri, pending_uri)) do
          {:ok, data} ->
            HLS.Playlist.unmarshal(data, %HLS.Playlist.Media{uri: pending_uri})

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
              HLS.Playlist.Media.build_segment_uri(master.uri, last_segment.init_section)
            )

          %{uri: last_segment.init_section, payload: payload}
        end

      new_stream = %StreamState{
        stream: stream,
        segment_extension: segment_extension,
        segment_count: length(all_segments),
        init_section: init_section,
        duration: HLS.Playlist.Media.compute_playlist_duration(media_playlist),
        media_playlist: media_playlist,
        pending_playlist: pending_playlist
      }

      Map.put(acc, stream.uri, new_stream)
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
end
