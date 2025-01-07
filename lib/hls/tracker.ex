defmodule HLS.Tracker do
  use GenServer

  alias HLS.Playlist.Media
  alias HLS.{Segment, Playlist, Storage}

  @max_initial_live_segments 3

  defstruct [
    :ref,
    :owner,
    :monitor,
    :storage,
    :uri,
    :timer,
    started: false,
    next_seq: 0
  ]

  def initial_live_buffer_size(), do: @max_initial_live_segments

  @spec start_link(Keyword.t()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    opts = Keyword.validate!(opts, [:name, :media_playlist_uri, :storage, :owner, :ref])
    {server_opts, opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, opts, server_opts)
  end

  @impl true
  def init(opts) do
    uri = Keyword.fetch!(opts, :media_playlist_uri)
    storage = Keyword.fetch!(opts, :storage)
    owner = Keyword.fetch!(opts, :owner)

    if Process.alive?(owner) do
      ref = Keyword.fetch!(opts, :ref)

      state = %__MODULE__{
        uri: uri,
        storage: storage,
        owner: owner,
        ref: ref,
        monitor: Process.monitor(owner)
      }

      {:ok, state, {:continue, :refresh}}
    else
      :ignore
    end
  end

  @impl true
  def handle_continue(:refresh, state) do
    handle_refresh(state)
  end

  @impl true
  def handle_info(:refresh, state) do
    handle_refresh(state)
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state = %__MODULE__{monitor: ref}) do
    {:stop, reason, state}
  end

  defp read_media_playlist(storage, uri) do
    raw_playlist = read!(storage, uri)
    Playlist.unmarshal(raw_playlist, %Media{uri: uri})
  end

  defp handle_refresh(state) do
    playlist = read_media_playlist(state.storage, state.uri)
    segs = Media.segments(playlist)

    if length(segs) < @max_initial_live_segments and !playlist.finished do
      # wait for some more segments to be listed in the playlist to
      # avoid playback stalls.
      wait = playlist.target_segment_duration * 1_000
      {:noreply, schedule_refresh_after(state, wait)}
    else
      send_updates(state, playlist)
    end
  end

  defp send_updates(state, playlist) do
    segs = Media.segments(playlist)

    # Determine initial sequence number, sending the start_of_track message if
    # needed.
    state =
      if state.started do
        state
      else
        next_seq =
          if !playlist.finished && length(segs) > @max_initial_live_segments do
            playlist.media_sequence_number + (length(segs) - @max_initial_live_segments)
          else
            playlist.media_sequence_number
          end

        send(state.owner, {:start_of_track, state.ref, next_seq})

        state
        |> put_in([Access.key!(:next_seq)], next_seq)
        |> put_in([Access.key!(:started)], true)
      end

    {segs, last_seq} =
      segs
      |> Enum.map_reduce(0, fn seg, _ ->
        seg = Segment.update_absolute_sequence(seg, playlist.media_sequence_number)
        {seg, seg.absolute_sequence}
      end)

    # Send new segments only
    segs
    |> Enum.filter(fn seg -> seg.absolute_sequence >= state.next_seq end)
    |> Enum.map(fn seg ->
      {:segment, state.ref, seg}
    end)
    |> Enum.each(&send(state.owner, &1))

    # Schedule a new refresh if needed
    state =
      state
      |> put_in([Access.key!(:next_seq)], last_seq + 1)

    if playlist.finished do
      send(state.owner, {:end_of_track, state.ref})
      {:stop, :normal, state}
    else
      wait = playlist.target_segment_duration * 1_000
      {:noreply, schedule_refresh_after(state, wait)}
    end
  end

  defp schedule_refresh_after(state, wait) do
    timer = Process.send_after(self(), :refresh, wait)
    put_in(state, [Access.key!(:timer)], timer)
  end

  defp read!(storage, uri) do
    case Storage.get(storage, uri) do
      {:ok, binary} -> binary
      {:error, :not_found} -> raise RuntimeError, "Read error on #{inspect(uri)}: not found"
      {:error, error} -> raise RuntimeError, "Read error on #{inspect(uri)}: #{inspect(error)}"
    end
  end
end
