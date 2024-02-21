defmodule HLS.Playlist.Media.Tracker do
  use GenServer

  alias HLS.Playlist.Media
  alias HLS.{Segment, Playlist}

  defstruct [:reader, :media_playlist_uri, following: %{}]

  @type target_t :: URI.t()

  @type reader_fun :: (URI.t() -> binary())

  @max_initial_live_segments 3

  defmodule Tracking do
    defstruct [:ref, :target, :follower, started: false, next_seq: 0]
  end

  def initial_live_buffer_size(), do: @max_initial_live_segments

  @spec start_link(reader_fun(), Keyword.t()) :: GenServer.on_start()
  def start_link(reader, opts \\ []) do
    GenServer.start_link(__MODULE__, reader, opts)
  end

  @spec stop(pid()) :: :ok
  def stop(pid), do: GenServer.stop(pid)

  @spec follow(pid(), target_t()) :: reference()
  def follow(pid, target) do
    GenServer.call(pid, {:follow, target})
  end

  @impl true
  def init(reader) do
    {:ok, %__MODULE__{reader: reader}}
  end

  @impl true
  def handle_call({:follow, target}, {from, _}, state) do
    tracking = %Tracking{ref: make_ref(), target: target, follower: from}
    following = Map.put(state.following, tracking.ref, tracking)
    state = %__MODULE__{state | following: following}
    {:reply, tracking.ref, state, {:continue, {:refresh, tracking}}}
  end

  @impl true
  def handle_continue({:refresh, tracking}, state) do
    handle_refresh(tracking, state)
  end

  @impl true
  def handle_info({:refresh, tracking}, state) do
    handle_refresh(tracking, state)
  end

  defp read_media_playlist(reader, uri) do
    raw_playlist = reader.(uri)
    Playlist.unmarshal(raw_playlist, %Media{uri: uri})
  end

  defp handle_refresh(tracking, state) do
    uri = tracking.target
    playlist = read_media_playlist(state.reader, uri)
    segs = Media.segments(playlist)

    if length(segs) < @max_initial_live_segments and !playlist.finished do
      # wait for some more segments to be listed in the playlist to
      # avoid playback stalls.
      wait = playlist.target_segment_duration * 1_000
      Process.send_after(self(), {:refresh, tracking}, wait)
      {:noreply, state}
    else
      send_updates(tracking, state, playlist)
    end
  end

  defp send_updates(tracking, state, playlist) do
    segs = Media.segments(playlist)

    # Determine initial sequence number, sending the start_of_track message if
    # needed.
    tracking =
      if tracking.started do
        tracking
      else
        next_seq =
          if !playlist.finished && length(segs) > @max_initial_live_segments do
            playlist.media_sequence_number + (length(segs) - @max_initial_live_segments)
          else
            playlist.media_sequence_number
          end

        send(tracking.follower, {:start_of_track, tracking.ref, next_seq})
        %Tracking{tracking | next_seq: next_seq, started: true}
      end

    {segs, last_seq} =
      segs
      |> Enum.map_reduce(0, fn seg, _ ->
        seg = Segment.update_absolute_sequence(seg, playlist.media_sequence_number)
        {seg, seg.absolute_sequence}
      end)

    # Send new segments only
    segs
    |> Enum.filter(fn seg -> seg.absolute_sequence >= tracking.next_seq end)
    |> Enum.map(fn seg -> {:segment, tracking.ref, seg} end)
    |> Enum.each(&send(tracking.follower, &1))

    # Schedule a new refresh if needed
    tracking = %Tracking{tracking | next_seq: last_seq + 1}

    if playlist.finished do
      send(tracking.follower, {:end_of_track, tracking.ref})
    else
      wait = playlist.target_segment_duration * 1_000
      Process.send_after(self(), {:refresh, tracking}, wait)
    end

    following = Map.put(state.following, tracking.ref, tracking)
    {:noreply, %__MODULE__{state | following: following}}
  end
end
