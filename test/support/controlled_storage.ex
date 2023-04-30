defmodule Support.ControlledStorage do
  defstruct [:pid, :master_playlist_uri]

  def new(opts \\ []) do
    opts =
      Keyword.validate!(opts, [
        :initial,
        max: 1,
        target_duration: 1,
        master_playlist_uri: URI.new!("master.m3u8")
      ])

    {:ok, pid} =
      Agent.start(fn ->
        %{
          initial: Keyword.fetch!(opts, :initial),
          max: Keyword.fetch!(opts, :max),
          calls: 0,
          target_duration: Keyword.fetch!(opts, :target_duration)
        }
      end)

    %__MODULE__{pid: pid, master_playlist_uri: Keyword.fetch!(opts, :master_playlist_uri)}
  end
end

defimpl HLS.Storage, for: Support.ControlledStorage do
  alias Support.ControlledStorage, as: Mock

  @media_track_path "one_more.m3u8"

  @impl true
  def read(%Mock{master_playlist_uri: playlist}, playlist, _) do
    {:ok,
     """
     #EXTM3U
     #EXT-X-VERSION:7
     #EXT-X-INDEPENDENT-SEGMENTS
     #EXT-X-STREAM-INF:BANDWIDTH=725435,CODECS="avc1.42e00a"
     #{@media_track_path}
     """}
  end

  @impl true
  def read(%Mock{pid: pid}, _, _) do
    config =
      Agent.get_and_update(pid, fn state ->
        {state, %{state | calls: state.calls + 1}}
      end)

    header = """
    #EXTM3U
    #EXT-X-VERSION:7
    #EXT-X-TARGETDURATION:#{config.target_duration}
    #EXT-X-MEDIA-SEQUENCE:0
    """

    calls = config.calls

    segs =
      Enum.map(Range.new(0, calls + config.initial), fn seq ->
        """
        #EXTINF:0.89,
        video_segment_#{seq}_video_720x480.ts
        """
      end)

    tail =
      if calls == config.max do
        "#EXT-X-ENDLIST"
      else
        ""
      end

    {:ok, Enum.join([header] ++ segs ++ [tail], "\n")}
  end

  @impl true
  def exists?(_, _), do: true

  @impl true
  def write(_, _, _, _) do
    raise "Not implemented"
  end
end
