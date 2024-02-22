defmodule Support.ControlledReader do
  defstruct [:pid]

  def new(opts \\ []) do
    opts =
      Keyword.validate!(opts,
        initial: 1,
        max: 1,
        target_duration: 1
      )

    if opts[:initial] <= 0 do
      raise "Initial segments cannot be <= 0"
    end

    {:ok, pid} =
      Agent.start(fn ->
        %{
          initial: Keyword.fetch!(opts, :initial),
          max: Keyword.fetch!(opts, :max),
          calls: 0,
          target_duration: Keyword.fetch!(opts, :target_duration)
        }
      end)

    fn uri ->
      read(pid, uri)
    end
  end

  def read(pid, _) do
    config =
      Agent.get_and_update(pid, fn state ->
        {state, %{state | calls: state.calls + 1}}
      end)

    header = """
    #EXTM3U
    #EXT-X-VERSION:7
    #EXT-X-TARGETDURATION:#{config.target_duration}
    #EXT-X-MEDIA-SEQUENCE:1
    """

    calls = config.calls

    segs =
      Enum.map(Range.new(1, calls + config.initial), fn seq ->
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

    Enum.join([header] ++ segs ++ [tail], "\n")
  end
end
