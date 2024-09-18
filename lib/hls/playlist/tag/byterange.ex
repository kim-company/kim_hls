defmodule HLS.Playlist.Tag.Byterange do
  use HLS.Playlist.Tag, id: :ext_x_byterange

  @impl true
  def unmarshal(data), do: capture_value!(data, ~s/\\d+@\\d+/, &parse_byterange/1)

  def parse_byterange(value) do
    case String.split(value, "@") do
      [length, offset] ->
        %{length: String.to_integer(length), offset: String.to_integer(offset)}

      [length] ->
        %{length: String.to_integer(length), offset: nil}
    end
  end

  def marshal(data) do
    if data.offset do
      "#{data.length}@#{data.offset}"
    else
      to_string(data.length)
    end
  end
end
