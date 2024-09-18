defmodule HLS.Playlist.Tag.Map do
  use HLS.Playlist.Tag, id: :ext_x_map
  alias HLS.Playlist.Tag.Byterange

  @impl true
  def unmarshal(line) do
    line
    |> capture_attribute_list!(fn
      "URI", value ->
        {:uri, value}

      "BYTERANGE", value ->
        {:byterange, Byterange.parse_byterange(value)}

      _key, _value ->
        :skip
    end)
  end

  def marshal_uri_and_byterange(data) do
    [
      data.uri && "URI=\"#{data.uri}\"",
      data.byterange && "BYTERANGE=\"#{Byterange.marshal(data.byterange)}\""
    ]
    |> Enum.reject(&is_nil/1)
    |> Enum.join(",")
  end
end
