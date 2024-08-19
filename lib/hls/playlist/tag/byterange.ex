defmodule HLS.Playlist.Tag.Byterange do
  use HLS.Playlist.Tag, id: :ext_x_byterange

  @impl true
  def unmarshal(data), do: capture_value!(data, ~s/\\d+@\\d+/, & &1)
end
