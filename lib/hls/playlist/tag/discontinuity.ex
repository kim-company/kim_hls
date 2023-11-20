defmodule HLS.Playlist.Tag.Discontinuity do
  use HLS.Playlist.Tag, id: :ext_x_discontinuity

  @impl true
  def unmarshal(_data), do: true
end
