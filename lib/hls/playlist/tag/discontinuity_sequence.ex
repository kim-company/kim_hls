defmodule HLS.Playlist.Tag.DiscontinuitySequence do
  use HLS.Playlist.Tag, id: :ext_x_discontinuity_sequence

  @impl true
  def unmarshal(data) do
    capture_value!(data, "([0-9]+)", &String.to_integer/1)
  end
end