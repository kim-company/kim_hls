defmodule HLS.Playlist.Tag.IndependentSegments do
	use HLS.Playlist.Tag, id: :ext_x_independent_segments

	@impl true
	def unmarshal(_data), do: true
end
