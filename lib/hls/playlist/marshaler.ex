defprotocol HLS.Playlist.Marshaler do
  @spec marshal(t()) :: binary()
  def marshal(playlist)
end
