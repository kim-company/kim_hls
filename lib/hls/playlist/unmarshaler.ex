defprotocol HLS.Playlist.Unmarshaler do
  alias HLS.Playlist.Tag

  @type tag_map_t :: %{required(Tag.tag_id_t() | {pos_integer(), :segment}) => [Tag.t()]}

  @doc """
  Returns a list of modules implementing the tag behaviour that are used to
  define the playlist.
  """
  @spec supported_tags(t()) :: [Tag.behaviour_t()]
  def supported_tags(playlist)

  @spec load_tags(t(), tag_map_t()) :: t()
  def load_tags(playlist, tags)
end
