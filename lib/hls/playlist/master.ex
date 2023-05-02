defmodule HLS.Playlist.Master do
  alias HLS.VariantStream
  alias HLS.Playlist

  @type t :: %__MODULE__{
          uri: URI.t() | nil,
          tags: Playlist.tag_map_t(),
          version: pos_integer(),
          streams: VariantStream.t()
        }
  defstruct [:version, :uri, tags: %{}, streams: []]

  @doc """
  Returns the variant streams of this playlist. If the master playlist is
  equipped with an uri, creates the absolute uris of the variant streams that
  can be used to fetch the playlist.
  """
  @spec variant_streams(t) :: [VariantStream.t()]
  def variant_streams(master = %__MODULE__{streams: streams}) do
    streams
    |> Enum.map(fn stream ->
      %VariantStream{stream | uri: build_media_uri(master.uri, stream.uri)}
    end)
  end

  @doc """
  Builds playlist's uri relative to its master playlist.
  """
  @spec build_media_uri(URI.t(), URI.t()) :: URI.t()
  def build_media_uri(master_uri = %URI{path: master_path}, %URI{path: relative_path}) do
    %URI{master_uri | path: Path.join([Path.dirname(master_path), relative_path])}
  end

  def build_media_uri(nil, media_uri), do: media_uri
end

defimpl HLS.Playlist.Unmarshaler, for: HLS.Playlist.Master do
  alias HLS.AlternativeRendition
  alias HLS.Playlist.Tag
  alias HLS.VariantStream

  @impl true
  def supported_tags(_) do
    [
      Tag.Version,
      Tag.VariantStream,
      Tag.AlternativeRendition
    ]
  end

  @impl true
  def load_tags(playlist, tags) do
    [version] = Map.fetch!(tags, Tag.Version.id())

    renditions =
      tags
      |> Map.get(Tag.AlternativeRendition.id(), [])
      |> Enum.map(&AlternativeRendition.from_tag(&1))

    streams =
      tags
      |> Map.get(Tag.VariantStream.id(), [])
      |> Enum.map(&VariantStream.from_tag(&1))
      |> Enum.map(&VariantStream.maybe_associate_alternative_rendition(&1, renditions))

    %HLS.Playlist.Master{playlist | tags: tags, version: version.value, streams: streams}
  end
end
