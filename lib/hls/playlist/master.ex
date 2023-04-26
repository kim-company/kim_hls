defmodule HLS.Playlist.Master do
  alias HLS.VariantStream
  alias HLS.Playlist

  @type t :: %__MODULE__{
          tags: Playlist.tag_map_t(),
          version: pos_integer(),
          streams: VariantStream.t()
        }
  defstruct [:version, tags: %{}, streams: []]

  @spec variant_streams(t) :: [VariantStream.t()]
  def variant_streams(%__MODULE__{streams: streams}), do: streams
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
