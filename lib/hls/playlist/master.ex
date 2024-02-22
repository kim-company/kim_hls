defmodule HLS.Playlist.Master do
  alias HLS.VariantStream
  alias HLS.AlternativeRendition

  @type t :: %__MODULE__{
          uri: URI.t() | nil,
          version: pos_integer(),
          streams: VariantStream.t(),
          independent_segments: boolean(),
          alternative_renditions: [AlternativeRendition.t()]
        }
  defstruct [:version, :independent_segments, :uri, streams: [], alternative_renditions: []]

  @doc """
  Returns the variant streams of this playlist. If the master playlist is
  equipped with an uri, creates the absolute uris of the variant streams that
  can be used to fetch the playlist. Associates any alternative rendition
  to each stream.
  """
  @spec variant_streams(t) :: [VariantStream.t()]
  def variant_streams(master = %__MODULE__{streams: streams}) do
    streams
    |> Enum.map(fn stream ->
      uri = build_media_uri(master.uri, stream.uri)
      %VariantStream{stream | uri: uri}
    end)
  end

  @doc """
  Use this function to obtain the alternative renditions for a stream. If
  the playlist has an uri, alternative rendition's uri will be updated such
  that it is fetchable.
  """
  @spec filter_alternative_renditions_for_stream(VariantStream.t(), t()) :: [
          AlternativeRendition.t()
        ]
  def filter_alternative_renditions_for_stream(stream, master) do
    group_ids = VariantStream.associated_group_ids(stream)

    master.alternative_renditions
    |> Enum.filter(fn rendition -> Enum.member?(group_ids, rendition.group_id) end)
    |> Enum.map(fn rendition ->
      uri = build_media_uri(master.uri, rendition.uri)
      %AlternativeRendition{rendition | uri: uri}
    end)
  end

  @doc """
  Builds playlist's uri relative to its master playlist.
  """
  @spec build_media_uri(URI.t(), URI.t()) :: URI.t()
  def build_media_uri(master_uri, media_uri), do: HLS.Helper.merge_uri(master_uri, media_uri)

  def add_alternative_renditions(master, alternatives) do
    %__MODULE__{master | alternative_renditions: master.alternative_renditions ++ alternatives}
  end
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
      Tag.AlternativeRendition,
      Tag.IndependentSegments
    ]
  end

  @impl true
  def load_tags(playlist, tags) do
    [version] = Map.get(tags, Tag.Version.id(), [%{value: 1}])
    independent_segments = Map.get(tags, Tag.IndependentSegments.id(), false)

    alternatives =
      tags
      |> Map.get(Tag.AlternativeRendition.id(), [])
      |> Enum.map(&AlternativeRendition.from_tag(&1))

    streams =
      tags
      |> Map.get(Tag.VariantStream.id(), [])
      |> Enum.map(&VariantStream.from_tag(&1))

    %HLS.Playlist.Master{
      playlist
      | version: version.value,
        streams: streams,
        alternative_renditions: alternatives,
        independent_segments: independent_segments
    }
  end
end

defimpl HLS.Playlist.Marshaler, for: HLS.Playlist.Master do
  alias HLS.Playlist.Tag
  alias HLS.VariantStream
  alias HLS.AlternativeRendition

  def marshal(playlist) do
    [
      "#EXTM3U",
      "#EXT-X-VERSION:#{playlist.version}",
      playlist.independent_segments && "#EXT-X-INDEPENDENT-SEGMENTS",
      marshal_stream_inf(playlist.streams),
      marshal_media(playlist.alternative_renditions)
    ]
    |> List.flatten()
    |> Enum.reject(&is_nil/1)
    |> Enum.join("\n")
  end

  defp marshal_stream_inf(streams) do
    tags =
      streams
      |> Enum.map(&VariantStream.to_tag/1)
      |> List.flatten()

    marshal_tags(tags, fn tag ->
      {uri, attributes} = Map.pop(tag.attributes, :uri)
      value = marshal_attributes(attributes)

      [Tag.marshal(tag, value), to_string(uri)]
    end)
  end

  defp marshal_media(alternatives) do
    tags =
      alternatives
      |> Enum.map(&AlternativeRendition.to_tag/1)
      |> List.flatten()

    marshal_tags(tags, fn tag ->
      value = marshal_attributes(tag.attributes)

      Tag.marshal(tag, value)
    end)
  end

  defp marshal_tags(tags, fun) do
    tags
    |> List.flatten()
    |> Enum.flat_map(fn tag -> tag |> fun.() |> List.wrap() end)
  end

  defp marshal_attributes(attributes) do
    attributes
    |> Enum.sort()
    |> Enum.map(&prepare_attributes/1)
    |> Enum.map_join(",", fn {key, value} ->
      value =
        case value do
          string when is_binary(string) and key != "TYPE" -> "\"#{string}\""
          true -> "YES"
          false -> "NO"
          other -> other
        end

      "#{key}=#{value}"
    end)
  end

  defp prepare_attributes({:resolution, {x, y}}), do: {"RESOLUTION", ~c"#{x}x#{y}"}
  defp prepare_attributes({:codecs, codecs}), do: {"CODECS", Enum.join(codecs, ",")}
  defp prepare_attributes({:type, type}), do: {"TYPE", String.upcase(to_string(type))}
  defp prepare_attributes({:uri, uri}), do: {"URI", to_string(uri)}
  defp prepare_attributes({:characteristics, list}), do: {"CHARACTERISTICS", Enum.join(list, ",")}

  defp prepare_attributes({:frame_rate, framerate}),
    do: {"FRAME-RATE", :erlang.float_to_list(framerate, decimals: 3)}

  defp prepare_attributes({key, value}) do
    key =
      key
      |> to_string()
      |> String.upcase()
      |> String.replace("_", "-")

    {key, value}
  end
end
