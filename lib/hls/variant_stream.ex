defmodule HLS.VariantStream do
  alias HLS.Playlist.Tag

  @type t :: %__MODULE__{
          uri: nil | URI.t(),
          bandwidth: pos_integer() | nil,
          average_bandwidth: pos_integer() | nil,
          codecs: [String.t()],
          resolution: {pos_integer(), pos_integer()},
          frame_rate: float(),
          hdcp_level: :none | :type_0,
          audio: Tag.group_id_t(),
          video: Tag.group_id_t(),
          subtitles: Tag.group_id_t(),
          closed_captions: Tag.group_id_t()
        }

  @mandatory_keys [:codecs]
  @enforce_keys @mandatory_keys
  @optional_keys [
    :uri,
    :bandwidth,
    :average_bandwidth,
    :resolution,
    :frame_rate,
    :audio,
    :video,
    :subtitles,
    :closed_captions,
    hdcp_level: :none
  ]
  defstruct @enforce_keys ++ @optional_keys

  def from_tag(%Tag{id: id, attributes: attrs}) do
    if id != Tag.VariantStream.id() do
      raise ArgumentError, "Cannot convert tag #{inspect(id)} to a variant stream instance"
    end

    mandatory =
      @mandatory_keys
      |> Enum.map(fn key -> {key, Map.fetch!(attrs, key)} end)
      |> Enum.into(%{})

    optional =
      @optional_keys
      |> Enum.reduce(%{}, fn key, acc ->
        case Map.get(attrs, key) do
          nil -> acc
          value -> Map.put(acc, key, value)
        end
      end)

    struct(__MODULE__, Map.merge(optional, mandatory))
  end

  def to_tag(stream) do
    data = Map.from_struct(stream)

    attrs =
      @mandatory_keys
      |> Enum.map(fn k -> {k, Map.fetch!(data, k)} end)
      |> Enum.into(%{})

    attrs =
      @optional_keys
      |> Enum.reduce(attrs, fn key, acc ->
        case Map.get(data, key) do
          nil -> acc
          value -> Map.put(acc, key, value)
        end
      end)

    %Tag{id: Tag.VariantStream.id(), class: :master_playlist, attributes: attrs}
  end

  def associated_group_ids(stream) do
    [:video, :audio, :subtitles, :closed_captions]
    |> Enum.reduce([], fn rendition_type, acc ->
      case Map.get(stream, rendition_type) do
        nil -> acc
        id when is_binary(id) -> [id | acc]
      end
    end)
  end
end
