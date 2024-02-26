defmodule HLS.AlternativeRendition do
  alias HLS.Playlist.Tag

  @type type_t :: :subtitles | :audio | :video | :cc
  @type t :: %__MODULE__{
          type: type_t(),
          group_id: Tag.group_id_t(),
          name: String.t(),
          uri: URI.t(),
          language: String.t(),
          assoc_language: String.t(),
          autoselect: boolean(),
          forced: boolean(),
          default: boolean(),
          instream_id: String.t(),
          channels: String.t(),
          characteristics: [String.t()]
        }

  @mandatory_keys [:type, :name]
  @enforce_keys @mandatory_keys
  @optional_keys [
    # Actually mandatory at marshal time.
    :group_id,
    :uri,
    :language,
    :assoc_language,
    :autoselect,
    :default,
    :forced,
    :instream_id,
    :channels,
    :characteristics
  ]

  defstruct @enforce_keys ++ @optional_keys

  def default_group_for_type(type) do
    type
    |> to_string()
    |> String.upcase()
  end

  def from_tag(%Tag{id: id, attributes: attrs}) do
    if id != Tag.AlternativeRendition.id() do
      raise ArgumentError, "Cannot convert tag #{inspect(id)} to a alternative rendition instance"
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

  def to_tag(alternative) do
    attributes =
      alternative
      |> Map.from_struct()
      |> Enum.filter(fn {_, val} -> val != nil end)

    %Tag{id: :ext_x_media, class: :master_playlist, attributes: attributes, sequence: 0}
  end
end
