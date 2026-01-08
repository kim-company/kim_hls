defmodule HLS.Playlist.Tag do
  @type group_id_t :: String.t()

  @type tag_class_t ::
          :media_segment
          | :media_playlist
          | :master_playlist
          | :playlist
          | :master_or_media_playlist

  # appear in both master and media playlists
  @type tag_id_t ::
          :ext_m3u
          | :ext_x_version

          # media segments tags
          | :extinf
          | :ext_x_byterange
          | :ext_x_discontinuity
          | :ext_x_key
          | :ext_x_map
          | :ext_x_program_date_time
          | :ext_x_daterange

          # media playlist tags
          | :ext_x_targetduration
          | :ext_x_media_sequence
          | :ext_x_discontinuity_sequence
          | :ext_x_endlist
          | :ext_x_playlist_type
          | :ext_x_i_frames_only

          # master playlist tags
          | :ext_x_media
          | :ext_x_stream_inf
          | :ext_x_i_frame_stream_inf
          | :ext_x_session_data
          | :ext_x_session_key

          # appear in either master or media playlist, not both
          | :ext_x_independent_segments
          | :ext_x_start

  @type attribute_list_t :: %{required(atom()) => any()}

  @type t :: %__MODULE__{
          id: tag_id_t,
          class: tag_class_t(),
          sequence: pos_integer(),
          value: any(),
          attributes: attribute_list_t
        }

  @type behaviour_t :: module()

  @callback match?(String.t()) :: boolean()
  @callback unmarshal(String.t() | [String.t()]) :: attribute_list_t() | any()
  @callback init(attribute_list_t() | any(), pos_integer()) :: t()
  @callback is_multiline?() :: boolean()
  @callback id() :: tag_id_t()
  @callback has_uri?() :: boolean()

  @enforce_keys [:id, :class]
  defstruct @enforce_keys ++ [:value, sequence: 0, attributes: []]

  defmacro __using__(id: tag_id) do
    quote do
      @behaviour HLS.Playlist.Tag
      alias HLS.Playlist.Tag

      @impl true
      def id(), do: unquote(tag_id)

      @impl true
      def is_multiline?(), do: false

      @impl true
      def match?(line) do
        prefix = Tag.marshal_id(unquote(tag_id))
        String.starts_with?(line, prefix)
      end

      @impl true
      def has_uri?(), do: false

      @impl true
      def init(attribute_list, sequence) when is_map(attribute_list) do
        %Tag{
          id: id(),
          class: Tag.class_from_id(id()),
          sequence: sequence,
          attributes: attribute_list
        }
      end

      def init(value, sequence) do
        %Tag{
          id: id(),
          class: Tag.class_from_id(id()),
          sequence: sequence,
          value: value
        }
      end

      @spec capture_value!(String.t(), String.t(), (String.t() -> any)) :: any
      def capture_value!(data, match_pattern, parser_fun) do
        Tag.capture_value!(data, unquote(tag_id), match_pattern, parser_fun)
      end

      def capture_attribute_list!(data, field_parser_fun) do
        Tag.capture_attribute_list!(data, unquote(tag_id), field_parser_fun)
      end

      defoverridable HLS.Playlist.Tag
    end
  end

  @spec marshal_id(tag_id_t()) :: String.t()
  def marshal_id(id) do
    id
    |> Atom.to_string()
    |> String.replace("_", "-", global: true)
    |> String.upcase()
    |> then(&Enum.join(["#", &1]))
  end

  @spec marshal(t()) :: String.t()
  def marshal(tag), do: marshal_id(get_tag_id(tag))

  @spec marshal(t(), any()) :: String.t()
  def marshal(tag, value) do
    marshal_id(get_tag_id(tag)) <> ":" <> to_string(value)
  end

  defp get_tag_id(tag) when is_atom(tag), do: tag.id()
  defp get_tag_id(tag) when is_struct(tag), do: tag.id

  @spec capture_attribute_list!(
          String.t(),
          tag_id_t(),
          (String.t(), String.t() -> :skip | {atom(), any})
        ) :: %{required(atom()) => any}
  def capture_attribute_list!(data, tag_id, field_parser_fun) do
    marshaled_tag_id = marshal_id(tag_id)
    regex = Regex.compile!("#{marshaled_tag_id}:(?<target>.*)")
    %{"target" => attribute_list_raw} = Regex.named_captures(regex, data)

    attribute_list_raw
    |> parse_attribute_list()
    |> Enum.reduce(%{}, fn {key, value}, acc ->
      case field_parser_fun.(key, value) do
        :skip -> acc
        {key, value} -> Map.put_new(acc, key, value)
      end
    end)
  end

  @spec parse_attribute_list(String.t()) :: %{required(String.t()) => String.t()}

  @doc """
  Parses an attribute list string as specified in RFC 8216, section 4.2
  Optimized version using binary pattern matching instead of codepoints
  """
  def parse_attribute_list(data) do
    parse_attribute_list_binary(data, :key, "", "", %{})
  end

  # State machine for parsing attribute lists using binary pattern matching
  # States: :key (parsing key), :val (parsing value), :qval (parsing quoted value)

  # Parsing key - equals sign transitions to value
  defp parse_attribute_list_binary(<<"=", rest::binary>>, :key, key, _val, acc) do
    parse_attribute_list_binary(rest, :val, key, "", acc)
  end

  # Parsing key - accumulate characters
  defp parse_attribute_list_binary(<<char::utf8, rest::binary>>, :key, key, val, acc) do
    parse_attribute_list_binary(rest, :key, key <> <<char::utf8>>, val, acc)
  end

  # Parsing value - quote starts quoted value
  defp parse_attribute_list_binary(<<"\"", rest::binary>>, :val, key, "", acc) do
    parse_attribute_list_binary(rest, :qval, key, "", acc)
  end

  # Parsing value - comma saves current pair and starts new key
  defp parse_attribute_list_binary(<<",", rest::binary>>, :val, key, val, acc) do
    parse_attribute_list_binary(rest, :key, "", "", Map.put(acc, key, val))
  end

  # Parsing value - accumulate characters
  defp parse_attribute_list_binary(<<char::utf8, rest::binary>>, :val, key, val, acc) do
    parse_attribute_list_binary(rest, :val, key, val <> <<char::utf8>>, acc)
  end

  # Parsing quoted value - quote ends quoted value
  defp parse_attribute_list_binary(<<"\"", rest::binary>>, :qval, key, val, acc) do
    parse_attribute_list_binary(rest, :val, key, val, acc)
  end

  # Parsing quoted value - accumulate characters (including commas)
  defp parse_attribute_list_binary(<<char::utf8, rest::binary>>, :qval, key, val, acc) do
    parse_attribute_list_binary(rest, :qval, key, val <> <<char::utf8>>, acc)
  end

  # End of input - save last key/value pair if present
  defp parse_attribute_list_binary("", _state, "", "", acc), do: acc
  defp parse_attribute_list_binary("", _state, key, val, acc), do: Map.put(acc, key, val)

  @spec capture_value!(String.t(), tag_id_t(), String.t(), (String.t() -> any)) :: any
  def capture_value!(data, tag_id, match_pattern, parser_fun) do
    marshaled_tag_id = marshal_id(tag_id)
    regex = Regex.compile!("#{marshaled_tag_id}:(?<target>#{match_pattern})")
    %{"target" => raw} = Regex.named_captures(regex, data)
    parser_fun.(raw)
  end

  @spec class_from_id(tag_id_t) :: tag_class_t()
  def class_from_id(:ext_m3u), do: :playlist
  def class_from_id(:ext_x_version), do: :playlist

  def class_from_id(:extinf), do: :media_segment
  def class_from_id(:ext_x_byterange), do: :media_segment
  def class_from_id(:ext_x_discontinuity), do: :media_segment
  def class_from_id(:ext_x_key), do: :media_segment
  def class_from_id(:ext_x_map), do: :media_segment
  def class_from_id(:ext_x_program_date_time), do: :media_segment
  def class_from_id(:ext_x_daterange), do: :media_segment

  def class_from_id(:ext_x_targetduration), do: :media_playlist
  def class_from_id(:ext_x_media_sequence), do: :media_playlist
  def class_from_id(:ext_x_discontinuity_sequence), do: :media_playlist
  def class_from_id(:ext_x_endlist), do: :media_playlist
  def class_from_id(:ext_x_playlist_type), do: :media_playlist
  def class_from_id(:ext_x_i_frames_only), do: :media_playlist

  def class_from_id(:ext_x_media), do: :master_playlist
  def class_from_id(:ext_x_stream_inf), do: :master_playlist
  def class_from_id(:ext_x_i_frame_stream_inf), do: :master_playlist
  def class_from_id(:ext_x_session_data), do: :master_playlist
  def class_from_id(:ext_x_session_key), do: :master_playlist

  def class_from_id(:ext_x_independent_segments), do: :master_or_media_playlist
  def class_from_id(:ext_x_start), do: :master_or_media_playlist
end
