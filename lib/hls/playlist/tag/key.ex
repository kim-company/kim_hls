defmodule HLS.Playlist.Tag.Key do
  @moduledoc """
  EXT-X-KEY tag implementation.

  Specifies how to decrypt Media Segments. It applies to every Media
  Segment and to every Media Initialization Section declared by an
  EXT-X-MAP tag that appears between it and the next EXT-X-KEY tag
  in the Playlist file with the same KEYFORMAT attribute (or the end
  of the Playlist file).

  See RFC 8216, section 4.3.2.4.
  """

  use HLS.Playlist.Tag, id: :ext_x_key

  @impl true
  def unmarshal(line) do
    line
    |> capture_attribute_list!(fn
      "METHOD", val -> {:method, val}
      "URI", val -> {:uri, val}
      "IV", val -> {:iv, val}
      "KEYFORMAT", val -> {:keyformat, val}
      "KEYFORMATVERSIONS", val -> {:keyformatversions, val}
      _key, _val -> :skip
    end)
  end

  @doc """
  Marshals a key attribute map into the tag value string.
  """
  def marshal_attributes(attrs) when is_map(attrs) do
    [
      marshal_attr("METHOD", attrs[:method], false),
      marshal_attr("URI", attrs[:uri], true),
      marshal_attr("IV", attrs[:iv], false),
      marshal_attr("KEYFORMAT", attrs[:keyformat], true),
      marshal_attr("KEYFORMATVERSIONS", attrs[:keyformatversions], true)
    ]
    |> Enum.reject(&is_nil/1)
    |> Enum.join(",")
  end

  def marshal_attributes(attrs) when is_list(attrs) do
    attrs |> Enum.into(%{}) |> marshal_attributes()
  end

  defp marshal_attr(_name, nil, _quote?), do: nil

  defp marshal_attr(name, value, true), do: "#{name}=\"#{value}\""
  defp marshal_attr(name, value, false), do: "#{name}=#{value}"
end
