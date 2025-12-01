defmodule HLS.Playlist do
  @moduledoc """
  HLS Playlist parses based on RFC 8216.
  """

  alias HLS.Playlist.{Unmarshaler, Marshaler}

  @doc """
  Given a valid playlist file and a playlist module implementation, returns the
  deserialized list of tags.
  """
  def unmarshal(data = "#EXTM3U" <> _rest, playlist) do
    unmarshalers = Unmarshaler.supported_tags(playlist)

    # Optimize: separate URI handler from tag handlers for faster lookup
    # SegmentURI is usually the most common "tag" in media playlists
    {uri_handler, tag_handlers} =
      Enum.split_with(unmarshalers, fn u -> u.id() == :uri end)

    uri_handler = List.first(uri_handler)

    init_tag = fn module, data, sequence ->
      value_or_attrs = module.unmarshal(data)
      tag = module.init(value_or_attrs, sequence)
      n = if module.has_uri?(), do: sequence + 1, else: sequence
      {tag, n}
    end

    tags =
      data
      |> split_lines()
      |> Enum.reduce({[], 0, nil}, fn
        line, acc = {marshaled, n, nil} ->
          # Fast path for segment URIs (most common in media playlists)
          matching_unmarshaler =
            if uri_handler && !String.starts_with?(line, "#") do
              uri_handler
            else
              Enum.find(tag_handlers, fn unmarshaler ->
                unmarshaler.match?(line)
              end)
            end

          if matching_unmarshaler == nil do
            # Ignore each line that we do not recognize.
            acc
          else
            if matching_unmarshaler.is_multiline?() do
              {marshaled, n, {matching_unmarshaler, line}}
            else
              {tag, n} = init_tag.(matching_unmarshaler, line, n)
              {[tag | marshaled], n, nil}
            end
          end

        line, {marshaled, n, {matching_unmarshaler, old_line}} ->
          {tag, n} = init_tag.(matching_unmarshaler, [old_line, line], n)
          {[tag | marshaled], n, nil}
      end)
      |> elem(0)
      |> Enum.reverse()
      |> Enum.group_by(fn tag ->
        if tag.class == :media_segment do
          {tag.sequence, :segment}
        else
          tag.id
        end
      end)

    Unmarshaler.load_tags(playlist, tags)
  end

  def unmarshal(data, _playlist) do
    raise ArgumentError, "Input data is not a valid M3U file: #{inspect(data)}"
  end

  # Optimized line splitting using list of separators instead of regex
  defp split_lines(data) do
    data
    |> String.split(["\r\n", "\r", "\n"], trim: true)
  end

  def marshal(playlist) do
    Marshaler.marshal(playlist)
  end

  @doc """
  Given a master or media playlist and its child resource, like segment, builds
  the absolute URI that can be used to fetch it. The .m3u8 allows such resources
  to be absolute URIs on their own. This function respects it leaving the child
  resource as is.
  """
  @spec build_absolute_uri(master_or_media :: URI.t(), rendition_or_segment :: URI.t()) :: URI.t()
  def build_absolute_uri(master_uri, media_uri), do: HLS.Helper.merge_uri(master_uri, media_uri)

  @doc """
  Extracts the relative URI of a child resource, if possible.
  """
  @spec extract_relative_uri(master_or_media :: URI.t(), rendition_or_segment :: URI.t()) ::
          URI.t()
  def extract_relative_uri(master_uri, media_uri) do
    master_base_path = Path.dirname(master_uri.path)
    relative_base_path = Path.dirname(media_uri.path)

    if master_base_path == relative_base_path and master_uri.host == media_uri.host do
      relative = String.trim_leading(media_uri.path, master_base_path <> "/")
      %URI{path: relative}
    else
      media_uri
    end
  end
end
