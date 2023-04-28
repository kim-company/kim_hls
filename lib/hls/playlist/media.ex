defmodule HLS.Playlist.Media do
  alias HLS.Playlist.Tag
  alias HLS.Segment

  @type t :: %__MODULE__{
          tags: Playlist.Unmarshaler.tag_map_t(),
          version: pos_integer(),
          # specifies the maximum length of segments.
          # https://www.rfc-editor.org/rfc/rfc8216#section-4.3.3.1
          target_segment_duration: pos_integer(),
          # the relative sequence number this playlist starts from. Used when
          # not all segments are listed in the playlist.
          # https://www.rfc-editor.org/rfc/# rfc8216#section-4.3.3.2
          media_sequence_number: pos_integer(),
          uri: URI.t() | nil,
          # Indicates that the playlist has become VOD.
          finished: boolean(),
          segments: [Segment.t()]
        }

  defstruct [
    :target_segment_duration,
    :uri,
    finished: false,
    media_sequence_number: 0,
    version: 7,
    tags: %{},
    segments: []
  ]

  @spec new(URI.t(), pos_integer()) :: t()
  def new(uri, target_segment_duration) do
    %__MODULE__{uri: uri, target_segment_duration: target_segment_duration}
  end

  @doc """
  Segment's path may be relative to their hosting media playlist URI. This
  function produces the URI that points to the resource.
  """
  @spec build_segment_uri(t(), Segment.t()) :: URI.t()
  def build_segment_uri(%__MODULE__{uri: base = %URI{path: path}}, %Segment{uri: rel}) do
    base = %URI{base | path: String.trim_trailing(path, Path.extname(path)) <> "/"}
    URI.merge(base, rel)
  end

  @spec segments(t) :: [Segment.t()]
  def segments(%__MODULE__{segments: segs}), do: segs

  @spec compute_playlist_duration(t()) :: float()
  def compute_playlist_duration(%__MODULE__{segments: segments}) do
    Enum.reduce(segments, 0.0, fn %Segment{duration: d}, acc -> acc + d end)
  end
end

defimpl HLS.Playlist.Marshaler, for: HLS.Playlist.Media do
  alias HLS.Playlist.Tag
  alias HLS.Segment

  @impl true
  def marshal(
        playlist = %HLS.Playlist.Media{
          version: 7,
          segments: segments,
          finished: finished
        }
      ) do
    header_tags =
      playlist
      |> header_tags_with_values()
      |> Enum.filter(fn {_, value} -> value != nil end)
      |> Enum.map(fn {tag, value} -> Tag.marshal(tag, value) end)
      |> Enum.join("\n")

    segments =
      segments
      |> Enum.map(fn %Segment{duration: duration, uri: uri} ->
        [Tag.marshal(Tag.Inf, duration) <> ",", to_string(uri)]
        |> Enum.join("\n")
      end)
      |> Enum.join("\n")

    rows = ["#EXTM3U", header_tags, segments]

    rows =
      if finished do
        rows ++ [Tag.marshal_id(Tag.EndList.id())]
      else
        rows
      end

    payload = rows |> Enum.join("\n")
    payload <> "\n"
  end

  def header_tags_with_values(%HLS.Playlist.Media{
        version: version,
        target_segment_duration: target_segment_duration,
        media_sequence_number: media_sequence_number
      }) do
    [
      {Tag.Version, version},
      {Tag.TargetSegmentDuration, trunc(target_segment_duration)},
      {Tag.MediaSequenceNumber, media_sequence_number}
    ]
  end
end

defimpl HLS.Playlist.Unmarshaler, for: HLS.Playlist.Media do
  alias HLS.Playlist.Tag
  alias HLS.Segment

  @impl true
  def supported_tags(_) do
    [
      Tag.Version,
      Tag.TargetSegmentDuration,
      Tag.MediaSequenceNumber,
      Tag.EndList,
      Tag.Inf,
      Tag.SegmentURI
    ]
  end

  @impl true
  def load_tags(playlist, tags) do
    [version] = Map.fetch!(tags, Tag.Version.id())
    [segment_duration] = Map.fetch!(tags, Tag.TargetSegmentDuration.id())
    [sequence_number] = Map.fetch!(tags, Tag.MediaSequenceNumber.id())

    finished =
      case Map.get(tags, Tag.EndList.id()) do
        nil -> false
        [endlist] -> endlist.value
      end

    segments =
      tags
      |> Enum.filter(fn
        {{_, :segment}, _val} -> true
        _other -> false
      end)
      |> Enum.map(fn {{seq, _}, val} -> {seq, val} end)
      |> Enum.into([])
      |> Enum.sort()
      |> Enum.map(fn {_, val} -> Segment.from_tags(val) end)
      |> Enum.map(&Segment.update_absolute_sequence(&1, sequence_number.value))
      |> Enum.reduce([], fn
        segment, [] ->
          [%Segment{segment | from: 0}]

        next, acc = [%Segment{from: from, duration: duration} | _] ->
          [%Segment{next | from: from + duration} | acc]
      end)
      |> Enum.reverse()

    %HLS.Playlist.Media{
      playlist
      | tags: tags,
        version: version.value,
        target_segment_duration: segment_duration.value,
        media_sequence_number: sequence_number.value,
        finished: finished,
        segments: segments
    }
  end
end
