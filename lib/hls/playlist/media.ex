defmodule HLS.Playlist.Media do
  alias HLS.Playlist.Tag
  alias HLS.Segment

  @type t :: %__MODULE__{
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
          type: Tag.PlaylistType.type() | nil,
          segments: [Segment.t()]
        }

  defstruct [
    :target_segment_duration,
    :uri,
    finished: false,
    type: nil,
    media_sequence_number: 0,
    version: 7,
    segments: []
  ]

  @spec new(URI.t(), pos_integer()) :: t()
  def new(uri, target_segment_duration) do
    %__MODULE__{uri: uri, target_segment_duration: target_segment_duration}
  end

  @doc """
  Builds segment's URI relative to its media playlist's uri.
  """
  @spec build_segment_uri(URI.t(), URI.t()) :: URI.t()
  def build_segment_uri(media_uri, segment_uri), do: HLS.Helper.merge_uri(media_uri, segment_uri)

  @spec segments(t) :: [Segment.t()]
  def segments(%__MODULE__{segments: segs, uri: uri}) do
    segs
    |> Enum.map(fn segment = %Segment{uri: segment_uri} ->
      %Segment{segment | uri: build_segment_uri(uri, segment_uri)}
    end)
  end

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
        media_sequence_number: media_sequence_number,
        type: type
      }) do
    [
      {Tag.Version, version},
      if(type == nil,
        do: nil,
        else: {Tag.PlaylistType, Tag.PlaylistType.marshal_playlist_type(type)}
      ),
      {Tag.TargetSegmentDuration, trunc(target_segment_duration)},
      {Tag.MediaSequenceNumber, media_sequence_number}
    ]
    |> Enum.reject(fn x -> x == nil end)
  end
end

defimpl HLS.Playlist.Unmarshaler, for: HLS.Playlist.Media do
  alias HLS.Playlist.Tag
  alias HLS.Segment

  @impl true
  def supported_tags(_) do
    [
      Tag.Version,
      Tag.PlaylistType,
      Tag.TargetSegmentDuration,
      Tag.MediaSequenceNumber,
      Tag.EndList,
      Tag.Inf,
      Tag.SegmentURI,
      Tag.Discontinuity
    ]
  end

  @impl true
  def load_tags(playlist, tags) do
    [version] = Map.get(tags, Tag.Version.id(), [%{value: 1}])
    [segment_duration] = Map.fetch!(tags, Tag.TargetSegmentDuration.id())
    [sequence_number] = Map.fetch!(tags, Tag.MediaSequenceNumber.id())

    finished? = Map.has_key?(tags, Tag.EndList.id())

    type =
      case Map.get(tags, Tag.PlaylistType.id(), nil) do
        # defaults to PlaylisType EVENT if the tag is not present
        nil -> nil
        [playlist_type] -> playlist_type.value
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
      | version: version.value,
        target_segment_duration: segment_duration.value,
        media_sequence_number: sequence_number.value,
        finished: finished?,
        type: type,
        segments: segments
    }
  end
end
