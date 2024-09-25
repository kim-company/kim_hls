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
  Builds segment absolute uri.
  """
  @spec build_segment_uri(media :: URI.t(), segment :: URI.t()) :: URI.t()
  def build_segment_uri(media_uri, segment_uri), do: HLS.Helper.merge_uri(media_uri, segment_uri)

  @doc """
  Returns the segments for this playlist.
  """
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
      |> Stream.transform(nil, fn segment, last ->
        if segment.init_section == last do
          {[%{segment | init_section: nil}], last}
        else
          {[segment], segment.init_section}
        end
      end)
      |> Stream.map(fn %Segment{duration: duration, uri: uri} = segment ->
        [
          segment.discontinuity && Tag.marshal(Tag.Discontinuity),
          segment.init_section &&
            Tag.marshal(Tag.Map, Tag.Map.marshal_uri_and_byterange(segment.init_section)),
          Tag.marshal(Tag.Inf, duration) <> ",",
          segment.byterange &&
            Tag.marshal(Tag.Byterange, Tag.Byterange.marshal(segment.byterange)),
          to_string(uri)
        ]
        |> Enum.reject(&is_nil/1)
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
      Tag.Discontinuity,
      Tag.Byterange,
      Tag.Map
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
      |> Stream.map(fn {_, val} -> Segment.from_tags(val) end)
      |> Stream.map(&Segment.update_absolute_sequence(&1, sequence_number.value))
      # For each segment, the EXT-X-MAP is the last map we've seen so far in the playlist.
      # See https://datatracker.ietf.org/doc/html/rfc8216#section-4.3.2.5
      |> Stream.transform(nil, fn
        %Segment{init_section: init_section} = segment, _last when init_section != nil ->
          {[segment], init_section}

        %Segment{} = segment, last ->
          {[%Segment{segment | init_section: last}], last}
      end)
      |> Stream.transform(0, fn segment, acc ->
        {[%Segment{segment | from: acc}], acc + segment.duration}
      end)
      |> Enum.to_list()

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
