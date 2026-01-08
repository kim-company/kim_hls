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
          # allows synchronization of Renditions with discontinuity tags
          # https://www.rfc-editor.org/rfc/rfc8216#section-4.3.3.7
          discontinuity_sequence: non_neg_integer(),
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
    discontinuity_sequence: 0,
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
          segment.program_date_time &&
            Tag.marshal(
              Tag.ProgramDateTime,
              Tag.ProgramDateTime.marshal_datetime(segment.program_date_time)
            ),
          Tag.marshal(Tag.Inf, :erlang.float_to_binary(duration * 1.0, decimals: 5)) <> ",",
          segment.byterange &&
            Tag.marshal(Tag.Byterange, Tag.Byterange.marshal(segment.byterange)),
          to_string(uri)
        ]
        |> Enum.filter(& &1)
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
        discontinuity_sequence: discontinuity_sequence,
        type: type,
        segments: segments
      }) do
    has_discontinuities = Enum.any?(segments, fn segment -> segment.discontinuity end)

    [
      {Tag.Version, version},
      if(type == nil,
        do: nil,
        else: {Tag.PlaylistType, Tag.PlaylistType.marshal_playlist_type(type)}
      ),
      {Tag.TargetSegmentDuration, trunc(target_segment_duration)},
      {Tag.MediaSequenceNumber, media_sequence_number},
      if(discontinuity_sequence > 0 or has_discontinuities,
        do: {Tag.DiscontinuitySequence, discontinuity_sequence},
        else: nil
      )
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
      Tag.DiscontinuitySequence,
      Tag.EndList,
      Tag.Inf,
      Tag.SegmentURI,
      Tag.Discontinuity,
      Tag.Byterange,
      Tag.Map,
      Tag.ProgramDateTime
    ]
  end

  @impl true
  def load_tags(playlist, tags) do
    [version] = Map.get(tags, Tag.Version.id(), [%{value: 1}])
    [segment_duration] = Map.fetch!(tags, Tag.TargetSegmentDuration.id())
    [sequence_number] = Map.fetch!(tags, Tag.MediaSequenceNumber.id())

    discontinuity_sequence =
      case Map.get(tags, Tag.DiscontinuitySequence.id(), nil) do
        nil -> 0
        [discontinuity_seq] -> discontinuity_seq.value
      end

    finished? = Map.has_key?(tags, Tag.EndList.id())

    type =
      case Map.get(tags, Tag.PlaylistType.id(), nil) do
        # defaults to PlaylisType EVENT if the tag is not present
        nil -> nil
        [playlist_type] -> playlist_type.value
      end

    # Optimized: combine filter and mapping in a single pass, then process segments
    segment_list =
      tags
      |> Enum.reduce([], fn
        {{seq, :segment}, val}, acc -> [{seq, val} | acc]
        _other, acc -> acc
      end)
      |> Enum.sort()

    # Process segments with init section tracking and from time calculation
    segments =
      segment_list
      |> Enum.reduce({[], nil, 0}, fn {_, val}, {acc, last_init, from_acc} ->
        segment =
          val
          |> Segment.from_tags()
          |> Segment.update_absolute_sequence(sequence_number.value)

        # Handle init section tracking
        {init_section, new_last_init} =
          if segment.init_section != nil do
            {segment.init_section, segment.init_section}
          else
            {last_init, last_init}
          end

        # Set from time and init section
        segment = %Segment{segment | init_section: init_section, from: from_acc}

        {[segment | acc], new_last_init, from_acc + segment.duration}
      end)
      |> elem(0)
      |> Enum.reverse()

    %HLS.Playlist.Media{
      playlist
      | version: version.value,
        target_segment_duration: segment_duration.value,
        media_sequence_number: sequence_number.value,
        discontinuity_sequence: discontinuity_sequence,
        finished: finished?,
        type: type,
        segments: segments
    }
  end
end
