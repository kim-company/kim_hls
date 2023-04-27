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
          segments_reversed: [Segment.t()]
        }

  defstruct [
    :target_segment_duration,
    :uri,
    finished: false,
    media_sequence_number: 0,
    version: 7,
    tags: %{},
    segments_reversed: []
  ]

  @spec segments(t) :: [Segment.t()]
  def segments(%__MODULE__{segments_reversed: segs}), do: Enum.reverse(segs)

  def update_segment_uri(
        segment = %Segment{absolute_sequence: sequence},
        %__MODULE__{uri: uri},
        extension
      ) do
    %URI{path: path} = uri
    dir = String.trim_trailing(path, ".m3u8")

    filename =
      sequence
      |> to_string()
      |> String.pad_leading(5, "0")

    filename_with_extension = filename <> extension
    uri = %URI{uri | path: Path.join([dir, filename_with_extension])}
    %Segment{segment | uri: uri}
  end

  def generate_missing_segments(
        playlist = %__MODULE__{
          segments_reversed: [],
          media_sequence_number: offset,
          target_segment_duration: duration
        },
        seconds,
        extension
      ) do
    segment =
      %Segment{
        relative_sequence: 0,
        from: 0,
        duration: duration
      }
      |> Segment.update_absolute_sequence(offset)
      |> update_segment_uri(playlist, extension)

    generate_missing_segments(
      %__MODULE__{playlist | segments_reversed: [segment]},
      seconds,
      extension
    )
  end

  def generate_missing_segments(
        playlist = %__MODULE__{
          segments_reversed: segments = [segment = %Segment{from: from, duration: duration} | _]
        },
        seconds,
        extension
      ) do
    to = duration + from

    if from <= seconds and seconds < to do
      # We're within the boundaries!
      playlist
    else
      if seconds >= to do
        segment =
          segment
          |> Segment.generate_next_segment()
          |> update_segment_uri(playlist, extension)

        generate_missing_segments(
          %__MODULE__{playlist | segments_reversed: [segment | segments]},
          seconds,
          extension
        )
      else
        raise "Target seconds lay in the past!"
      end
    end
  end

  defimpl HLS.Playlist.Marshaler, for: HLS.Playlist.Media do
    alias HLS.Playlist.Tag
    alias HLS.Segment

    @impl true
    def marshal(
          playlist = %HLS.Playlist.Media{
            version: 7,
            segments_reversed: segments,
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
        |> Enum.reverse()
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

      %HLS.Playlist.Media{
        playlist
        | tags: tags,
          version: version.value,
          target_segment_duration: segment_duration.value,
          media_sequence_number: sequence_number.value,
          finished: finished,
          segments_reversed: segments
      }
    end
  end
end
