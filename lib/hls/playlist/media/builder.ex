defmodule HLS.Playlist.Media.Builder do
  alias HLS.Playlist.Media
  alias HLS.Segment

  defstruct [
    :playlist,
    :segment_extension,
    replace_empty_segments_uri: false,
    in_flight: %{},
    acknoledged: []
  ]

  @doc """
  Creates a new Media playlist builder validating the input playlist. Accepts
  playlists that both not yet finished and contain all segments required to
  compute their playback duration, i.e., their media_sequence_number is zero.
  """
  def new(playlist, opts \\ [])

  def new(%Media{finished: true}, _opts) do
    raise "not with a finished playlist"
  end

  def new(%Media{media_sequence_number: seq}, _opts) when seq != 0 do
    raise "not with a media_sequence_number which is not zero, it is not possible to compute the playlist's actual playback "
  end

  # https://www.rfc-editor.org/rfc/rfc8216#section-6.2.2 specifies that
  # live playlists should not contain any playlist-type tag as that does
  # not allow it to remove segments.
  def new(%Media{type: type}, _opts) when type in [:vod, :event] do
    raise "not with a playlist of type :vod|:event"
  end

  def new(playlist, opts) do
    %__MODULE__{
      playlist: playlist,
      segment_extension: Keyword.get(opts, :segment_extension, ".vtt"),
      replace_empty_segments_uri: Keyword.get(opts, :replace_empty_segments_uri, false)
    }
  end

  def segment_uri(builder, sequence) do
    name = build_segment_name(builder, sequence)
    build_segment_uri(builder, name)
  end

  def empty_segment_uri(builder) do
    build_segment_uri(builder, "empty" <> builder.segment_extension)
  end

  @doc """
  Returns the playlist stored in the builder.
  """
  def playlist(%__MODULE__{playlist: playlist}), do: playlist

  @doc """
  Acknoledges that the segment uploadable referenced by ref has been successfully stored
  at the location pointed by its URI, meaning that it is now accessible for final users.
  After this action, the segments is included in the playlist if it is the next in the queue.
  """
  def ack(builder, ref, is_empty? \\ false) do
    {segment, in_flight} = Map.pop!(builder.in_flight, ref)

    segment =
      if is_empty? do
        %Segment{segment | uri: empty_segment_uri(builder)}
      else
        segment
      end

    builder = %__MODULE__{
      builder
      | in_flight: in_flight,
        acknoledged: [segment | builder.acknoledged]
    }

    consolidate_playlist(builder)
  end

  @doc """
  Negatively acknoledges the processing of the uploadable referenced by ref. Its relative
  segment is going to be added to the playlist as an empty one.
  """
  def nack(builder, ref) do
    {segment, in_flight} = Map.pop!(builder.in_flight, ref)
    segment = %Segment{segment | uri: empty_segment_uri(builder)}

    builder = %__MODULE__{
      builder
      | in_flight: in_flight,
        acknoledged: [segment | builder.acknoledged]
    }

    consolidate_playlist(builder)
  end

  @doc """
  Fills the playlist with the segments required to get in sync with playback. Returns
  the segments that should be uploaded before the next segment is filled.
  """
  def sync(builder, playback) do
    playlist_playback =
      builder.playlist.segments
      |> Enum.map(fn %Segment{duration: duration} -> duration end)
      |> Enum.sum()

    if playlist_playback > playback do
      raise "cannot fast forward within consolidated segments (#{playlist_playback} > #{playback})"
    end

    segments_count = Enum.count(builder.playlist.segments)
    segment_duration = builder.playlist.target_segment_duration

    # How many segments are needed to fit the payloads?
    n = ceil((playback - playlist_playback) / builder.playlist.target_segment_duration)

    segments =
      if n > 0 do
        # Is there a better way to repeat this operation n times? Recursion?
        Range.new(0, n - 1)
        |> Enum.with_index(segments_count)
        |> Enum.map(fn {seq, index} ->
          segment = %Segment{
            from: playlist_playback + segment_duration * seq,
            duration: segment_duration,
            relative_sequence: index,
            absolute_sequence: index,
            uri: segment_uri(builder, index),
            ref: make_ref()
          }

          if builder.replace_empty_segments_uri do
            %Segment{segment | uri: empty_segment_uri(builder)}
          else
            segment
          end
        end)
      else
        []
      end

    in_flight =
      Enum.reduce(segments, builder.in_flight, fn segment, acc ->
        Map.put(acc, segment.ref, segment)
      end)

    {segments, %__MODULE__{builder | in_flight: in_flight}}
  end

  @doc """
  Returns the next segment that should be filled with content. This call updates
  the segments `in flight`, meaning that the next time it is called it will
  return the segment after. Once the segment is filled and uploaded, it has to
  be acknoledged to end up in the segment playlist list.
  """
  def next_segment(builder) do
    # Find the last sequence number.
    last_segment =
      if map_size(builder.in_flight) > 0 do
        builder.in_flight
        |> Enum.map(fn {_ref, segment} -> segment end)
        |> Enum.sort(fn %Segment{relative_sequence: left}, %Segment{relative_sequence: right} ->
          left < right
        end)
        |> List.last()
      else
        List.last(builder.playlist.segments)
      end

    index = if is_nil(last_segment), do: 0, else: last_segment.relative_sequence + 1
    segment_duration = builder.playlist.target_segment_duration
    from = if is_nil(last_segment), do: 0, else: last_segment.from + last_segment.duration

    segment = %Segment{
      from: from,
      duration: segment_duration,
      relative_sequence: index,
      absolute_sequence: index,
      uri: segment_uri(builder, index),
      ref: make_ref()
    }

    in_flight = Map.put(builder.in_flight, segment.ref, segment)

    {segment, %__MODULE__{builder | in_flight: in_flight}}
  end

  # Updates playlist segments.
  defp consolidate_playlist(builder) do
    last_relative_sequence =
      builder.playlist.segments
      |> Enum.map(fn %Segment{relative_sequence: x} -> x end)
      |> List.last()

    next_sequence = if last_relative_sequence != nil, do: last_relative_sequence + 1, else: 0

    ready =
      builder.acknoledged
      |> Enum.sort(fn %Segment{relative_sequence: left}, %Segment{relative_sequence: right} ->
        left < right
      end)
      # Pair each segment with their expected relative sequence number
      |> Enum.with_index(next_sequence)
      # While the numbers match take them. If there are holes in the sequence
      # in means some uploadable has not been acknoledged yet.
      |> Enum.take_while(fn {%Segment{relative_sequence: x}, y} -> x == y end)
      |> Enum.map(fn {x, _} -> x end)

    playlist = %Media{builder.playlist | segments: builder.playlist.segments ++ ready}
    acknoledged = Enum.drop(builder.acknoledged, length(ready))
    %__MODULE__{builder | playlist: playlist, acknoledged: acknoledged}
  end

  defp build_segment_uri(builder, name) do
    path = builder.playlist.uri.path

    root =
      path
      |> Path.basename(path)
      |> String.trim_trailing(Path.extname(path))

    [root, name]
    |> Path.join()
    |> URI.new!()
  end

  defp build_segment_name(%__MODULE__{segment_extension: extension}, sequence) do
    sequence
    |> to_string()
    |> String.pad_leading(5, "0")
    |> List.wrap()
    |> Enum.concat([extension])
    |> Enum.join()
  end
end
