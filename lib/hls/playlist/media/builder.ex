defmodule HLS.Playlist.Media.Builder do
  alias HLS.Playlist.Media
  alias HLS.Segment

  @type timed_payload :: %{
          from: float(),
          to: float(),
          payload: binary()
        }

  @type uploadable :: %{
          from: float(),
          to: float(),
          segment: Segment.t(),
          payloads: [timed_payload()]
        }

  defstruct [
    :playlist,
    :segment_extension,
    replace_empty_segments_uri: false,
    timed_payloads: [],
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
  def ack(builder, ref) do
    {segment, in_flight} = Map.pop!(builder.in_flight, ref)

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
  Fits a timed payload inside the builder. It does not accept payload that fall
  within the boundaries of the consolidated segments, which are the ones that
  have been already included in the playlist.
  """
  def fit(
        builder = %__MODULE__{playlist: %Media{segments: segments}, timed_payloads: buf},
        timed_payload = %{from: from}
      ) do
    playback =
      segments
      |> Enum.map(fn %Segment{duration: duration} -> duration end)
      |> Enum.sum()

    if from < playback do
      raise "timed payload #{inspect(timed_payload)} is late (< #{playback})"
    end

    %__MODULE__{builder | timed_payloads: [timed_payload | buf]}
  end

  @doc """
  Pops segments that are ready to be consolidated, i.e., stored insiede the
  segment/playlist storage and added to the playlist's list of segments. A
  segment is considered complete if contains timed payloads that finish or
  excceed its `to` value. If the force option is set to true, all segments are
  returned even though they are not complete. Useful at EOS.
  """
  def pop(builder, opts \\ [])

  def pop(builder = %__MODULE__{timed_payloads: []}, _opts) do
    {[], builder}
  end

  def pop(builder, opts) do
    # Create the list of segments which can contain the timed_payloads. Return
    # the ones that are complete or all of them, if forced. Create a reference
    # of each segment and store it. When the segments are (na)acknoledged, add
    # them to the playlist.

    force? = Keyword.get(opts, :force, false)
    segment_duration = builder.playlist.target_segment_duration

    playlist_playback =
      builder.playlist.segments
      |> Enum.map(fn %Segment{duration: duration} -> duration end)
      |> Enum.sum()

    segments_count = Enum.count(builder.playlist.segments)

    %{to: payloads_playback} =
      builder.timed_payloads
      |> Enum.sort(fn %{from: left}, %{from: right} -> left < right end)
      |> List.last()

    # How many segments are needed to fit the payloads? As soon as we have some
    # timed payloads here, at least 1.
    n = ceil((payloads_playback - playlist_playback) / builder.playlist.target_segment_duration)

    uploadables =
      Range.new(0, n - 1)
      |> Enum.reduce([], fn seq, acc ->
        from = playlist_playback + segment_duration * seq
        to = from + segment_duration
        [%{from: from, to: to, payloads: [], segment: nil} | acc]
      end)
      |> Enum.reverse()
      |> Enum.with_index(segments_count)
      |> Enum.map(fn {uploadable, index} ->
        segment = %Segment{
          duration: segment_duration,
          relative_sequence: index,
          absolute_sequence: index,
          uri: segment_uri(builder, index)
        }

        %{uploadable | segment: segment}
      end)
      |> fit_payloads_into_segments(builder.timed_payloads, [])
      |> Enum.map(&Map.put(&1, :ref, make_ref()))

    uploadables =
      if not force? and not Enum.empty?(uploadables) do
        # Return only those segments that are complete. As soon as we create just
        # the exact amount of segments required to fit the timed payloads, only the
        # last segment needs a check.
        last = List.last(uploadables)

        if is_full(last) do
          uploadables
        else
          Enum.drop(uploadables, -1)
        end
      else
        uploadables
      end

    # count the number of timed_payloads that have been fit inside
    # the segments: those ones are gone.
    processed_payloads_count =
      uploadables
      |> Enum.map(fn %{payloads: acc} -> acc end)
      |> List.flatten()
      |> Enum.count()

    timed_payloads = Enum.drop(builder.timed_payloads, processed_payloads_count)

    uploadables =
      if builder.replace_empty_segments_uri do
        uploadables
        |> Enum.map(fn uploadable ->
          if is_empty_uploadable(uploadable) do
            %{
              uploadable
              | segment: %Segment{uploadable.segment | uri: empty_segment_uri(builder)}
            }
          else
            uploadable
          end
        end)
      else
        uploadables
      end

    in_flight =
      Enum.reduce(uploadables, builder.in_flight, fn %{ref: ref, segment: segment}, acc ->
        Map.put(acc, ref, segment)
      end)

    {uploadables, %__MODULE__{builder | timed_payloads: timed_payloads, in_flight: in_flight}}
  end

  defp is_empty_uploadable(%{payloads: payloads}) do
    data =
      payloads
      |> Enum.map(&Map.get(&1, :payload, <<>>))
      |> Enum.join()

    data == <<>>
  end

  @doc """
  Combines fit and pop action in one call.
  """
  def fit_and_pop(builder, payload, opts \\ []) do
    builder
    |> fit(payload)
    |> pop(opts)
  end

  defp is_full(%{payloads: []}), do: false

  defp is_full(%{to: to, payloads: payloads}) do
    %{to: payloads_to} = List.last(payloads)
    payloads_to >= to
  end

  defp fit_payloads_into_segments([], [], acc), do: Enum.reverse(acc)

  defp fit_payloads_into_segments([segment_head | segments], [], acc) do
    # Once we finish the payloads, fast forward.
    fit_payloads_into_segments(segments, [], [segment_head | acc])
  end

  defp fit_payloads_into_segments([segment_head | segments], [payload_head | payloads], acc) do
    if payload_head.from < segment_head.to do
      # This payload fits in the segment. It does not mean that the segment is complete.
      segment_head = update_in(segment_head, [:payloads], fn acc -> acc ++ [payload_head] end)
      fit_payloads_into_segments([segment_head | segments], payloads, acc)
    else
      # the current head is complete.
      fit_payloads_into_segments(segments, [payload_head | payloads], [segment_head | acc])
    end
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
