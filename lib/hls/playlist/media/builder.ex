defmodule HLS.Playlist.Media.Builder do
  alias HLS.Playlist.Media
  alias HLS.Segment

  @type timed_payload :: %{from: float(), to: float(), payload: binary()}
  @type timed_segment :: %{
          from: float(),
          to: float(),
          segment: Segment.t(),
          acc: [timed_payload()]
        }
  defstruct [:playlist, :segment_extension, timed_segments: [], to_upload: [], closed: false]

  def new(
        playlist = %Media{
          segments: [],
          target_segment_duration: target_segment_duration,
          media_sequence_number: 0
        },
        segment_extension
      ) do
    %__MODULE__{
      playlist: playlist,
      segment_extension: segment_extension,
      timed_segments: [
        %{
          from: 0,
          to: target_segment_duration,
          segment: Segment.new(target_segment_duration, 0, 0, segment_extension),
          acc: []
        }
      ]
    }
  end

  defp current_segment_from([%{from: from} | _]), do: from
  defp current_segment_to([%{to: to} | _]), do: to

  defp real_segment_to(%{acc: [%{to: to} | _]}), do: to

  def fit(%__MODULE__{closed: true}, _) do
    raise "Cannot fit timed payload into a finished playlist"
  end

  def fit(
        builder = %__MODULE__{
          timed_segments: segments,
          to_upload: to_upload,
          playlist:
            playlist = %Media{
              target_segment_duration: segment_duration,
              segments: playlist_segments
            }
        },
        timed_payload = %{from: from}
      ) do
    segment_from = current_segment_from(segments)
    segment_to = current_segment_to(segments)

    # After this check we're ensured that the last timed segments can hold the current buffer.
    # Previous segments can be considered complete!
    [last_timed_segment | rest] =
      cond do
        from < segment_from ->
          raise "A timed payload that starts before the active segment was received"

        from >= segment_to ->
          extend_timed_segments_till(segments, from, segment_duration)

        true ->
          # Segments can already hold this buffer.
          segments
      end

    last_timed_segment =
      update_in(last_timed_segment, [:acc], fn acc -> [timed_payload | acc] end)

    # Here we're checking wether the last buffer we put in the last segment is
    # going to be the last one for it. It happens when the buffer finishes after
    # the segment duration (or at its boundary)
    last_to = real_segment_to(last_timed_segment)
    segment_to = current_segment_to([last_timed_segment])

    {complete_segments, timed_segments} =
      if real_segment_to(last_timed_segment) >= segment_to do
        all = [last_timed_segment | rest]
        [h | _] = extend_timed_segments_till(all, last_to, segment_duration)
        {all, [h]}
      else
        {rest, [last_timed_segment]}
      end

    new_segments =
      complete_segments
      |> Enum.map(fn %{segment: x} -> x end)
      |> Enum.reverse()

    playlist = %Media{playlist | segments: playlist_segments ++ new_segments}

    %__MODULE__{
      builder
      | timed_segments: timed_segments,
        to_upload: to_upload ++ complete_segments,
        playlist: playlist
    }
  end

  def flush(
        builder = %__MODULE__{
          timed_segments: timed_segments,
          to_upload: to_upload,
          playlist: playlist = %Media{segments: playlist_segments}
        }
      ) do
    new_segments =
      timed_segments
      |> Enum.map(fn %{segment: x} -> x end)
      |> Enum.reverse()

    playlist = %Media{playlist | segments: playlist_segments ++ new_segments, finished: true}

    %__MODULE__{
      builder
      | timed_segments: [],
        to_upload: to_upload ++ Enum.reverse(timed_segments),
        playlist: playlist,
        closed: true
    }
  end

  def playlist(%__MODULE__{playlist: playlist}), do: playlist

  def take_uploadables(builder = %__MODULE__{to_upload: to_upload, playlist: playlist}) do
    uploadables =
      to_upload
      |> Enum.map(fn %{segment: segment, acc: acc} ->
        %{payload: Enum.reverse(acc), uri: Media.build_segment_uri(playlist.uri, segment.uri)}
      end)
      |> Enum.reverse()

    {uploadables, %__MODULE__{builder | to_upload: []}}
  end

  defp extend_timed_segments_till(
         segments = [%{to: segment_to, segment: segment} | _],
         from,
         segment_duration
       )
       when from >= segment_to do
    next_segment = Segment.generate_next_segment(segment, segment_duration)

    segments = [
      %{from: segment_to, to: segment_to + segment_duration, segment: next_segment, acc: []}
      | segments
    ]

    extend_timed_segments_till(segments, from, segment_duration)
  end

  defp extend_timed_segments_till(segments, _from, _segment_duration) do
    segments
  end
end
