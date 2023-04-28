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
  defstruct [:playlist, :segment_extension, timed_segments: [], to_upload: []]

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

  def fit(
        builder = %__MODULE__{
          timed_segments: segments,
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

    new_segments =
      rest
      |> Enum.map(fn %{segment: x} -> x end)
      |> Enum.reverse()

    playlist = %Media{playlist | segments: playlist_segments ++ new_segments}

    %__MODULE__{
      builder
      | timed_segments: [last_timed_segment],
        to_upload: rest,
        playlist: playlist
    }
  end

  def flush(
        builder = %__MODULE__{
          timed_segments: timed_segments,
          playlist: playlist = %Media{segments: playlist_segments}
        }
      ) do
    new_segments =
      timed_segments
      |> Enum.map(fn %{segment: x} -> x end)
      |> Enum.reverse()

    playlist = %Media{playlist | segments: playlist_segments ++ new_segments}

    %__MODULE__{
      builder
      | timed_segments: [],
        to_upload: new_segments,
        playlist: playlist
    }
  end

  def playlist(%__MODULE__{playlist: playlist}), do: playlist

  def take_uploadables(builder = %__MODULE__{to_upload: to_upload, playlist: playlist}) do
    uploadables =
      to_upload
      |> Enum.map(fn %{segment: segment, acc: acc} ->
        %{payload: Enum.reverse(acc), uri: Media.build_segment_uri(playlist, segment)}
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
