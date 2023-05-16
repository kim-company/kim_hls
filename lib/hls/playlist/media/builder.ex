defmodule HLS.Playlist.Media.Builder do
  alias HLS.Playlist.Media
  alias HLS.Segment

  @type timed_payload :: %{
          from: float(),
          to: float(),
          payload: binary()
        }

  @type timed_segment :: %{
          from: float(),
          to: float(),
          segment: Segment.t(),
          acc: [timed_payload()]
        }

  defstruct [
    :playlist,
    segment_relative_uri_fun: &__MODULE__.build_segment_relative_uri/1,
    timed_payloads: []
    # timed_segments: [],
    # to_upload: [],
    # closed: false,
    # filter_uploadables_from: nil
  ]

  def build_segment_relative_uri(sequence) do
    sequence
    |> to_string()
    |> String.pad_leading(5, "0")
    |> List.wrap()
    |> Enum.concat([".vtt"])
    |> Enum.join()
    |> URI.new!()
  end

  @doc """
  Creates a new Media playlist builder validating the input playlist. Accepts
  playlists that both not yet finished and contain all segments required to
  compute their playback duration, i.e., their media_sequence_number is zero.
  """
  def new(%Media{finished: true}) do
    raise "not with a finished playlist"
  end

  def new(%Media{media_sequence_number: seq}) when seq != 0 do
    raise "not with a media_sequence_number which is not zero, it is not possible to compute the playlist's actual playback "
  end

  # https://www.rfc-editor.org/rfc/rfc8216#section-6.2.2 specifies that
  # live playlists should not contain any playlist-type tag as that does
  # not allow it to remove segments.
  def new(%Media{type: type}) when type in [:vod, :event] do
    raise "not with a playlist of type :vod|:event"
  end

  def new(playlist) do
    %__MODULE__{playlist: playlist}
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

  def pop(builder, _opts) do
    {[], builder}
    # Create the list of segments which can contain the timed_payloads. Return
    # the ones that are complete or all of them, if forced. Create a reference
    # of each segment and store it. When the segments are (na)acknoledged, add
    # them to the playlist.

    # force? = Keyword.get(opts, :force, false)

    # playback =
    #   segments
    #   |> Enum.map(fn %Segment{duration: duration} -> duration end)
    #   |> Enum.sum()

    # acc
    # |> Enum.sort(fn %{from: left}, {from: right} -> left < right end)
    # |> 
  end

  # def fit(%__MODULE__{closed: true}, payload) do
  #   raise "Cannot fit timed payload #{inspect(payload)} into a finished playlist"
  # end

  #   def fit(builder = %__MODULE__{filter_uploadables_from: nil}, timed_payload = %{from: from}) do
  #     fit(%__MODULE__{builder | filter_uploadables_from: from}, timed_payload)
  #   end

  #   def fit(
  #         builder = %__MODULE__{
  #           timed_segments: segments,
  #           to_upload: to_upload,
  #           filter_uploadables_from: filter_uploadables_from,
  #           playlist:
  #             playlist = %Media{
  #               segments: playlist_segments
  #             }
  #         },
  #         timed_payload = %{from: from}
  #       ) do
  #     segment_from = current_segment_from(segments)
  #     segment_to = current_segment_to(segments)

  #     # After this check we're ensured that the last timed segments can hold the current buffer.
  #     # Previous segments can be considered complete!
  #     [last_timed_segment | rest] =
  #       cond do
  #         from < segment_from ->
  #           raise "Cannot fit timed payload #{inspect(timed_payload)} into current segment which starts at #{inspect(segment_from)}"

  #         from >= segment_to ->
  #           extend_timed_segments_till(segments, from, builder)

  #         true ->
  #           # Segments can already hold this buffer.
  #           segments
  #       end

  #     last_timed_segment =
  #       update_in(last_timed_segment, [:acc], fn acc -> [timed_payload | acc] end)

  #     # Here we're checking wether the last buffer we put in the last segment is
  #     # going to be the last one for it. It happens when the buffer finishes after
  #     # the segment duration (or at its boundary)
  #     last_to = real_segment_to(last_timed_segment)
  #     segment_to = current_segment_to([last_timed_segment])

  #     {complete_segments, timed_segments} =
  #       if last_to >= segment_to do
  #         all = [last_timed_segment | rest]

  #         timed_segments =
  #           [last_timed_segment]
  #           |> extend_timed_segments_till(last_to, builder)
  #           |> Enum.slice(Range.new(0, -2))

  #         if length(timed_segments) > 1 do
  #           # It means that the last segment contained something that
  #           # spans over multiple segment, which we consider ready.
  #           [h | ready] = timed_segments
  #           {ready ++ all, [h]}
  #         else
  #           {all, timed_segments}
  #         end
  #       else
  #         {rest, [last_timed_segment]}
  #       end

  #     complete_segments = Enum.reverse(complete_segments)

  #     uploadable_segments =
  #       Enum.drop_while(complete_segments, fn %{to: to} -> to <= filter_uploadables_from end)

  #     new_segments =
  #       complete_segments
  #       |> Enum.map(fn %{segment: x} -> x end)

  #     playlist = %Media{playlist | segments: playlist_segments ++ new_segments}

  #     %__MODULE__{
  #       builder
  #       | timed_segments: timed_segments,
  #         to_upload: to_upload ++ uploadable_segments,
  #         playlist: playlist
  #     }
  #   end

  #   def flush(
  #         builder = %__MODULE__{
  #           timed_segments: timed_segments,
  #           to_upload: to_upload,
  #           playlist: playlist = %Media{segments: playlist_segments}
  #         }
  #       ) do
  #     timed_segments = Enum.drop_while(timed_segments, fn %{acc: acc} -> Enum.empty?(acc) end)

  #     new_segments =
  #       timed_segments
  #       |> Enum.map(fn %{segment: x} -> x end)
  #       |> Enum.reverse()

  #     playlist = %Media{
  #       playlist
  #       | segments: playlist_segments ++ new_segments,
  #         finished: true,
  #         type: :vod
  #     }

  #     %__MODULE__{
  #       builder
  #       | timed_segments: [],
  #         to_upload: to_upload ++ Enum.reverse(timed_segments),
  #         playlist: playlist,
  #         closed: true
  #     }
  #   end

  #   def playlist(%__MODULE__{playlist: playlist}), do: playlist

  #   def take_uploadables(builder = %__MODULE__{to_upload: to_upload, playlist: playlist}) do
  #     uploadables =
  #       to_upload
  #       |> Enum.map(fn %{segment: segment, acc: acc, from: from, to: to} ->
  #         %{
  #           buffers: Enum.reverse(acc),
  #           uri: Media.build_segment_uri(playlist.uri, segment.uri),
  #           from: from,
  #           to: to
  #         }
  #       end)

  #     {uploadables, %__MODULE__{builder | to_upload: []}}
  #   end

  #   defp extend_timed_segments_till(
  #          segments = [%{to: segment_to, segment: segment} | _],
  #          from,
  #          builder = %{
  #            segment_extension: extension,
  #            playlist: %Media{uri: uri, target_segment_duration: duration}
  #          }
  #        )
  #        when from >= segment_to do
  #     next_segment = generate_next_segment(segment, duration, uri, extension)

  #     segments = [
  #       %{from: segment_to, to: segment_to + duration, segment: next_segment, acc: []}
  #       | segments
  #     ]

  #     extend_timed_segments_till(segments, from, builder)
  #   end

  #   defp extend_timed_segments_till(segments, _from, _builder) do
  #     segments
  #   end

  #   defp current_segment_from([%{from: from} | _]), do: from
  #   defp current_segment_to([%{to: to} | _]), do: to

  #   defp real_segment_to(%{acc: [%{to: to} | _]}), do: to

  #   defp generate_first_segment(target_segment_duration, media_playlist_uri, segment_extension) do
  #     %Segment{
  #       duration: target_segment_duration,
  #       relative_sequence: 0,
  #       absolute_sequence: 0
  #     }
  #     |> fill_segment_uri(media_playlist_uri, segment_extension)
  #   end

  #   defp generate_next_segment(
  #          %Segment{
  #            relative_sequence: relative_sequence,
  #            absolute_sequence: absolute_sequence
  #          },
  #          duration,
  #          media_playlist_uri,
  #          segment_extension
  #        ) do
  #     %Segment{
  #       duration: duration,
  #       relative_sequence: relative_sequence + 1,
  #       absolute_sequence: absolute_sequence + 1
  #     }
  #     |> fill_segment_uri(media_playlist_uri, segment_extension)
  #   end

  #   defp fill_segment_uri(segment = %Segment{absolute_sequence: seq}, %URI{path: path}, extension) do
  #     root =
  #       path
  #       |> Path.basename(path)
  #       |> String.trim_trailing(Path.extname(path))

  #     uri =
  #       [root, filename]
  #       |> Path.join()
  #       |> URI.new!()

  #     %Segment{segment | uri: uri}
  #   end
end
