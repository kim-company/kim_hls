defmodule PlaylistMarshalBenchmark do
  @moduledoc """
  Benchmarks for HLS.Playlist.Media marshaling performance.
  Tests marshaling performance with playlists containing different numbers of segments.
  """

  alias HLS.Playlist.Media
  alias HLS.Segment

  @doc """
  Generates a list of fake segments for benchmarking purposes.

  ## Parameters
    - count: Number of segments to generate
    - opts: Options for segment generation
      - :duration - Duration of each segment in seconds (default: 6.0)
      - :with_discontinuity - Probability of discontinuity (0.0-1.0, default: 0.0)
      - :with_init_section - Whether to include init sections (default: false)
      - :with_byterange - Whether to include byteranges (default: false)
      - :with_program_date_time - Whether to include program date time (default: false)

  ## Examples

      iex> generate_segments(10)
      [%Segment{...}, ...]

      iex> generate_segments(100, with_discontinuity: 0.1, with_init_section: true)
      [%Segment{...}, ...]
  """
  def generate_segments(count, opts \\ []) when count > 0 do
    duration = Keyword.get(opts, :duration, 6.0)
    discontinuity_prob = Keyword.get(opts, :with_discontinuity, 0.0)
    with_init_section = Keyword.get(opts, :with_init_section, false)
    with_byterange = Keyword.get(opts, :with_byterange, false)
    with_program_date_time = Keyword.get(opts, :with_program_date_time, false)

    base_time = DateTime.utc_now()

    Enum.map(1..count, fn i ->
      # Add some variation to segment duration (±10%)
      segment_duration = duration + (:rand.uniform() - 0.5) * duration * 0.2

      discontinuity = if discontinuity_prob > 0, do: :rand.uniform() < discontinuity_prob, else: false

      init_section =
        if with_init_section and rem(i, 50) == 1 do
          %{uri: "init_segment_#{div(i, 50)}.mp4"}
        else
          nil
        end

      byterange =
        if with_byterange do
          %{length: 1_000_000 + :rand.uniform(500_000), offset: (i - 1) * 1_500_000}
        else
          nil
        end

      program_date_time =
        if with_program_date_time do
          DateTime.add(base_time, trunc((i - 1) * duration), :second)
        else
          nil
        end

      %Segment{
        uri: URI.parse("segment_#{String.pad_leading(Integer.to_string(i), 6, "0")}.ts"),
        ref: make_ref(),
        duration: segment_duration,
        relative_sequence: i - 1,
        absolute_sequence: i - 1,
        from: (i - 1) * duration,
        discontinuity: discontinuity,
        init_section: init_section,
        byterange: byterange,
        program_date_time: program_date_time
      }
    end)
  end

  @doc """
  Creates a media playlist with the specified number of segments.
  """
  def create_playlist(segment_count, opts \\ []) do
    target_duration = Keyword.get(opts, :target_duration, 6)
    finished = Keyword.get(opts, :finished, false)

    segments = generate_segments(segment_count, opts)

    %Media{
      version: 7,
      uri: URI.parse("playlist.m3u8"),
      target_segment_duration: target_duration,
      media_sequence_number: 0,
      discontinuity_sequence: 0,
      finished: finished,
      type: nil,
      segments: segments
    }
  end
end

# Define benchmark scenarios
scenarios = %{
  "10 segments" => 10,
  "50 segments" => 50,
  "100 segments" => 100,
  "500 segments" => 500,
  "1000 segments" => 1000,
  "2000 segments" => 2000,
  "4000 segments" => 4000,
  "8000 segments" => 8000
}

# Simple playlists (basic segments only)
IO.puts("\n=== Benchmarking Simple Playlists (basic segments) ===\n")

simple_playlists =
  scenarios
  |> Map.new(fn {name, count} ->
    {name, PlaylistMarshalBenchmark.create_playlist(count)}
  end)

Benchee.run(
  simple_playlists
  |> Map.new(fn {name, playlist} ->
    {name, fn -> HLS.Playlist.marshal(playlist) end}
  end),
  time: 5,
  memory_time: 2,
  warmup: 1,
  formatters: [
    Benchee.Formatters.Console
  ],
  print: [
    fast_warning: false
  ]
)

# Complex playlists (with discontinuities, init sections, and program date time)
IO.puts("\n=== Benchmarking Complex Playlists (with discontinuities, init sections, PDT) ===\n")

complex_playlists =
  scenarios
  |> Map.new(fn {name, count} ->
    {name,
     PlaylistMarshalBenchmark.create_playlist(count,
       with_discontinuity: 0.05,
       with_init_section: true,
       with_program_date_time: true
     )}
  end)

Benchee.run(
  complex_playlists
  |> Map.new(fn {name, playlist} ->
    {name, fn -> HLS.Playlist.marshal(playlist) end}
  end),
  time: 5,
  memory_time: 2,
  warmup: 1,
  formatters: [
    Benchee.Formatters.Console
  ],
  print: [
    fast_warning: false
  ]
)

# Playlists with byteranges (typical for fragmented MP4)
IO.puts("\n=== Benchmarking Playlists with Byteranges (fMP4 style) ===\n")

byterange_playlists =
  scenarios
  |> Map.new(fn {name, count} ->
    {name,
     PlaylistMarshalBenchmark.create_playlist(count,
       with_init_section: true,
       with_byterange: true
     )}
  end)

Benchee.run(
  byterange_playlists
  |> Map.new(fn {name, playlist} ->
    {name, fn -> HLS.Playlist.marshal(playlist) end}
  end),
  time: 5,
  memory_time: 2,
  warmup: 1,
  formatters: [
    Benchee.Formatters.Console
  ],
  print: [
    fast_warning: false
  ]
)

# Finished/VOD playlists
IO.puts("\n=== Benchmarking Finished/VOD Playlists ===\n")

vod_playlists =
  scenarios
  |> Map.new(fn {name, count} ->
    {name, PlaylistMarshalBenchmark.create_playlist(count, finished: true)}
  end)

Benchee.run(
  vod_playlists
  |> Map.new(fn {name, playlist} ->
    {name, fn -> HLS.Playlist.marshal(playlist) end}
  end),
  time: 5,
  memory_time: 2,
  warmup: 1,
  formatters: [
    Benchee.Formatters.Console
  ],
  print: [
    fast_warning: false
  ]
)
