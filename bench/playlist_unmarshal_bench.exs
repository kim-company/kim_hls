defmodule PlaylistUnmarshalBench do
  @moduledoc """
  Benchmarks for HLS playlist unmarshaling performance.

  Run with: mix run bench/playlist_unmarshal_bench.exs
  """

  defmodule Fixtures do
    @moduledoc "Generates test fixtures for benchmarking"

    def generate_media_playlist(num_segments) do
      header = """
      #EXTM3U
      #EXT-X-VERSION:4
      #EXT-X-TARGETDURATION:11
      #EXT-X-MEDIA-SEQUENCE:1
      #EXT-X-PLAYLIST-TYPE:VOD
      """

      segments =
        1..num_segments
        |> Enum.map(fn i ->
          """
          #EXTINF:10.000,
          segment_#{String.pad_leading(Integer.to_string(i), 5, "0")}.ts
          """
        end)
        |> Enum.join()

      header <> segments <> "#EXT-X-ENDLIST\n"
    end

    def generate_media_playlist_with_init_sections(num_segments) do
      header = """
      #EXTM3U
      #EXT-X-VERSION:7
      #EXT-X-TARGETDURATION:11
      #EXT-X-MEDIA-SEQUENCE:1
      #EXT-X-PLAYLIST-TYPE:VOD
      #EXT-X-MAP:URI="init.mp4"
      """

      segments =
        1..num_segments
        |> Enum.map(fn i ->
          discontinuity = if rem(i, 50) == 0, do: "#EXT-X-DISCONTINUITY\n", else: ""
          new_map = if rem(i, 100) == 0, do: "#EXT-X-MAP:URI=\"init_#{div(i, 100)}.mp4\"\n", else: ""

          """
          #{discontinuity}#{new_map}#EXTINF:10.000,
          segment_#{String.pad_leading(Integer.to_string(i), 5, "0")}.m4s
          """
        end)
        |> Enum.join()

      header <> segments <> "#EXT-X-ENDLIST\n"
    end

    def generate_master_playlist(num_variants, num_audio_tracks) do
      header = """
      #EXTM3U
      #EXT-X-VERSION:4
      #EXT-X-INDEPENDENT-SEGMENTS
      """

      audio_tracks =
        1..num_audio_tracks
        |> Enum.map(fn i ->
          """
          #EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",LANGUAGE="eng",NAME="Track #{i}",AUTOSELECT=YES,DEFAULT=#{if i == 1, do: "YES", else: "NO"},URI="audio_#{i}.m3u8"
          """
        end)
        |> Enum.join()

      variants =
        1..num_variants
        |> Enum.map(fn i ->
          bandwidth = 500_000 * i
          resolution = "#{640 * i}x#{360 * i}"

          """
          #EXT-X-STREAM-INF:BANDWIDTH=#{bandwidth},AVERAGE-BANDWIDTH=#{trunc(bandwidth * 0.9)},CODECS="avc1.64001f,mp4a.40.2",RESOLUTION=#{resolution},FRAME-RATE=30.000,AUDIO="audio"
          stream_#{i}.m3u8
          """
        end)
        |> Enum.join()

      header <> audio_tracks <> variants
    end

    def load_real_fixture(path) do
      File.read!(path)
    end
  end

  # Load existing fixtures
  fixtures_base = Path.join([__DIR__, "..", "test", "fixtures"])

  real_master = Fixtures.load_real_fixture(Path.join(fixtures_base, "master_playlists/stream_with_audio_tracks.m3u8"))
  real_media = Fixtures.load_real_fixture(Path.join(fixtures_base, "bunny-ts/stream_640x360.m3u8"))

  # Generate synthetic fixtures of varying sizes
  small_media = Fixtures.generate_media_playlist(10)
  medium_media = Fixtures.generate_media_playlist(100)
  large_media = Fixtures.generate_media_playlist(1_000)
  xlarge_media = Fixtures.generate_media_playlist(10_000)

  small_media_with_init = Fixtures.generate_media_playlist_with_init_sections(10)
  medium_media_with_init = Fixtures.generate_media_playlist_with_init_sections(100)
  large_media_with_init = Fixtures.generate_media_playlist_with_init_sections(1_000)

  small_master = Fixtures.generate_master_playlist(3, 2)
  medium_master = Fixtures.generate_master_playlist(10, 5)
  large_master = Fixtures.generate_master_playlist(50, 20)

  IO.puts("\n=== Playlist Unmarshaling Benchmarks ===\n")
  IO.puts("Fixture sizes:")
  IO.puts("  Real master: #{byte_size(real_master)} bytes")
  IO.puts("  Real media: #{byte_size(real_media)} bytes")
  IO.puts("  Small media (10 segments): #{byte_size(small_media)} bytes")
  IO.puts("  Medium media (100 segments): #{byte_size(medium_media)} bytes")
  IO.puts("  Large media (1K segments): #{byte_size(large_media)} bytes")
  IO.puts("  XLarge media (10K segments): #{byte_size(xlarge_media)} bytes")
  IO.puts("  Medium media with init (100 segments): #{byte_size(medium_media_with_init)} bytes")
  IO.puts("  Large media with init (1K segments): #{byte_size(large_media_with_init)} bytes")
  IO.puts("  Small master (3 variants, 2 audio): #{byte_size(small_master)} bytes")
  IO.puts("  Medium master (10 variants, 5 audio): #{byte_size(medium_master)} bytes")
  IO.puts("  Large master (50 variants, 20 audio): #{byte_size(large_master)} bytes")
  IO.puts("")

  Benchee.run(
    %{
      "real master (small)" => fn ->
        HLS.Playlist.unmarshal(real_master, %HLS.Playlist.Master{})
      end,
      "real media (3 segments)" => fn ->
        HLS.Playlist.unmarshal(real_media, %HLS.Playlist.Media{})
      end,
      "media - 10 segments" => fn ->
        HLS.Playlist.unmarshal(small_media, %HLS.Playlist.Media{})
      end,
      "media - 100 segments" => fn ->
        HLS.Playlist.unmarshal(medium_media, %HLS.Playlist.Media{})
      end,
      "media - 1K segments" => fn ->
        HLS.Playlist.unmarshal(large_media, %HLS.Playlist.Media{})
      end,
      "media - 10K segments" => fn ->
        HLS.Playlist.unmarshal(xlarge_media, %HLS.Playlist.Media{})
      end,
      "media with init - 100 segments" => fn ->
        HLS.Playlist.unmarshal(medium_media_with_init, %HLS.Playlist.Media{})
      end,
      "media with init - 1K segments" => fn ->
        HLS.Playlist.unmarshal(large_media_with_init, %HLS.Playlist.Media{})
      end,
      "master - 3 variants, 2 audio" => fn ->
        HLS.Playlist.unmarshal(small_master, %HLS.Playlist.Master{})
      end,
      "master - 10 variants, 5 audio" => fn ->
        HLS.Playlist.unmarshal(medium_master, %HLS.Playlist.Master{})
      end,
      "master - 50 variants, 20 audio" => fn ->
        HLS.Playlist.unmarshal(large_master, %HLS.Playlist.Master{})
      end
    },
    time: 5,
    memory_time: 2,
    warmup: 2,
    formatters: [
      {Benchee.Formatters.Console, extended_statistics: true}
    ],
    print: [
      fast_warning: false
    ]
  )
end
