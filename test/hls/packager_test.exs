defmodule HLS.PackagerTest do
  use ExUnit.Case
  doctest HLS.Packager

  alias HLS.Packager

  describe "new/1" do
    test "initializes a new packager from an existing playlist" do
      manifest_uri = URI.new!("file://test/fixtures/bunny-ts/stream.m3u8")
      storage = HLS.Storage.File.new()

      packager = Packager.new(storage: storage, manifest_uri: manifest_uri)
      assert packager.master_written?
      assert map_size(packager.tracks) == 4
    end

    @tag :tmp_dir
    test "initializes a new packager from a non-existing playlist", %{tmp_dir: tmp_dir} do
      manifest_uri = URI.new!("file://#{tmp_dir}/stream.m3u8")
      storage = HLS.Storage.File.new()

      packager = Packager.new(storage: storage, manifest_uri: manifest_uri)
      refute packager.master_written?
    end

    test "fails initializing the packager on a finished playlist" do
      manifest_uri = URI.new!("file://test/fixtures/mpeg-ts/stream.m3u8")
      storage = HLS.Storage.File.new()

      assert_raise(HLS.Packager.PlaylistFinishedError, fn ->
        Packager.new(storage: storage, manifest_uri: manifest_uri)
      end)
    end
  end

  describe "add_track/2" do
    test "adds a new track to the packager" do
      packager = new_packager()

      {packager, _stream_id} =
        Packager.add_track(packager,
          stream: %HLS.VariantStream{
            uri: Packager.new_variant_uri(packager, "_video_480p"),
            bandwidth: 14_000_000,
            resolution: {1920, 1080},
            codecs: []
          },
          segment_extension: ".m4s",
          target_segment_duration: 7
        )

      assert map_size(packager.tracks) == 1
    end

    test "raises if the track already exists" do
      {packager, _track} =
        new_packager()
        |> with_track("_416x234")

      assert_raise HLS.Packager.AddTrackError, fn ->
        Packager.add_track(packager,
          stream: %HLS.VariantStream{
            uri: URI.new!("stream_416x234.m3u8"),
            bandwidth: 341_276,
            resolution: {416, 234},
            codecs: ["avc1.64000c", "mp4a.40.2"]
          },
          segment_extension: ".m4s",
          target_segment_duration: 7
        )
      end
    end

    test "fails to add a new track if the packager is already in its ready state" do
      packager = existing_packager()

      assert_raise HLS.Packager.AddTrackError, fn ->
        Packager.add_track(packager,
          stream: %HLS.VariantStream{
            uri: URI.new!("stream_new.m3u8"),
            bandwidth: 0,
            resolution: {0, 0},
            codecs: []
          },
          segment_extension: ".m4s",
          target_segment_duration: 5
        )
      end
    end
  end

  describe "put_init_section/3" do
    test "writes a new init segment if the previous one is different" do
      {packager, media} =
        new_packager()
        |> with_track("_video_480p")

      packager
      |> Packager.put_init_section(media, <<1>>)
      |> Packager.put_segment(media, <<1>>, 10_000)
      |> Packager.put_init_section(media, <<1>>)
      |> Packager.put_segment(media, <<1>>, 10_000)
      |> Packager.put_init_section(media, <<2>>)
      |> Packager.put_segment(media, <<1>>, 10_000)

      uri = URI.new!("file://x/stream_video_480p/00000/stream_video_480p_00001_init.mp4")
      assert_received {:put, ^uri, <<1>>}

      uri = URI.new!("file://x/stream_video_480p/00000/stream_video_480p_00002_init.mp4")
      refute_received {:put, ^uri, <<1>>}

      uri = URI.new!("file://x/stream_video_480p/00000/stream_video_480p_00003_init.mp4")
      assert_received {:put, ^uri, <<2>>}
    end
  end

  describe "put_segment/2" do
    test "writes segments into a pending playlist" do
      {packager, media} =
        new_packager()
        |> with_track("_video_480p")

      Packager.put_segment(packager, media, <<1>>, 10_000)

      uri = URI.new!("file://x/stream_video_480p/00000/stream_video_480p_00001.m4s")
      assert_received {:put, ^uri, <<1>>}

      uri = URI.new!("file://x/stream_video_480p_pending.m3u8")
      assert_received {:put, ^uri, _playlist}
    end
  end

  describe "sync/1" do
    test "writes all pending segments up to sync_point into the media playlist" do
      {packager, media} =
        new_packager()
        |> with_track("_video_480p")

      packager = Packager.put_segment(packager, media, <<1>>, 10_000)

      uri = URI.new!("file://x/stream_video_480p/00000/stream_video_480p_00001.m4s")
      assert_received {:put, ^uri, <<1>>}
      uri = URI.new!("file://x/stream_video_480p_pending.m3u8")
      assert_received {:put, ^uri, _playlist}
      uri = URI.new!("file://x/stream_video_480p.m3u8")
      refute_received {:put, ^uri, _playlist}

      Packager.sync(packager, 15_000)

      uri = URI.new!("file://x/stream_video_480p_pending.m3u8")
      assert_received {:put, ^uri, _playlist}
      uri = URI.new!("file://x/stream_video_480p.m3u8")
      assert_received {:put, ^uri, _playlist}
    end
  end

  describe "flush/1" do
    test "writes all pending segments into the media playlist" do
      {packager, media} =
        new_packager()
        |> with_track("_video_480p")

      packager = Packager.put_segment(packager, media, <<1>>, 10_000)

      uri = URI.new!("file://x/stream_video_480p/00000/stream_video_480p_00001.m4s")
      assert_received {:put, ^uri, <<1>>}
      uri = URI.new!("file://x/stream_video_480p_pending.m3u8")
      assert_received {:put, ^uri, _playlist}
      uri = URI.new!("file://x/stream_video_480p.m3u8")
      refute_received {:put, ^uri, _playlist}

      Packager.flush(packager)

      uri = URI.new!("file://x/stream_video_480p_pending.m3u8")
      assert_received {:put, ^uri, _playlist}
      uri = URI.new!("file://x/stream_video_480p.m3u8")
      assert_received {:put, ^uri, _playlist}
    end
  end

  describe "build_master/1" do
    test "returns the master playlist" do
      packager = existing_packager()
      master = Packager.build_master(packager)

      assert master.uri == packager.manifest_uri
      assert master.version == 4
      assert master.independent_segments == true
      assert length(master.streams) == 3
      assert length(master.alternative_renditions) == 1
    end
  end

  defp with_track(packager, track_id) do
    Packager.add_track(packager,
      stream: %HLS.VariantStream{
        uri: Packager.new_variant_uri(packager, track_id),
        bandwidth: 14_000_000,
        resolution: {1920, 1080},
        codecs: []
      },
      segment_extension: ".m4s",
      target_segment_duration: 7
    )
  end

  defp new_packager() do
    manifest_uri = URI.new!("file://x/stream.m3u8")
    storage = HLS.Storage.Test.new(self())

    Packager.new(storage: storage, manifest_uri: manifest_uri)
  end

  defp existing_packager() do
    manifest_uri = URI.new!("file://test/fixtures/bunny-ts/stream.m3u8")
    storage = HLS.Storage.File.new()

    Packager.new(storage: storage, manifest_uri: manifest_uri)
  end
end
