defmodule HLS.PackagerTest do
  use ExUnit.Case, async: true
  doctest HLS.Packager

  alias HLS.Packager
  alias HLS.VariantStream

  describe "sliding window functionality" do
    setup do
      storage = HLS.Storage.File.new()

      # Create a temporary directory for test files
      temp_dir = System.tmp_dir!() |> Path.join("hls_packager_test_#{:rand.uniform(1_000_000)}")
      File.mkdir_p!(temp_dir)

      manifest_uri = URI.new!("file://#{temp_dir}/stream.m3u8")

      on_exit(fn ->
        File.rm_rf!(temp_dir)
      end)

      %{storage: storage, manifest_uri: manifest_uri, temp_dir: temp_dir}
    end

    test "sliding window limits segments in playlists", %{
      storage: storage,
      manifest_uri: manifest_uri
    } do
      max_segments = 3

      {:ok, packager} =
        Packager.start_link(
          storage: storage,
          manifest_uri: manifest_uri,
          max_segments: max_segments
        )

      # Add a track
      :ok =
        Packager.add_track(packager, "test_track",
          stream: %VariantStream{
            uri: URI.new!("test_track.m3u8"),
            bandwidth: 1000,
            codecs: ["avc1.64001e"]
          },
          segment_extension: ".ts",
          target_segment_duration: 1
        )

      # Add more segments than the limit to trigger sliding window
      # 5 segments, limit is 3
      total_segments = max_segments + 2

      for i <- 1..total_segments do
        :ok = Packager.put_segment(packager, "test_track", "segment_#{i}_data", 1.0)
      end

      # Wait for uploads to complete and move to pending_playlist
      :timer.sleep(200)

      # Force sync with a large sync point to move all segments to media playlist
      :ok = Packager.sync(packager, 10)

      # Verify sliding window was applied
      tracks = Packager.tracks(packager)
      track = tracks["test_track"]

      # Should have exactly max_segments in media playlist (sliding window applied)
      assert length(track.media_playlist.segments) == max_segments

      # Media sequence number should be updated to reflect removed segments
      # If we added 5 segments and kept 3, then 2 were removed, so sequence should be 2
      expected_sequence = total_segments - max_segments
      assert track.media_playlist.media_sequence_number == expected_sequence

      # The segments should be the most recent ones (segments 3, 4, 5)
      segment_uris = Enum.map(track.media_playlist.segments, &to_string(&1.uri))
      # segment 4
      assert Enum.any?(segment_uris, &String.contains?(&1, "00004"))
      # segment 5
      assert Enum.any?(segment_uris, &String.contains?(&1, "00005"))
    end

    test "packager without max_segments keeps all segments", %{
      storage: storage,
      manifest_uri: manifest_uri
    } do
      {:ok, packager} =
        Packager.start_link(
          storage: storage,
          manifest_uri: manifest_uri
          # max_segments: nil (default)
        )

      # Add a track
      :ok =
        Packager.add_track(packager, "test_track",
          stream: %VariantStream{
            uri: URI.new!("test_track.m3u8"),
            bandwidth: 1000,
            codecs: ["avc1.64001e"]
          },
          segment_extension: ".ts",
          target_segment_duration: 6
        )

      # Add multiple segments
      segment_count = 10

      for i <- 1..segment_count do
        :ok = Packager.put_segment(packager, "test_track", "segment_#{i}_data", 6.0)
      end

      # Flush to move all segments to the main playlist
      :ok = Packager.flush(packager)

      # Get tracks and verify all segments are kept
      tracks = Packager.tracks(packager)
      track = tracks["test_track"]

      # Should have all segments
      assert length(track.media_playlist.segments) == segment_count

      # Media sequence number should remain 0
      assert track.media_playlist.media_sequence_number == 0
    end

    test "flush operation cleans up storage when max_segments is set", %{
      storage: storage,
      manifest_uri: manifest_uri,
      temp_dir: temp_dir
    } do
      max_segments = 2

      {:ok, packager} =
        Packager.start_link(
          storage: storage,
          manifest_uri: manifest_uri,
          max_segments: max_segments
        )

      # Add a track
      :ok =
        Packager.add_track(packager, "test_track",
          stream: %VariantStream{
            uri: URI.new!("test_track.m3u8"),
            bandwidth: 1000,
            codecs: ["avc1.64001e"]
          },
          segment_extension: ".ts",
          target_segment_duration: 6
        )

      # Add segments
      for i <- 1..4 do
        :ok = Packager.put_segment(packager, "test_track", "segment_#{i}_data", 6.0)
      end

      # Flush should clean up all storage
      :ok = Packager.flush(packager)

      # Verify that tracks are now empty (cleaned up)
      tracks = Packager.tracks(packager)
      assert tracks == %{}

      # Verify that storage files are deleted (master playlist should be gone)
      master_file = Path.join(temp_dir, "stream.m3u8")
      refute File.exists?(master_file)
    end

    test "flush operation creates VOD when max_segments is nil", %{
      storage: storage,
      manifest_uri: manifest_uri
    } do
      {:ok, packager} =
        Packager.start_link(
          storage: storage,
          manifest_uri: manifest_uri
          # max_segments: nil (default)
        )

      # Add a track
      :ok =
        Packager.add_track(packager, "test_track",
          stream: %VariantStream{
            uri: URI.new!("test_track.m3u8"),
            bandwidth: 1000,
            codecs: ["avc1.64001e"]
          },
          segment_extension: ".ts",
          target_segment_duration: 6
        )

      # Add segments
      segment_count = 4

      for i <- 1..segment_count do
        :ok = Packager.put_segment(packager, "test_track", "segment_#{i}_data", 6.0)
      end

      # Flush should create VOD playlist
      :ok = Packager.flush(packager)

      # Get tracks and verify VOD behavior
      tracks = Packager.tracks(packager)
      track = tracks["test_track"]

      # Should have all segments and be marked as finished VOD
      assert length(track.media_playlist.segments) == segment_count
      assert track.media_playlist.finished == true
      assert track.media_playlist.type == :vod
      assert track.media_playlist.media_sequence_number == 0
    end

    test "sliding window preserves shared init sections", %{
      storage: storage,
      manifest_uri: manifest_uri,
      temp_dir: temp_dir
    } do
      max_segments = 2

      {:ok, packager} =
        Packager.start_link(
          storage: storage,
          manifest_uri: manifest_uri,
          max_segments: max_segments
        )

      # Add a track that uses init sections (fragmented MP4)
      :ok =
        Packager.add_track(packager, "test_track",
          stream: %VariantStream{
            uri: URI.new!("test_track.m3u8"),
            bandwidth: 1000,
            codecs: ["avc1.64001e"]
          },
          # fragmented MP4 segments
          segment_extension: ".m4s",
          target_segment_duration: 1
        )

      # Set an init section that will be shared across segments
      shared_init_payload = "shared_init_section_data"
      :ok = Packager.put_init_section(packager, "test_track", shared_init_payload)

      # Create the expected init section file path manually to verify it exists
      # The init section should be created when the first segment is added

      # Add segments that will share the same init section
      # 4 segments, limit is 2
      total_segments = max_segments + 2

      for i <- 1..total_segments do
        :ok = Packager.put_segment(packager, "test_track", "segment_#{i}_data", 1.0)
      end

      # Wait for uploads to complete
      :timer.sleep(300)

      # Force sync to trigger sliding window
      :ok = Packager.sync(packager, 10)

      # Verify sliding window was applied
      tracks_after = Packager.tracks(packager)
      track_after = tracks_after["test_track"]

      # Should have exactly max_segments
      assert length(track_after.media_playlist.segments) == max_segments

      # Init section should still exist and be referenced by remaining segments  
      assert track_after.init_section != nil

      # All remaining segments should reference the same init section
      init_section_uris =
        track_after.media_playlist.segments
        |> Enum.map(& &1.init_section)
        |> Enum.filter(&(&1 != nil))
        |> Enum.map(& &1.uri)
        |> Enum.uniq()

      # All segments should share the same init section URI
      assert length(init_section_uris) == 1

      # Find the actual init section file path from the track
      init_section_uri = track_after.init_section.uri
      init_section_path = Path.join(temp_dir, to_string(init_section_uri))

      # The shared init section file should still exist (not deleted during cleanup)
      assert File.exists?(init_section_path),
             "Shared init section should not be deleted at #{init_section_path}"
    end

    test "assigns program date times to segments when max_segments is configured", %{
      storage: storage,
      manifest_uri: manifest_uri
    } do
      max_segments = 3

      {:ok, packager} =
        Packager.start_link(
          storage: storage,
          manifest_uri: manifest_uri,
          max_segments: max_segments
        )

      # Add a track
      :ok =
        Packager.add_track(packager, "test_track",
          stream: %VariantStream{
            uri: URI.new!("test_track.m3u8"),
            bandwidth: 1000,
            codecs: ["avc1.64001e"]
          },
          segment_extension: ".ts",
          target_segment_duration: 2
        )

      # Add segments with specific durations
      segment_durations = [2.0, 1.5, 2.5, 1.0]

      for {duration, i} <- Enum.with_index(segment_durations, 1) do
        :ok = Packager.put_segment(packager, "test_track", "segment_#{i}_data", duration)
      end

      # Wait for uploads to complete
      :timer.sleep(200)

      # Capture sync time and force sync to move segments to media playlist
      sync_start_time = DateTime.utc_now()
      :ok = Packager.sync(packager, 10)
      sync_end_time = DateTime.utc_now()

      # Get tracks and verify program date times were assigned
      tracks = Packager.tracks(packager)
      track = tracks["test_track"]

      # All segments should have program_date_time assigned
      segments_with_datetime =
        track.media_playlist.segments
        |> Enum.filter(&(&1.program_date_time != nil))

      assert length(segments_with_datetime) == length(track.media_playlist.segments),
             "All segments in sliding window mode should have program_date_time assigned"

      # Program date times should be in ascending order
      datetimes = Enum.map(track.media_playlist.segments, & &1.program_date_time)
      sorted_datetimes = Enum.sort(datetimes, DateTime)
      assert datetimes == sorted_datetimes, "Program date times should be chronologically ordered"

      # The time difference between consecutive segments should match their durations
      datetime_pairs = Enum.zip(datetimes, tl(datetimes))
      duration_pairs = Enum.zip(track.media_playlist.segments, tl(track.media_playlist.segments))

      for {{dt1, dt2}, {seg1, _seg2}} <- Enum.zip(datetime_pairs, duration_pairs) do
        expected_diff_ms = trunc(seg1.duration * 1000)
        actual_diff_ms = DateTime.diff(dt2, dt1, :millisecond)

        # Allow small margin for rounding
        assert abs(actual_diff_ms - expected_diff_ms) <= 1,
               "Time difference between segments should match segment duration. Expected: #{expected_diff_ms}ms, Actual: #{actual_diff_ms}ms"
      end

      # All program date times should be reasonable (between sync start and end, accounting for segment history)
      total_segment_duration = Enum.sum(segment_durations)

      earliest_expected =
        DateTime.add(sync_start_time, -trunc(total_segment_duration * 1000), :millisecond)

      latest_expected = sync_end_time

      for segment <- track.media_playlist.segments do
        assert DateTime.compare(segment.program_date_time, earliest_expected) != :lt,
               "Program date time should not be earlier than calculated start time"

        assert DateTime.compare(segment.program_date_time, latest_expected) != :gt,
               "Program date time should not be later than sync end time"
      end
    end

    test "does not assign program date times when max_segments is nil", %{
      storage: storage,
      manifest_uri: manifest_uri
    } do
      {:ok, packager} =
        Packager.start_link(
          storage: storage,
          manifest_uri: manifest_uri
          # max_segments: nil (default)
        )

      # Add a track  
      :ok =
        Packager.add_track(packager, "test_track",
          stream: %VariantStream{
            uri: URI.new!("test_track.m3u8"),
            bandwidth: 1000,
            codecs: ["avc1.64001e"]
          },
          segment_extension: ".ts",
          target_segment_duration: 2
        )

      # Add some segments
      for i <- 1..3 do
        :ok = Packager.put_segment(packager, "test_track", "segment_#{i}_data", 2.0)
      end

      # Wait for uploads and sync
      :timer.sleep(200)
      :ok = Packager.sync(packager, 10)

      # Get tracks and verify no program date times were assigned
      tracks = Packager.tracks(packager)
      track = tracks["test_track"]

      # No segments should have program_date_time when max_segments is nil
      segments_with_datetime =
        track.media_playlist.segments
        |> Enum.filter(&(&1.program_date_time != nil))

      assert length(segments_with_datetime) == 0,
             "No segments should have program_date_time when max_segments is nil"
    end
  end
end
