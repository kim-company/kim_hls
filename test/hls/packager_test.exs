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

      # Force sync to move segments to media playlist
      :ok = Packager.sync(packager, 10)

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
        expected_diff_seconds = round(seg1.duration)
        actual_diff_seconds = DateTime.diff(dt2, dt1, :second)

        assert actual_diff_seconds == expected_diff_seconds,
               "Time difference between segments should match rounded segment duration. Expected: #{expected_diff_seconds}s, Actual: #{actual_diff_seconds}s"
      end

      # All program date times should be reasonable - they should be close to current time
      current_time = DateTime.utc_now()

      for segment <- track.media_playlist.segments do
        # Program date times should be within a reasonable range (allowing for test execution time and clock variance)
        time_diff_seconds = DateTime.diff(segment.program_date_time, current_time, :second)

        assert abs(time_diff_seconds) <= 10,
               "Program date time should be close to current time. Diff: #{time_diff_seconds}s"
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

    test "fix_tracks synchronizes out-of-sync playlists on packager restart", %{
      storage: storage,
      manifest_uri: manifest_uri,
      temp_dir: temp_dir
    } do
      max_segments = 3

      # First, create out-of-sync playlists by manually setting up storage files
      # This simulates a scenario where tracks got out-of-sync due to different segment production rates

      # Create master playlist
      master_content = """
      #EXTM3U
      #EXT-X-VERSION:4
      #EXT-X-INDEPENDENT-SEGMENTS
      #EXT-X-STREAM-INF:BANDWIDTH=1000000,CODECS="avc1.64001e",RESOLUTION=640x360
      stream_track1.m3u8
      #EXT-X-STREAM-INF:BANDWIDTH=500000,CODECS="avc1.64001e",RESOLUTION=416x234
      stream_track2.m3u8
      """

      master_path = Path.join(temp_dir, "stream.m3u8")
      File.write!(master_path, master_content)

      # Create track1 playlist with 5 segments (ahead)
      track1_content = """
      #EXTM3U
      #EXT-X-VERSION:7
      #EXT-X-TARGETDURATION:2
      #EXT-X-MEDIA-SEQUENCE:0
      #EXTINF:2.0,
      stream_track1/00000/stream_track1_00001.ts
      #EXTINF:2.0,
      stream_track1/00000/stream_track1_00002.ts
      #EXTINF:2.0,
      stream_track1/00000/stream_track1_00003.ts
      #EXTINF:2.0,
      stream_track1/00000/stream_track1_00004.ts
      #EXTINF:2.0,
      stream_track1/00000/stream_track1_00005.ts
      """

      track1_path = Path.join(temp_dir, "stream_track1.m3u8")
      File.write!(track1_path, track1_content)

      # Create track2 playlist with 3 segments (behind)
      track2_content = """
      #EXTM3U
      #EXT-X-VERSION:7
      #EXT-X-TARGETDURATION:2
      #EXT-X-MEDIA-SEQUENCE:0
      #EXTINF:2.0,
      stream_track2/00000/stream_track2_00001.ts
      #EXTINF:2.0,
      stream_track2/00000/stream_track2_00002.ts
      #EXTINF:2.0,
      stream_track2/00000/stream_track2_00003.ts
      """

      track2_path = Path.join(temp_dir, "stream_track2.m3u8")
      File.write!(track2_path, track2_content)

      # Create the actual segment files that the playlists reference
      for track <- ["track1", "track2"] do
        for i <- 1..5 do
          segment_dir = Path.join(temp_dir, "stream_#{track}/00000")
          File.mkdir_p!(segment_dir)

          segment_path =
            Path.join(segment_dir, "stream_#{track}_#{"#{i}" |> String.pad_leading(5, "0")}.ts")

          File.write!(segment_path, "fake_segment_data_#{track}_#{i}")
        end
      end

      # Now restart the packager with max_segments configured, which should trigger fix_tracks
      {:ok, packager} =
        Packager.start_link(
          storage: storage,
          manifest_uri: manifest_uri,
          max_segments: max_segments,
          resume_finished_tracks: true
        )

      # Get the tracks to verify current state
      tracks = Packager.tracks(packager)

      # Before the fix is implemented, the tracks will be out of sync
      # track1 should have 5 segments, track2 should have 3 segments
      track1 = tracks["track1"]
      track2 = tracks["track2"]

      assert track1 != nil, "track1 should exist"
      assert track2 != nil, "track2 should exist"

      # Verify the fix worked correctly
      # 1. Both tracks should have the same segment count (synchronized to minimum = 3)
      assert track1.segment_count == 3, "track1 should be trimmed to 3 segments"
      assert track2.segment_count == 3, "track2 should keep its 3 segments"
      assert track1.segment_count == track2.segment_count, "tracks should be synchronized"

      # 2. Media sequence numbers should reflect removed segments
      assert track1.media_playlist.media_sequence_number == 2,
             "track1 should have sequence 2 (removed first 2)"

      assert track2.media_playlist.media_sequence_number == 0, "track2 should keep sequence 0"

      # 3. Verify remaining segments in track1 are the last 3 (segments 3, 4, 5)
      track1_segment_uris = Enum.map(track1.media_playlist.segments, &to_string(&1.uri))

      assert Enum.any?(track1_segment_uris, &String.contains?(&1, "00003")),
             "track1 should contain segment 3"

      assert Enum.any?(track1_segment_uris, &String.contains?(&1, "00004")),
             "track1 should contain segment 4"

      assert Enum.any?(track1_segment_uris, &String.contains?(&1, "00005")),
             "track1 should contain segment 5"

      refute Enum.any?(track1_segment_uris, &String.contains?(&1, "00001")),
             "track1 should not contain segment 1 (trimmed)"

      refute Enum.any?(track1_segment_uris, &String.contains?(&1, "00002")),
             "track1 should not contain segment 2 (trimmed)"

      # 4. Track2 should keep all its original segments (1, 2, 3)
      track2_segment_uris = Enum.map(track2.media_playlist.segments, &to_string(&1.uri))

      assert Enum.any?(track2_segment_uris, &String.contains?(&1, "00001")),
             "track2 should contain segment 1"

      assert Enum.any?(track2_segment_uris, &String.contains?(&1, "00002")),
             "track2 should contain segment 2"

      assert Enum.any?(track2_segment_uris, &String.contains?(&1, "00003")),
             "track2 should contain segment 3"

      # 5. Updated durations should reflect only the remaining segments
      # 3 segments * 2.0s each
      expected_track1_duration = 3 * 2.0
      # 3 segments * 2.0s each  
      expected_track2_duration = 3 * 2.0
      assert track1.duration == expected_track1_duration, "track1 duration should be updated"
      assert track2.duration == expected_track2_duration, "track2 duration should be correct"
    end

    test "fix_tracks synchronizes playlists with different media_sequence_number values", %{
      storage: storage,
      manifest_uri: manifest_uri,
      temp_dir: temp_dir
    } do
      max_segments = 3

      # Create master playlist
      master_content = """
      #EXTM3U
      #EXT-X-VERSION:4
      #EXT-X-INDEPENDENT-SEGMENTS
      #EXT-X-STREAM-INF:BANDWIDTH=1000000,CODECS="avc1.64001e",RESOLUTION=640x360
      stream_track1.m3u8
      #EXT-X-STREAM-INF:BANDWIDTH=500000,CODECS="avc1.64001e",RESOLUTION=416x234
      stream_track2.m3u8
      """

      master_path = Path.join(temp_dir, "stream.m3u8")
      File.write!(master_path, master_content)

      # Create track1 playlist: far ahead with media_sequence_number: 10, only 2 segments
      # segment_count = length(segments) + media_sequence_number = 2 + 10 = 12
      track1_content = """
      #EXTM3U
      #EXT-X-VERSION:7
      #EXT-X-TARGETDURATION:2
      #EXT-X-MEDIA-SEQUENCE:10
      #EXTINF:2.0,
      stream_track1/00000/stream_track1_00011.ts
      #EXTINF:2.0,
      stream_track1/00000/stream_track1_00012.ts
      """

      track1_path = Path.join(temp_dir, "stream_track1.m3u8")
      File.write!(track1_path, track1_content)

      # Create track2 playlist: behind with media_sequence_number: 0, 5 segments  
      # segment_count = length(segments) + media_sequence_number = 5 + 0 = 5
      track2_content = """
      #EXTM3U
      #EXT-X-VERSION:7
      #EXT-X-TARGETDURATION:2
      #EXT-X-MEDIA-SEQUENCE:0
      #EXTINF:2.0,
      stream_track2/00000/stream_track2_00001.ts
      #EXTINF:2.0,
      stream_track2/00000/stream_track2_00002.ts
      #EXTINF:2.0,
      stream_track2/00000/stream_track2_00003.ts
      #EXTINF:2.0,
      stream_track2/00000/stream_track2_00004.ts
      #EXTINF:2.0,
      stream_track2/00000/stream_track2_00005.ts
      """

      track2_path = Path.join(temp_dir, "stream_track2.m3u8")
      File.write!(track2_path, track2_content)

      # Create segment files for all referenced segments
      for track <- ["track1", "track2"] do
        range = if track == "track1", do: 11..12, else: 1..5

        for i <- range do
          segment_dir = Path.join(temp_dir, "stream_#{track}/00000")
          File.mkdir_p!(segment_dir)

          segment_path =
            Path.join(segment_dir, "stream_#{track}_#{"#{i}" |> String.pad_leading(5, "0")}.ts")

          File.write!(segment_path, "fake_segment_data_#{track}_#{i}")
        end
      end

      # Start packager - should trigger fix_tracks due to different segment_counts
      {:ok, packager} =
        Packager.start_link(
          storage: storage,
          manifest_uri: manifest_uri,
          max_segments: max_segments,
          resume_finished_tracks: true
        )

      # Get tracks to verify synchronization
      tracks = Packager.tracks(packager)
      track1 = tracks["track1"]
      track2 = tracks["track2"]

      assert track1 != nil, "track1 should exist"
      assert track2 != nil, "track2 should exist"

      # Since media_sequence_numbers differ (10 vs 0), this is a RESET case, not recovery
      # The fix should align both tracks to the furthest logical position
      # track1: media_sequence=10 + 2 segments = position 12
      # track2: media_sequence=0 + 5 segments = position 5
      # → Both should be reset to position 12

      # Both tracks should be aligned to the same logical position (12)
      assert track1.segment_count == 12, "track1 should have segment_count 12"
      assert track2.segment_count == 12, "track2 should have segment_count 12"
      assert track1.segment_count == track2.segment_count, "tracks should be synchronized"

      # Both tracks should have the same media_sequence_number (furthest position)
      assert track1.media_playlist.media_sequence_number == 12,
             "track1 should have media_sequence 12"

      assert track2.media_playlist.media_sequence_number == 12,
             "track2 should have media_sequence 12"

      # Both tracks should have empty playlists (reset for clean synchronization)
      assert length(track1.media_playlist.segments) == 0,
             "track1 should have empty playlist (reset)"

      assert length(track2.media_playlist.segments) == 0,
             "track2 should have empty playlist (reset)"

      # Both tracks should have zero duration (fresh start)
      assert track1.duration == 0.0, "track1 duration should be 0"
      assert track2.duration == 0.0, "track2 duration should be 0"

      # This represents a reset scenario where tracks had different media_sequence_numbers
      # indicating they were on different logical timelines. Complete reset ensures 
      # both tracks are now ready to receive segments from the same synchronized position.
    end
  end
end
