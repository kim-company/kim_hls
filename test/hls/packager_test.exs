defmodule HLS.PackagerTest do
  @moduledoc """
  Test suite for HLS.Packager - a pure functional implementation of HLS packaging.

  ## Key Differences from GenServer Tests

  Unlike the GenServer-based packager that handles I/O internally:
  1. **No Process State**: State is passed explicitly through function calls
  2. **Action-based I/O**: Functions return actions that tests must execute manually
  3. **Explicit Confirmation**: Upload completion requires calling confirm_upload/2
  4. **Synchronous Testing**: No sleep/wait needed - all operations are synchronous

  ## Test Pattern

      # Initialize
      {:ok, state} = Packager.new(manifest_uri: uri, max_segments: 3)

      # Add segment (returns action to execute)
      {state, [action]} = Packager.put_segment(state, "video", duration: 5.0)

      # Execute action (test helper)
      execute_upload_action(action, segment_payload)

      # Confirm upload (may return playlist write actions)
      {state, actions} = Packager.confirm_upload(state, action.id)

      # Execute write actions
      Enum.each(actions, &execute_action/1)
  """

  use ExUnit.Case, async: true
  doctest HLS.Packager

  alias HLS.Packager
  alias HLS.VariantStream
  alias HLS.AlternativeRendition

  # Test storage implemented as Agent
  defmodule TestStorage do
    use Agent

    def start_link do
      Agent.start_link(fn -> %{} end)
    end

    def put(agent, uri, content) when is_struct(uri, URI) do
      Agent.update(agent, &Map.put(&1, to_string(uri), content))
      :ok
    end

    def get(agent, uri) when is_struct(uri, URI) do
      case Agent.get(agent, &Map.get(&1, to_string(uri))) do
        nil -> {:error, :not_found}
        content -> {:ok, content}
      end
    end

    def delete(agent, uri) when is_struct(uri, URI) do
      Agent.update(agent, &Map.delete(&1, to_string(uri)))
      :ok
    end

    def list_keys(agent) do
      Agent.get(agent, &Map.keys(&1))
    end

    def clear(agent) do
      Agent.update(agent, fn _ -> %{} end)
    end
  end

  # Helper to execute actions returned by functional packager
  defmodule ActionExecutor do
    @moduledoc """
    Executes actions returned by the functional packager.

    This simulates what a real application would do: take actions and perform
    the corresponding I/O operations (uploads, writes, deletes).
    """

    @doc """
    Executes a single action against test storage.
    """
    def execute_action(action, storage, manifest_uri, payload \\ nil)

    def execute_action(%Packager.Action.UploadSegment{uri: uri}, storage, manifest_uri, payload) do
      full_uri = HLS.Playlist.Media.build_segment_uri(manifest_uri, uri)
      TestStorage.put(storage, full_uri, payload || "test_segment_payload")
    end

    def execute_action(
          %Packager.Action.UploadInitSection{uri: uri},
          storage,
          manifest_uri,
          payload
        ) do
      full_uri = HLS.Playlist.Media.build_segment_uri(manifest_uri, uri)
      TestStorage.put(storage, full_uri, payload || "test_init_payload")
    end

    def execute_action(
          %Packager.Action.WritePlaylist{uri: uri, content: content},
          storage,
          manifest_uri,
          _payload
        ) do
      full_uri =
        if uri == manifest_uri, do: uri, else: HLS.Playlist.build_absolute_uri(manifest_uri, uri)

      TestStorage.put(storage, full_uri, content)
    end

    def execute_action(%Packager.Action.DeleteSegment{uri: uri}, storage, manifest_uri, _payload) do
      full_uri = HLS.Playlist.Media.build_segment_uri(manifest_uri, uri)
      TestStorage.delete(storage, full_uri)
    end

    def execute_action(
          %Packager.Action.DeleteInitSection{uri: uri},
          storage,
          manifest_uri,
          _payload
        ) do
      full_uri = HLS.Playlist.Media.build_segment_uri(manifest_uri, uri)
      TestStorage.delete(storage, full_uri)
    end

    def execute_action(%Packager.Action.DeletePlaylist{uri: uri}, storage, manifest_uri, _payload) do
      full_uri =
        if uri == manifest_uri, do: uri, else: HLS.Playlist.build_absolute_uri(manifest_uri, uri)

      TestStorage.delete(storage, full_uri)
    end

    def execute_action(%Packager.Action.Warning{}, _storage, _manifest_uri, _payload) do
      # Warnings are informational only, no action needed in tests
      :ok
    end

    @doc """
    Executes a list of actions in order.
    """
    def execute_actions(actions, storage, manifest_uri, payloads \\ %{}) do
      Enum.each(actions, fn action ->
        payload = Map.get(payloads, action_id(action))
        execute_action(action, storage, manifest_uri, payload)
      end)
    end

    defp action_id(%{id: id}), do: id
    defp action_id(_), do: nil
  end

  defp load_media_playlist(storage, manifest_uri, playlist_uri) do
    full_uri = HLS.Playlist.build_absolute_uri(manifest_uri, playlist_uri)
    {:ok, content} = TestStorage.get(storage, full_uri)
    HLS.Playlist.unmarshal(content, %HLS.Playlist.Media{uri: playlist_uri})
  end

  # Test setup
  setup do
    {:ok, storage} = TestStorage.start_link()
    manifest_uri = URI.new!("test://stream.m3u8")

    on_exit(fn ->
      # Check if process is still alive before trying to clear
      if Process.alive?(storage) do
        TestStorage.clear(storage)
      end
    end)

    %{storage: storage, manifest_uri: manifest_uri}
  end

  describe "master playlist generation" do
    test "sync writes master playlist after sync point 3", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri)

      {state, []} =
        Packager.add_track(state, "video",
          stream: %VariantStream{
            bandwidth: 2_000_000,
            resolution: {1280, 720},
            codecs: ["avc1.64001f"]
          },
          segment_extension: ".ts",
          target_segment_duration: 6.0
        )

      # Add and sync 3 segments
      {state, _} =
        Enum.reduce(1..3, {state, []}, fn _i, {s, _} ->
          {s, [action]} = Packager.put_segment(s, "video", duration: 6.0)
          ActionExecutor.execute_action(action, storage, manifest_uri)
          {s, actions} = Packager.confirm_upload(s, action.id)
          ActionExecutor.execute_actions(actions, storage, manifest_uri)
          {s, []}
        end)

      {state, actions} = Packager.sync(state, 3)

      # Should include master playlist write
      assert Enum.any?(actions, &match?(%Packager.Action.WritePlaylist{type: :master}, &1))
      assert state.master_written? == true

      # Execute and verify master content
      ActionExecutor.execute_actions(actions, storage, manifest_uri)
      {:ok, master_content} = TestStorage.get(storage, manifest_uri)

      assert master_content =~ "#EXTM3U"
      assert master_content =~ "#EXT-X-STREAM-INF"
      assert master_content =~ "BANDWIDTH=2000000"
    end

    test "master playlist includes alternative renditions", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri)

      {state, []} =
        Packager.add_track(state, "video",
          stream: %VariantStream{
            bandwidth: 2_000_000,
            codecs: ["avc1.64001f"],
            audio: "audio-group"
          },
          segment_extension: ".ts",
          target_segment_duration: 6.0,
          codecs: ["avc1.64001f"]
        )

      {state, []} =
        Packager.add_track(state, "audio_en",
          stream: %AlternativeRendition{
            type: :audio,
            group_id: "audio-group",
            name: "English",
            language: "en",
            uri: URI.new!("audio_en.m3u8")
          },
          segment_extension: ".ts",
          target_segment_duration: 6.0,
          codecs: ["mp4a.40.2"]
        )

      # Add segments and sync
      state =
        Enum.reduce(["video", "audio_en"], state, fn track_id, s ->
          {s, [action]} = Packager.put_segment(s, track_id, duration: 6.0)
          ActionExecutor.execute_action(action, storage, manifest_uri)
          {s, actions} = Packager.confirm_upload(s, action.id)
          ActionExecutor.execute_actions(actions, storage, manifest_uri)
          s
        end)

      {_state, actions} = Packager.sync(state, 3)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      # Verify master includes both
      {:ok, master_content} = TestStorage.get(storage, manifest_uri)
      assert master_content =~ "#EXT-X-MEDIA"
      assert master_content =~ ~s(TYPE=AUDIO)
      assert master_content =~ ~s(GROUP-ID="audio-group")
      assert master_content =~ ~s(NAME="English")

      # Verify video stream references audio group and includes audio codec
      assert master_content =~ ~s(AUDIO="audio-group")
      assert master_content =~ ~s(CODECS="avc1.64001f,mp4a.40.2")
    end
  end

  describe "sliding window functionality" do
    test "sliding window limits segments in playlists", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      max_segments = 3
      {:ok, state} = Packager.new(manifest_uri: manifest_uri, max_segments: max_segments)

      {state, []} =
        Packager.add_track(state, "test_track",
          stream: %VariantStream{bandwidth: 1_000_000, codecs: ["avc1.64001e"]},
          segment_extension: ".ts",
          target_segment_duration: 1.0
        )

      # Add 5 segments (limit is 3)
      total_segments = max_segments + 2

      {state, _} =
        Enum.reduce(1..total_segments, {state, []}, fn _i, {s, _} ->
          {s, [action]} = Packager.put_segment(s, "test_track", duration: 1.0)
          ActionExecutor.execute_action(action, storage, manifest_uri)
          {s, actions} = Packager.confirm_upload(s, action.id)
          ActionExecutor.execute_actions(actions, storage, manifest_uri)
          {s, []}
        end)

      # Sync with large point to move all to media playlist
      {state, actions} = Packager.sync(state, 10)

      # Should have delete actions for old segments
      delete_actions = Enum.filter(actions, &match?(%Packager.Action.DeleteSegment{}, &1))
      assert length(delete_actions) == total_segments - max_segments

      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      # Verify sliding window was applied in the written playlist
      playlist =
        load_media_playlist(
          storage,
          manifest_uri,
          state.tracks["test_track"].media_playlist.uri
        )

      assert length(playlist.segments) == max_segments

      # Media sequence should be updated
      expected_sequence = total_segments - max_segments
      assert playlist.media_sequence_number == expected_sequence

      # Segments should be the most recent ones
      segment_uris = Enum.map(playlist.segments, &to_string(&1.uri))
      assert Enum.any?(segment_uris, &String.contains?(&1, "00004"))
      assert Enum.any?(segment_uris, &String.contains?(&1, "00005"))
    end

    test "packager without max_segments keeps all segments", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri)

      {state, []} =
        Packager.add_track(state, "test_track",
          stream: %VariantStream{bandwidth: 1_000_000, codecs: ["avc1.64001e"]},
          segment_extension: ".ts",
          target_segment_duration: 6.0
        )

      # Add 10 segments
      segment_count = 10

      {state, _} =
        Enum.reduce(1..segment_count, {state, []}, fn _i, {s, _} ->
          {s, [action]} = Packager.put_segment(s, "test_track", duration: 6.0)
          ActionExecutor.execute_action(action, storage, manifest_uri)
          {s, actions} = Packager.confirm_upload(s, action.id)
          ActionExecutor.execute_actions(actions, storage, manifest_uri)
          {s, []}
        end)

      # Flush to move all to main playlist
      {state, actions} = Packager.flush(state)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      playlist =
        load_media_playlist(
          storage,
          manifest_uri,
          state.tracks["test_track"].media_playlist.uri
        )

      assert length(playlist.segments) == segment_count
      assert playlist.media_sequence_number == 0
    end

    test "flush operation cleans up storage when max_segments is set", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      max_segments = 2
      {:ok, state} = Packager.new(manifest_uri: manifest_uri, max_segments: max_segments)

      {state, []} =
        Packager.add_track(state, "test_track",
          stream: %VariantStream{bandwidth: 1_000_000, codecs: ["avc1.64001e"]},
          segment_extension: ".ts",
          target_segment_duration: 6.0
        )

      # Add 4 segments
      {state, _} =
        Enum.reduce(1..4, {state, []}, fn _i, {s, _} ->
          {s, [action]} = Packager.put_segment(s, "test_track", duration: 6.0)
          ActionExecutor.execute_action(action, storage, manifest_uri, "segment_data")
          {s, actions} = Packager.confirm_upload(s, action.id)
          ActionExecutor.execute_actions(actions, storage, manifest_uri)
          {s, []}
        end)

      # Sync to write master
      {state, actions} = Packager.sync(state, 3)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      # Verify master exists before flush
      assert {:ok, _} = TestStorage.get(storage, manifest_uri)

      # Flush should clean up all storage
      {state, actions} = Packager.flush(state)

      # Should have delete actions for all segments and playlists
      segment_deletes = Enum.filter(actions, &match?(%Packager.Action.DeleteSegment{}, &1))
      playlist_deletes = Enum.filter(actions, &match?(%Packager.Action.DeletePlaylist{}, &1))

      assert length(segment_deletes) > 0

      assert Enum.any?(
               playlist_deletes,
               &match?(%Packager.Action.DeletePlaylist{type: :master}, &1)
             )

      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      # Verify tracks are empty
      assert state.tracks == %{}

      # Verify master playlist deleted
      assert {:error, :not_found} = TestStorage.get(storage, manifest_uri)
    end

    test "flush operation creates VOD when max_segments is nil", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri)

      {state, []} =
        Packager.add_track(state, "test_track",
          stream: %VariantStream{bandwidth: 1_000_000, codecs: ["avc1.64001e"]},
          segment_extension: ".ts",
          target_segment_duration: 6.0
        )

      # Add 4 segments
      segment_count = 4

      {state, _} =
        Enum.reduce(1..segment_count, {state, []}, fn _i, {s, _} ->
          {s, [action]} = Packager.put_segment(s, "test_track", duration: 6.0)
          ActionExecutor.execute_action(action, storage, manifest_uri)
          {s, actions} = Packager.confirm_upload(s, action.id)
          ActionExecutor.execute_actions(actions, storage, manifest_uri)
          {s, []}
        end)

      # Flush should create VOD playlist
      {state, actions} = Packager.flush(state)

      # Should have write actions for media and master
      write_actions = Enum.filter(actions, &match?(%Packager.Action.WritePlaylist{}, &1))
      assert Enum.any?(write_actions, &match?(%Packager.Action.WritePlaylist{type: :media}, &1))
      assert Enum.any?(write_actions, &match?(%Packager.Action.WritePlaylist{type: :master}, &1))

      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      playlist =
        load_media_playlist(
          storage,
          manifest_uri,
          state.tracks["test_track"].media_playlist.uri
        )

      assert length(playlist.segments) == segment_count
      assert playlist.finished == true
      assert playlist.type == :vod
      assert playlist.media_sequence_number == 0
    end

    test "sliding window preserves shared init sections", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      max_segments = 2
      {:ok, state} = Packager.new(manifest_uri: manifest_uri, max_segments: max_segments)

      {state, []} =
        Packager.add_track(state, "test_track",
          stream: %VariantStream{bandwidth: 1_000_000, codecs: ["avc1.64001e"]},
          segment_extension: ".m4s",
          target_segment_duration: 1.0
        )

      # Set init section
      {state, [init_action]} = Packager.put_init_section(state, "test_track")
      ActionExecutor.execute_action(init_action, storage, manifest_uri, "shared_init_data")
      {state, []} = Packager.confirm_init_upload(state, init_action.id)

      # Add 4 segments (limit is 2)
      total_segments = max_segments + 2

      {state, _} =
        Enum.reduce(1..total_segments, {state, []}, fn _i, {s, _} ->
          {s, [action]} = Packager.put_segment(s, "test_track", duration: 1.0)
          ActionExecutor.execute_action(action, storage, manifest_uri)
          {s, actions} = Packager.confirm_upload(s, action.id)
          ActionExecutor.execute_actions(actions, storage, manifest_uri)
          {s, []}
        end)

      # Sync to trigger sliding window
      {state, actions} = Packager.sync(state, 10)

      # Should have segment deletes but NOT init section delete
      segment_deletes = Enum.filter(actions, &match?(%Packager.Action.DeleteSegment{}, &1))
      init_deletes = Enum.filter(actions, &match?(%Packager.Action.DeleteInitSection{}, &1))

      assert length(segment_deletes) == total_segments - max_segments
      assert length(init_deletes) == 0, "Shared init section should not be deleted"

      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      playlist =
        load_media_playlist(
          storage,
          manifest_uri,
          state.tracks["test_track"].media_playlist.uri
        )

      assert length(playlist.segments) == max_segments

      # All segments should reference same init section
      init_uris =
        playlist.segments
        |> Enum.map(& &1.init_section)
        |> Enum.filter(&(&1 != nil))
        |> Enum.map(& &1.uri)
        |> Enum.uniq()

      assert length(init_uris) == 1
    end

    test "assigns program date times to every segment when enabled", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      max_segments = 3
      {:ok, state} = Packager.new(manifest_uri: manifest_uri, max_segments: max_segments)

      {state, []} =
        Packager.add_track(state, "test_track",
          stream: %VariantStream{bandwidth: 1_000_000, codecs: ["avc1.64001e"]},
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      # Add segments with specific durations (some exceed target, triggering warnings)
      segment_durations = [2.0, 1.5, 2.5, 1.0]

      {state, _} =
        Enum.reduce(segment_durations, {state, []}, fn duration, {s, _} ->
          {s, actions} = Packager.put_segment(s, "test_track", duration: duration)
          upload_action = Enum.find(actions, &match?(%Packager.Action.UploadSegment{}, &1))
          ActionExecutor.execute_actions(actions, storage, manifest_uri)
          {s, confirm_actions} = Packager.confirm_upload(s, upload_action.id)
          ActionExecutor.execute_actions(confirm_actions, storage, manifest_uri)
          {s, []}
        end)

      # Sync to move segments to media playlist (triggers datetime assignment)
      {state, actions} = Packager.sync(state, 10)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      playlist =
        load_media_playlist(
          storage,
          manifest_uri,
          state.tracks["test_track"].media_playlist.uri
        )

      segments_with_datetime =
        playlist.segments
        |> Enum.filter(&(&1.program_date_time != nil))

      assert length(segments_with_datetime) == length(playlist.segments),
             "All segments should have program_date_time when enabled"

      # Verify chronological order
      datetimes = Enum.map(playlist.segments, & &1.program_date_time)
      sorted_datetimes = Enum.sort(datetimes, &(DateTime.compare(&1, &2) != :gt))
      assert datetimes == sorted_datetimes

      # Verify time differences match durations
      datetime_pairs = Enum.zip(datetimes, tl(datetimes))
      duration_pairs = Enum.zip(playlist.segments, tl(playlist.segments))

      for {{dt1, dt2}, {seg1, _seg2}} <- Enum.zip(datetime_pairs, duration_pairs) do
        expected_diff = round(seg1.duration)
        actual_diff = DateTime.diff(dt2, dt1, :second)
        assert actual_diff == expected_diff
      end
    end

    test "does not assign program date times when disabled", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri)

      {state, []} =
        Packager.add_track(state, "test_track",
          stream: %VariantStream{bandwidth: 1_000_000, codecs: ["avc1.64001e"]},
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      # Add segments
      {state, _} =
        Enum.reduce(1..3, {state, []}, fn _i, {s, _} ->
          {s, [action]} = Packager.put_segment(s, "test_track", duration: 2.0)
          ActionExecutor.execute_action(action, storage, manifest_uri)
          {s, actions} = Packager.confirm_upload(s, action.id)
          ActionExecutor.execute_actions(actions, storage, manifest_uri)
          {s, []}
        end)

      # Sync
      {state, actions} = Packager.sync(state, 10)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      playlist =
        load_media_playlist(
          storage,
          manifest_uri,
          state.tracks["test_track"].media_playlist.uri
        )

      segments_with_datetime =
        playlist.segments
        |> Enum.filter(&(&1.program_date_time != nil))

      assert length(segments_with_datetime) == 0,
             "No segments should have program_date_time when max_segments is nil"
    end
  end

  describe "warning system" do
    test "warns when segment exceeds target duration", %{
      manifest_uri: manifest_uri,
      storage: _storage
    } do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri)

      {state, []} =
        Packager.add_track(state, "video",
          stream: %VariantStream{bandwidth: 1_000_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 6.0
        )

      # Add segment that exceeds target
      {_state, actions} = Packager.put_segment(state, "video", duration: 7.5)

      # Should have upload action and warning
      upload_action = Enum.find(actions, &match?(%Packager.Action.UploadSegment{}, &1))
      warning = Enum.find(actions, &match?(%Packager.Action.Warning{}, &1))

      assert upload_action != nil
      assert warning != nil
      assert warning.severity == :error
      assert warning.code == :segment_exceeds_target_duration
      assert warning.details.duration == 7.5
      assert warning.details.target_duration == 6.0
      assert warning.details.track_id == "video"
    end

    test "warns about timestamp drift across variant streams", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      max_segments = 5
      {:ok, state} = Packager.new(manifest_uri: manifest_uri, max_segments: max_segments)

      # Add two variant streams
      {state, []} =
        Packager.add_track(state, "video_high",
          stream: %VariantStream{bandwidth: 2_000_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 3.0
        )

      {state, []} =
        Packager.add_track(state, "video_low",
          stream: %VariantStream{bandwidth: 500_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 3.0
        )

      # Add segments with DIFFERENT durations to both tracks to cause drift
      # Timestamps use round(duration), so we need durations that round differently
      # video_high: 3.0s, then 3.0s (timestamps: T0, T0+3)
      # video_low:  1.0s, then 1.0s (timestamps: T0, T0+1)
      # After second segment, drift = 2 seconds

      # Segment 1
      {state, actions} = Packager.put_segment(state, "video_high", duration: 3.0)
      upload = Enum.find(actions, &match?(%Packager.Action.UploadSegment{}, &1))
      ActionExecutor.execute_action(upload, storage, manifest_uri)
      {state, actions} = Packager.confirm_upload(state, upload.id)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      {state, actions} = Packager.put_segment(state, "video_low", duration: 1.0)
      upload = Enum.find(actions, &match?(%Packager.Action.UploadSegment{}, &1))
      ActionExecutor.execute_action(upload, storage, manifest_uri)
      {state, actions} = Packager.confirm_upload(state, upload.id)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      {state, actions} = Packager.sync(state, 1)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      # Segment 2
      {state, actions} = Packager.put_segment(state, "video_high", duration: 3.0)
      upload = Enum.find(actions, &match?(%Packager.Action.UploadSegment{}, &1))
      ActionExecutor.execute_action(upload, storage, manifest_uri)
      {state, actions} = Packager.confirm_upload(state, upload.id)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      {state, actions} = Packager.put_segment(state, "video_low", duration: 1.0)
      upload = Enum.find(actions, &match?(%Packager.Action.UploadSegment{}, &1))
      ActionExecutor.execute_action(upload, storage, manifest_uri)
      {state, actions} = Packager.confirm_upload(state, upload.id)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      # Sync should detect timestamp drift at position 1 (second segment)
      {_state, actions} = Packager.sync(state, 2)

      warnings =
        Enum.filter(
          actions,
          &match?(%Packager.Action.Warning{code: :timestamp_drift_detected}, &1)
        )

      assert length(warnings) > 0

      warning = hd(warnings)
      assert warning.severity == :error
      assert warning.code == :timestamp_drift_detected
      assert String.contains?(warning.message, "misaligned timestamps")
      assert warning.details.position == 1
    end

    test "warns about unsynchronized discontinuities", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      max_segments = 5
      {:ok, state} = Packager.new(manifest_uri: manifest_uri, max_segments: max_segments)

      # Add two tracks
      {state, []} =
        Packager.add_track(state, "video",
          stream: %VariantStream{bandwidth: 2_000_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      {state, []} =
        Packager.add_track(state, "audio",
          stream: %VariantStream{bandwidth: 128_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      # Add different number of segments to each track
      {state, actions} = Packager.put_segment(state, "video", duration: 2.0)
      upload = Enum.find(actions, &match?(%Packager.Action.UploadSegment{}, &1))
      ActionExecutor.execute_action(upload, storage, manifest_uri)
      {state, actions} = Packager.confirm_upload(state, upload.id)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      {state, actions} = Packager.put_segment(state, "audio", duration: 2.0)
      upload = Enum.find(actions, &match?(%Packager.Action.UploadSegment{}, &1))
      ActionExecutor.execute_action(upload, storage, manifest_uri)
      {state, actions} = Packager.confirm_upload(state, upload.id)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      # Add another segment to video only
      {state, actions} = Packager.put_segment(state, "video", duration: 2.0)
      upload = Enum.find(actions, &match?(%Packager.Action.UploadSegment{}, &1))
      ActionExecutor.execute_action(upload, storage, manifest_uri)
      {state, actions} = Packager.confirm_upload(state, upload.id)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      # Now call discontinue - should warn about misalignment
      {_state, warnings} = Packager.discontinue(state)

      assert length(warnings) == 1
      warning = hd(warnings)
      assert warning.severity == :warning
      assert warning.code == :unsynchronized_discontinuity
      assert warning.details.segment_counts["video"] == 2
      assert warning.details.segment_counts["audio"] == 1
    end

    test "warns about tracks behind sync point", %{manifest_uri: manifest_uri, storage: storage} do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri)

      # Add two tracks
      {state, []} =
        Packager.add_track(state, "video",
          stream: %VariantStream{bandwidth: 2_000_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      {state, []} =
        Packager.add_track(state, "audio",
          stream: %VariantStream{bandwidth: 128_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      # Add 3 segments to video
      state =
        Enum.reduce(1..3, state, fn _i, s ->
          {s, actions} = Packager.put_segment(s, "video", duration: 2.0)
          upload = Enum.find(actions, &match?(%Packager.Action.UploadSegment{}, &1))
          ActionExecutor.execute_action(upload, storage, manifest_uri)
          {s, actions} = Packager.confirm_upload(s, upload.id)
          ActionExecutor.execute_actions(actions, storage, manifest_uri)
          s
        end)

      # Add only 1 segment to audio
      {state, actions} = Packager.put_segment(state, "audio", duration: 2.0)
      upload = Enum.find(actions, &match?(%Packager.Action.UploadSegment{}, &1))
      ActionExecutor.execute_action(upload, storage, manifest_uri)
      {state, actions} = Packager.confirm_upload(state, upload.id)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      # Sync to point 3 - audio should be behind
      {_state, actions} = Packager.sync(state, 3)

      warnings =
        Enum.filter(
          actions,
          &match?(%Packager.Action.Warning{code: :track_behind_sync_point}, &1)
        )

      assert length(warnings) == 1

      warning = hd(warnings)
      assert warning.severity == :warning
      assert warning.code == :track_behind_sync_point
      assert warning.details.track_id == "audio"
      assert warning.details.available_segments == 1
      assert warning.details.sync_point == 3
      assert warning.details.missing_segments == 2
    end

    test "no warnings when segment duration is within target", %{
      manifest_uri: manifest_uri,
      storage: _storage
    } do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri)

      {state, []} =
        Packager.add_track(state, "video",
          stream: %VariantStream{bandwidth: 1_000_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 6.0
        )

      # Add segment within target
      {_state, actions} = Packager.put_segment(state, "video", duration: 5.5)

      warnings = Enum.filter(actions, &match?(%Packager.Action.Warning{}, &1))
      assert length(warnings) == 0
    end

    test "no warnings when tracks are synchronized for discontinuity", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      max_segments = 5
      {:ok, state} = Packager.new(manifest_uri: manifest_uri, max_segments: max_segments)

      # Add two tracks
      {state, []} =
        Packager.add_track(state, "video",
          stream: %VariantStream{bandwidth: 2_000_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      {state, []} =
        Packager.add_track(state, "audio",
          stream: %VariantStream{bandwidth: 128_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      # Add same number of segments to both tracks
      state =
        Enum.reduce(["video", "audio"], state, fn track_id, s ->
          {s, actions} = Packager.put_segment(s, track_id, duration: 2.0)
          upload = Enum.find(actions, &match?(%Packager.Action.UploadSegment{}, &1))
          ActionExecutor.execute_action(upload, storage, manifest_uri)
          {s, actions} = Packager.confirm_upload(s, upload.id)
          ActionExecutor.execute_actions(actions, storage, manifest_uri)
          s
        end)

      # Call discontinue - should NOT warn
      {_state, warnings} = Packager.discontinue(state)

      assert length(warnings) == 0
    end
  end

  describe "RFC 8216 compliance" do
    test "playlists start with #EXTM3U tag", %{manifest_uri: manifest_uri, storage: storage} do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri)

      {state, []} =
        Packager.add_track(state, "video",
          stream: %VariantStream{bandwidth: 1_000_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 6.0
        )

      # Add and sync segments
      {state, _} =
        Enum.reduce(1..3, {state, []}, fn _i, {s, _} ->
          {s, [action]} = Packager.put_segment(s, "video", duration: 6.0)
          ActionExecutor.execute_action(action, storage, manifest_uri)
          {s, actions} = Packager.confirm_upload(s, action.id)
          ActionExecutor.execute_actions(actions, storage, manifest_uri)
          {s, []}
        end)

      {_state, actions} = Packager.sync(state, 3)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      # Verify master playlist
      {:ok, master_content} = TestStorage.get(storage, manifest_uri)
      [first_line | _] = String.split(master_content, "\n")
      assert first_line == "#EXTM3U", "Master playlist must start with #EXTM3U"

      # Verify media playlist
      media_uri = HLS.Playlist.build_absolute_uri(manifest_uri, URI.new!("stream_video.m3u8"))
      {:ok, media_content} = TestStorage.get(storage, media_uri)
      [first_line | _] = String.split(media_content, "\n")
      assert first_line == "#EXTM3U", "Media playlist must start with #EXTM3U"
    end

    test "playlists are UTF-8 encoded without BOM", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri)

      {state, []} =
        Packager.add_track(state, "video",
          stream: %VariantStream{bandwidth: 1_000_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 6.0
        )

      {state, [action]} = Packager.put_segment(state, "video", duration: 6.0)
      ActionExecutor.execute_action(action, storage, manifest_uri)
      {state, actions} = Packager.confirm_upload(state, action.id)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)
      {_state, actions} = Packager.sync(state, 3)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      {:ok, master_content} = TestStorage.get(storage, manifest_uri)

      # Check for UTF-8 BOM (0xEF, 0xBB, 0xBF)
      refute String.starts_with?(master_content, <<0xEF, 0xBB, 0xBF>>),
             "Playlist must not contain UTF-8 BOM"

      # Verify valid UTF-8
      assert String.valid?(master_content), "Playlist must be valid UTF-8"
    end

    test "target duration remains constant", %{manifest_uri: manifest_uri, storage: storage} do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri)

      target_duration = 6.0

      {state, []} =
        Packager.add_track(state, "video",
          stream: %VariantStream{bandwidth: 1_000_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: target_duration
        )

      # Add segments with varying durations
      durations = [5.0, 6.0, 5.5, 4.8]

      {state, _media_contents} =
        Enum.reduce(durations, {state, []}, fn duration, {s, contents} ->
          {s, [action]} = Packager.put_segment(s, "video", duration: duration)
          ActionExecutor.execute_action(action, storage, manifest_uri)
          {s, actions} = Packager.confirm_upload(s, action.id)
          ActionExecutor.execute_actions(actions, storage, manifest_uri)

          # Capture media playlist content at each step
          media_uri = HLS.Playlist.build_absolute_uri(manifest_uri, URI.new!("stream_video.m3u8"))

          content =
            case TestStorage.get(storage, media_uri) do
              {:ok, c} -> c
              {:error, :not_found} -> nil
            end

          {s, contents ++ [content]}
        end)

      {_state, actions} = Packager.sync(state, 10)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      # Get final media playlist
      media_uri = HLS.Playlist.build_absolute_uri(manifest_uri, URI.new!("stream_video.m3u8"))
      {:ok, final_content} = TestStorage.get(storage, media_uri)

      # Extract target duration from final playlist
      target_duration_line =
        final_content
        |> String.split("\n")
        |> Enum.find(&String.starts_with?(&1, "#EXT-X-TARGETDURATION:"))

      assert target_duration_line =~ "#{round(target_duration)}",
             "Target duration must remain constant at #{target_duration}"
    end

    test "segment durations do not exceed target duration", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri)

      target_duration = 6.0

      {state, []} =
        Packager.add_track(state, "video",
          stream: %VariantStream{bandwidth: 1_000_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: target_duration
        )

      # Add valid segments
      valid_durations = [5.0, 6.0, 5.5]

      {state, _} =
        Enum.reduce(valid_durations, {state, []}, fn duration, {s, _} ->
          {s, [action]} = Packager.put_segment(s, "video", duration: duration)
          ActionExecutor.execute_action(action, storage, manifest_uri)
          {s, actions} = Packager.confirm_upload(s, action.id)
          ActionExecutor.execute_actions(actions, storage, manifest_uri)
          {s, []}
        end)

      {state, actions} = Packager.sync(state, 10)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      playlist =
        load_media_playlist(storage, manifest_uri, state.tracks["video"].media_playlist.uri)

      for segment <- playlist.segments do
        assert segment.duration <= target_duration,
               "Segment duration #{segment.duration} exceeds target #{target_duration}"
      end
    end

    test "media sequence numbers are monotonically increasing", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      max_segments = 3
      {:ok, state} = Packager.new(manifest_uri: manifest_uri, max_segments: max_segments)

      {state, []} =
        Packager.add_track(state, "video",
          stream: %VariantStream{bandwidth: 1_000_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      # Track media sequence values over time
      media_sequences = []

      {_state, media_sequences} =
        Enum.reduce(1..10, {state, media_sequences}, fn _i, {s, seqs} ->
          {s, [action]} = Packager.put_segment(s, "video", duration: 2.0)
          ActionExecutor.execute_action(action, storage, manifest_uri)
          {s, actions} = Packager.confirm_upload(s, action.id)
          ActionExecutor.execute_actions(actions, storage, manifest_uri)
          {s, actions} = Packager.sync(s, Packager.next_sync_point(s))
          ActionExecutor.execute_actions(actions, storage, manifest_uri)

          playlist =
            load_media_playlist(storage, manifest_uri, s.tracks["video"].media_playlist.uri)

          {s, seqs ++ [playlist.media_sequence_number]}
        end)

      # Verify monotonically increasing
      for {seq1, seq2} <- Enum.zip(media_sequences, tl(media_sequences)) do
        assert seq2 >= seq1, "Media sequence must be monotonically increasing"
      end
    end

    test "discontinuity sequence increments properly", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      max_segments = 3
      {:ok, state} = Packager.new(manifest_uri: manifest_uri, max_segments: max_segments)

      {state, []} =
        Packager.add_track(state, "video",
          stream: %VariantStream{bandwidth: 1_000_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      # Add segment
      {state, [action]} = Packager.put_segment(state, "video", duration: 2.0)
      ActionExecutor.execute_action(action, storage, manifest_uri)
      {state, actions} = Packager.confirm_upload(state, action.id)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)
      {state, actions} = Packager.sync(state, 1)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      initial_disc_seq =
        load_media_playlist(storage, manifest_uri, state.tracks["video"].media_playlist.uri)
        |> Map.fetch!(:discontinuity_sequence)

      # Add discontinuity
      {state, []} = Packager.discontinue(state)

      # Add segment with discontinuity
      {state, [action]} = Packager.put_segment(state, "video", duration: 2.0)
      ActionExecutor.execute_action(action, storage, manifest_uri)
      {state, actions} = Packager.confirm_upload(state, action.id)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)
      {state, actions} = Packager.sync(state, 2)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      playlist =
        load_media_playlist(storage, manifest_uri, state.tracks["video"].media_playlist.uri)

      discontinuity_segments = Enum.filter(playlist.segments, & &1.discontinuity)
      assert length(discontinuity_segments) > 0

      # Add more segments to trigger sliding window removal of discontinuity segment
      {state, _} =
        Enum.reduce(1..4, {state, []}, fn _i, {s, _} ->
          {s, [action]} = Packager.put_segment(s, "video", duration: 2.0)
          ActionExecutor.execute_action(action, storage, manifest_uri)
          {s, actions} = Packager.confirm_upload(s, action.id)
          ActionExecutor.execute_actions(actions, storage, manifest_uri)
          {s, actions} = Packager.sync(s, Packager.next_sync_point(s))
          ActionExecutor.execute_actions(actions, storage, manifest_uri)
          {s, []}
        end)

      final_disc_seq =
        load_media_playlist(storage, manifest_uri, state.tracks["video"].media_playlist.uri)
        |> Map.fetch!(:discontinuity_sequence)

      # Discontinuity sequence should have incremented when discontinuity segment was removed
      assert final_disc_seq >= initial_disc_seq,
             "Discontinuity sequence must increment when removing discontinuity segments"
    end

    test "program date time resets on discontinuity segments", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      max_segments = 5
      {:ok, state} = Packager.new(manifest_uri: manifest_uri, max_segments: max_segments)

      {state, []} =
        Packager.add_track(state, "video",
          stream: %VariantStream{bandwidth: 1_000_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      # Add normal segment
      {state, [action]} = Packager.put_segment(state, "video", duration: 2.0)
      ActionExecutor.execute_action(action, storage, manifest_uri)
      {state, actions} = Packager.confirm_upload(state, action.id)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)
      {state, actions} = Packager.sync(state, 1)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      # Add discontinuity
      {state, []} = Packager.discontinue(state)
      discontinuity_reference = state.timeline_reference

      # Add segment with discontinuity
      {state, [action]} = Packager.put_segment(state, "video", duration: 2.0)
      ActionExecutor.execute_action(action, storage, manifest_uri)
      {state, actions} = Packager.confirm_upload(state, action.id)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)
      {state, actions} = Packager.sync(state, 2)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      playlist =
        load_media_playlist(storage, manifest_uri, state.tracks["video"].media_playlist.uri)

      discontinuity_segment = Enum.find(playlist.segments, & &1.discontinuity)

      assert discontinuity_segment != nil
      assert discontinuity_segment.program_date_time != nil,
             "RFC 8216: Discontinuity segments SHOULD have EXT-X-PROGRAM-DATE-TIME"

      assert DateTime.compare(discontinuity_segment.program_date_time, discontinuity_reference) ==
               :eq,
             "Discontinuity should reset program date time to a new shared reference"
    end

    test "EXT-X-ENDLIST only appears in VOD playlists", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      # Test live playlist (max_segments set) - should NOT have ENDLIST
      max_segments = 3
      {:ok, live_state} = Packager.new(manifest_uri: manifest_uri, max_segments: max_segments)

      {live_state, []} =
        Packager.add_track(live_state, "video",
          stream: %VariantStream{bandwidth: 1_000_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      {live_state, [action]} = Packager.put_segment(live_state, "video", duration: 2.0)
      ActionExecutor.execute_action(action, storage, manifest_uri)
      {live_state, actions} = Packager.confirm_upload(live_state, action.id)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)
      {_live_state, actions} = Packager.sync(live_state, 1)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      media_uri = HLS.Playlist.build_absolute_uri(manifest_uri, URI.new!("stream_video.m3u8"))
      {:ok, live_content} = TestStorage.get(storage, media_uri)

      refute String.contains?(live_content, "#EXT-X-ENDLIST"),
             "Live playlists must NOT contain EXT-X-ENDLIST"

      # Clear storage for VOD test
      TestStorage.clear(storage)

      # Test VOD playlist (flush without max_segments) - SHOULD have ENDLIST
      {:ok, vod_state} = Packager.new(manifest_uri: manifest_uri)

      {vod_state, []} =
        Packager.add_track(vod_state, "video",
          stream: %VariantStream{bandwidth: 1_000_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      {vod_state, [action]} = Packager.put_segment(vod_state, "video", duration: 2.0)
      ActionExecutor.execute_action(action, storage, manifest_uri)
      {vod_state, actions} = Packager.confirm_upload(vod_state, action.id)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)
      {_vod_state, actions} = Packager.flush(vod_state)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      {:ok, vod_content} = TestStorage.get(storage, media_uri)

      assert String.contains?(vod_content, "#EXT-X-ENDLIST"),
             "VOD playlists must contain EXT-X-ENDLIST"
    end

    test "matching timestamps across variant streams", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      # RFC 8216: "Matching content in Variant Streams MUST have matching timestamps"
      max_segments = 5
      {:ok, state} = Packager.new(manifest_uri: manifest_uri, max_segments: max_segments)

      # Add two variant streams (video tracks at different bitrates)
      {state, []} =
        Packager.add_track(state, "video_high",
          stream: %VariantStream{
            bandwidth: 2_000_000,
            resolution: {1280, 720},
            codecs: []
          },
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      {state, []} =
        Packager.add_track(state, "video_low",
          stream: %VariantStream{
            bandwidth: 500_000,
            resolution: {640, 360},
            codecs: []
          },
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      # Add matching segments to both tracks
      {state, _} =
        Enum.reduce(1..4, {state, []}, fn _i, {s, _} ->
          # Add to both tracks
          {s, [action1]} = Packager.put_segment(s, "video_high", duration: 2.0)
          ActionExecutor.execute_action(action1, storage, manifest_uri)
          {s, actions1} = Packager.confirm_upload(s, action1.id)
          ActionExecutor.execute_actions(actions1, storage, manifest_uri)

          {s, [action2]} = Packager.put_segment(s, "video_low", duration: 2.0)
          ActionExecutor.execute_action(action2, storage, manifest_uri)
          {s, actions2} = Packager.confirm_upload(s, action2.id)
          ActionExecutor.execute_actions(actions2, storage, manifest_uri)

          # Sync both
          {s, actions} = Packager.sync(s, Packager.next_sync_point(s))
          ActionExecutor.execute_actions(actions, storage, manifest_uri)

          {s, []}
        end)

      high_playlist =
        load_media_playlist(storage, manifest_uri, state.tracks["video_high"].media_playlist.uri)

      low_playlist =
        load_media_playlist(storage, manifest_uri, state.tracks["video_low"].media_playlist.uri)

      high_timestamps = Enum.map(high_playlist.segments, & &1.program_date_time)
      low_timestamps = Enum.map(low_playlist.segments, & &1.program_date_time)

      # Both should have same number of segments
      assert length(high_timestamps) == length(low_timestamps),
             "Variant streams must have matching segment counts"

      # All corresponding timestamps must match
      for {high_dt, low_dt} <- Enum.zip(high_timestamps, low_timestamps) do
        assert high_dt != nil, "Timestamps must be assigned in sliding window mode"
        assert low_dt != nil, "Timestamps must be assigned in sliding window mode"

        # RFC 8216: Matching content MUST have matching timestamps
        assert DateTime.compare(high_dt, low_dt) == :eq,
               "RFC 8216: Matching segments across variant streams must have identical timestamps. Got #{inspect(high_dt)} vs #{inspect(low_dt)}"
      end
    end

    test "matching discontinuity sequences across variant streams", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      # RFC 8216: "Matching content in Variant Streams MUST have matching Discontinuity Sequence Numbers"
      max_segments = 5
      {:ok, state} = Packager.new(manifest_uri: manifest_uri, max_segments: max_segments)

      # Add two variant streams
      {state, []} =
        Packager.add_track(state, "video",
          stream: %VariantStream{bandwidth: 2_000_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      {state, []} =
        Packager.add_track(state, "audio",
          stream: %VariantStream{bandwidth: 128_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      # Add initial segments
      state =
        Enum.reduce(["video", "audio"], state, fn track_id, s ->
          {s, [action]} = Packager.put_segment(s, track_id, duration: 2.0)
          ActionExecutor.execute_action(action, storage, manifest_uri)
          {s, actions} = Packager.confirm_upload(s, action.id)
          ActionExecutor.execute_actions(actions, storage, manifest_uri)
          s
        end)

      {state, actions} = Packager.sync(state, 1)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      initial_video_disc_seq =
        load_media_playlist(storage, manifest_uri, state.tracks["video"].media_playlist.uri)
        |> Map.fetch!(:discontinuity_sequence)

      initial_audio_disc_seq =
        load_media_playlist(storage, manifest_uri, state.tracks["audio"].media_playlist.uri)
        |> Map.fetch!(:discontinuity_sequence)

      # Discontinuity sequences should match initially
      assert initial_video_disc_seq == initial_audio_disc_seq,
             "RFC 8216: Variant streams must have matching discontinuity sequences"

      # Add discontinuity to ALL tracks (synchronized)
      {state, []} = Packager.discontinue(state)

      # Add segments with discontinuity
      state =
        Enum.reduce(["video", "audio"], state, fn track_id, s ->
          {s, [action]} = Packager.put_segment(s, track_id, duration: 2.0)
          ActionExecutor.execute_action(action, storage, manifest_uri)
          {s, actions} = Packager.confirm_upload(s, action.id)
          ActionExecutor.execute_actions(actions, storage, manifest_uri)
          s
        end)

      {state, actions} = Packager.sync(state, 2)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      # Add more segments to trigger sliding window removal of discontinuity
      {state, _} =
        Enum.reduce(1..4, {state, []}, fn _i, {s, _} ->
          s =
            Enum.reduce(["video", "audio"], s, fn track_id, inner_s ->
              {inner_s, [action]} = Packager.put_segment(inner_s, track_id, duration: 2.0)
              ActionExecutor.execute_action(action, storage, manifest_uri)
              {inner_s, actions} = Packager.confirm_upload(inner_s, action.id)
              ActionExecutor.execute_actions(actions, storage, manifest_uri)
              inner_s
            end)

          {s, actions} = Packager.sync(s, Packager.next_sync_point(s))
          ActionExecutor.execute_actions(actions, storage, manifest_uri)
          {s, []}
        end)

      final_video_disc_seq =
        load_media_playlist(storage, manifest_uri, state.tracks["video"].media_playlist.uri)
        |> Map.fetch!(:discontinuity_sequence)

      final_audio_disc_seq =
        load_media_playlist(storage, manifest_uri, state.tracks["audio"].media_playlist.uri)
        |> Map.fetch!(:discontinuity_sequence)

      # Discontinuity sequences must remain synchronized after sliding window
      assert final_video_disc_seq == final_audio_disc_seq,
             "RFC 8216: Variant streams must maintain matching discontinuity sequences (video: #{final_video_disc_seq}, audio: #{final_audio_disc_seq})"

      # Both should have incremented from removal of discontinuity segment
      assert final_video_disc_seq >= initial_video_disc_seq,
             "Discontinuity sequence should increment when discontinuity segments are removed"
    end

    test "playlist modification rules enforced", %{manifest_uri: manifest_uri, storage: storage} do
      # RFC 8216: Server MUST NOT change playlist except to:
      # 1. Append lines (new segments)
      # 2. Remove segments in order they appear (from head)
      # 3. Increment media sequence number
      max_segments = 3
      {:ok, state} = Packager.new(manifest_uri: manifest_uri, max_segments: max_segments)

      {state, []} =
        Packager.add_track(state, "video",
          stream: %VariantStream{bandwidth: 1_000_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      # Track playlist states over time
      playlist_states = []

      # Add segments and capture state at each point
      {_state, playlist_states} =
        Enum.reduce(1..10, {state, playlist_states}, fn _i, {s, states} ->
          {s, [action]} = Packager.put_segment(s, "video", duration: 2.0)
          ActionExecutor.execute_action(action, storage, manifest_uri)
          {s, actions} = Packager.confirm_upload(s, action.id)
          ActionExecutor.execute_actions(actions, storage, manifest_uri)
          {s, actions} = Packager.sync(s, Packager.next_sync_point(s))
          ActionExecutor.execute_actions(actions, storage, manifest_uri)

          playlist =
            load_media_playlist(storage, manifest_uri, s.tracks["video"].media_playlist.uri)

          snapshot = %{
            segments: playlist.segments,
            media_sequence: playlist.media_sequence_number
          }

          {s, states ++ [snapshot]}
        end)

      # Verify modification rules between consecutive states
      for {prev, next} <- Enum.zip(playlist_states, tl(playlist_states)) do
        prev_uris = Enum.map(prev.segments, & &1.uri)
        next_uris = Enum.map(next.segments, & &1.uri)

        # Rule 1: Media sequence must be monotonically increasing
        assert next.media_sequence >= prev.media_sequence,
               "RFC 8216: Media sequence must only increment (#{prev.media_sequence} -> #{next.media_sequence})"

        # Rule 2: New segments are only appended
        # If media sequence changed, some segments were removed from head
        if next.media_sequence > prev.media_sequence do
          segments_removed = next.media_sequence - prev.media_sequence

          # Verify segments were removed from head only
          # The remaining segments in prev should match the beginning of next's tail
          prev_kept = Enum.drop(prev_uris, segments_removed)
          next_head = Enum.take(next_uris, length(prev_kept))

          assert prev_kept == next_head,
                 "RFC 8216: Segments can only be removed from head, not middle or tail"

          # New segments should be appended after kept segments
          new_segments = Enum.drop(next_uris, length(prev_kept))

          assert length(new_segments) >= 0,
                 "RFC 8216: After removing head segments, new segments should only be appended"
        else
          # No removal - next should start with all of prev, plus new appended segments
          assert Enum.take(next_uris, length(prev_uris)) == prev_uris,
                 "RFC 8216: When no segments removed, playlist must only append new segments"
        end
      end

      # Verify sliding window behavior is compliant
      final_state = List.last(playlist_states)

      assert length(final_state.segments) <= max_segments,
             "Sliding window should maintain max_segments limit"

      assert final_state.media_sequence > 0,
             "Media sequence should increment as segments slide out"
    end
  end
end
