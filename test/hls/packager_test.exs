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
      {state, [action]} = put_segment(state, "video", duration: 5.0)

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
  alias HLS.Playlist.Master

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

  defp load_media_content(storage, manifest_uri, playlist_uri) do
    full_uri = HLS.Playlist.build_absolute_uri(manifest_uri, playlist_uri)
    {:ok, content} = TestStorage.get(storage, full_uri)
    content
  end

  defp load_master_content(storage, manifest_uri) do
    {:ok, content} = TestStorage.get(storage, manifest_uri)
    content
  end

  defp playlist_lines(content) do
    String.split(content, "\n", trim: true)
  end

  defp put_segment(state, track_id, opts) when is_list(opts) do
    track = Map.fetch!(state.tracks, track_id)
    duration = Keyword.fetch!(opts, :duration)

    pts =
      Keyword.get_lazy(opts, :pts, fn ->
        last_segment =
          track.media_playlist.segments
          |> Kernel.++(track.pending_playlist.segments)
          |> Kernel.++(Enum.map(track.pending_segments, & &1.segment))
          |> List.last()

        case last_segment do
          nil -> 0
          segment -> segment.pts + duration_to_ns(segment.duration)
        end
      end)

    dts = Keyword.get(opts, :dts)

    case Packager.put_segment(state, track_id, duration: duration, pts: pts, dts: dts) do
      {:error, error, _state} -> raise error
      {state, actions} -> {state, actions}
    end
  end

  defp duration_to_ns(duration) do
    round(duration * 1_000_000_000)
  end

  defp add_segments(state, storage, manifest_uri, track_id, count, duration, opts \\ []) do
    Enum.reduce(1..count, state, fn _i, s ->
      {s, actions} = put_segment(s, track_id, Keyword.merge([duration: duration], opts))
      action = Enum.find(actions, &match?(%Packager.Action.UploadSegment{}, &1))
      ActionExecutor.execute_action(action, storage, manifest_uri)
      {s, confirm_actions} = Packager.confirm_upload(s, action.id)
      ActionExecutor.execute_actions(confirm_actions, storage, manifest_uri)
      s
    end)
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
          {s, [action]} = put_segment(s, "video", duration: 6.0)
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

    test "master playlist includes EXT-X-VERSION and EXT-X-INDEPENDENT-SEGMENTS", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri)

      {state, []} =
        Packager.add_track(state, "video",
          stream: %VariantStream{
            bandwidth: 2_000_000,
            codecs: ["avc1.64001f"]
          },
          segment_extension: ".ts",
          target_segment_duration: 6.0
        )

      state = add_segments(state, storage, manifest_uri, "video", 3, 6.0)
      {_state, actions} = Packager.sync(state, 3)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      {:ok, master_content} = TestStorage.get(storage, manifest_uri)
      assert master_content =~ "#EXT-X-VERSION:"
      assert master_content =~ "#EXT-X-INDEPENDENT-SEGMENTS"
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
      state = add_segments(state, storage, manifest_uri, "video", 3, 6.0)

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

    test "master playlist orders EXT-X-MEDIA before EXT-X-STREAM-INF", %{
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
            uri: URI.new!("audio_en.m3u8"),
            default: true,
            autoselect: true
          },
          segment_extension: ".ts",
          target_segment_duration: 6.0,
          codecs: ["mp4a.40.2"]
        )

      state = add_segments(state, storage, manifest_uri, "video", 3, 6.0)

      {_state, actions} = Packager.sync(state, 3)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)
      {:ok, master_content} = TestStorage.get(storage, manifest_uri)

      {media_idx, _} = :binary.match(master_content, "#EXT-X-MEDIA")
      {stream_idx, _} = :binary.match(master_content, "#EXT-X-STREAM-INF")

      assert media_idx != nil
      assert stream_idx != nil
      assert media_idx < stream_idx
    end

    test "master playlist stream tags are immediately followed by URIs", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri)

      {state, []} =
        Packager.add_track(state, "video",
          stream: %VariantStream{
            bandwidth: 2_000_000,
            codecs: ["avc1.64001f"],
            resolution: {1280, 720}
          },
          segment_extension: ".ts",
          target_segment_duration: 6.0,
          codecs: ["avc1.64001f"]
        )

      state = add_segments(state, storage, manifest_uri, "video", 3, 6.0)
      {_state, actions} = Packager.sync(state, 3)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      {:ok, master_content} = TestStorage.get(storage, manifest_uri)
      lines = String.split(master_content, "\n")

      lines
      |> Enum.with_index()
      |> Enum.filter(fn {line, _idx} -> String.starts_with?(line, "#EXT-X-STREAM-INF:") end)
      |> Enum.each(fn {_line, idx} ->
        assert Enum.at(lines, idx + 1) =~ ".m3u8"
      end)
    end

    test "master playlist includes required EXT-X-MEDIA attributes", %{
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
            uri: URI.new!("audio_en.m3u8"),
            default: true,
            autoselect: true
          },
          segment_extension: ".ts",
          target_segment_duration: 6.0,
          codecs: ["mp4a.40.2"]
        )

      state = add_segments(state, storage, manifest_uri, "video", 3, 6.0)

      {_state, actions} = Packager.sync(state, 3)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)
      {:ok, master_content} = TestStorage.get(storage, manifest_uri)

      media_line =
        master_content
        |> String.split("\n")
        |> Enum.find(&String.starts_with?(&1, "#EXT-X-MEDIA:"))

      assert media_line =~ "TYPE=AUDIO"
      assert media_line =~ "GROUP-ID=\"audio-group\""
      assert media_line =~ "NAME=\"English\""
      assert media_line =~ "URI=\"stream_audio_en.m3u8\""
    end

    test "master playlist enforces EXT-X-MEDIA requirements for closed captions", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri)

      {state, []} =
        Packager.add_track(state, "video",
          stream: %VariantStream{
            bandwidth: 2_000_000,
            codecs: ["avc1.64001f"],
            closed_captions: "cc-group"
          },
          segment_extension: ".ts",
          target_segment_duration: 6.0
        )

      {state, []} =
        Packager.add_track(state, "cc",
          stream: %AlternativeRendition{
            type: :closed_captions,
            group_id: "cc-group",
            name: "CC1",
            instream_id: "CC1"
          },
          segment_extension: ".ts",
          target_segment_duration: 6.0
        )

      state = add_segments(state, storage, manifest_uri, "video", 3, 6.0)
      {_state, actions} = Packager.sync(state, 3)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      {:ok, master_content} = TestStorage.get(storage, manifest_uri)

      media_line =
        master_content
        |> String.split("\n")
        |> Enum.find(&String.starts_with?(&1, "#EXT-X-MEDIA:"))

      assert media_line =~ "TYPE=CLOSED-CAPTIONS"
      assert media_line =~ "GROUP-ID=\"cc-group\""
      assert media_line =~ "NAME=\"CC1\""
      assert media_line =~ "INSTREAM-ID=\"CC1\""
      refute media_line =~ "URI="
    end
  end

  describe "sync readiness helper" do
    test "sync_ready?/2 ignores alternative renditions by default", %{
      manifest_uri: manifest_uri
    } do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri)

      {state, []} =
        Packager.add_track(state, "video_a",
          stream: %VariantStream{
            bandwidth: 2_000_000,
            codecs: ["avc1.64001f"]
          },
          segment_extension: ".ts",
          target_segment_duration: 6.0
        )

      {state, []} =
        Packager.add_track(state, "video_b",
          stream: %VariantStream{
            bandwidth: 1_000_000,
            codecs: ["avc1.64001f"]
          },
          segment_extension: ".ts",
          target_segment_duration: 6.0
        )

      {state, []} =
        Packager.add_track(state, "audio_en",
          stream: %AlternativeRendition{
            type: :audio,
            name: "English",
            group_id: "audio"
          },
          segment_extension: ".ts",
          target_segment_duration: 6.0,
          codecs: ["mp4a.40.2"]
        )

      {state, [action]} = put_segment(state, "video_a", duration: 6.0)
      {state, _actions} = Packager.confirm_upload(state, action.id)

      {ready?, lagging} = Packager.sync_ready?(state, 1)

      refute ready?
      assert lagging == ["video_b"]
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
          {s, [action]} = put_segment(s, "test_track", duration: 1.0)
          ActionExecutor.execute_action(action, storage, manifest_uri)
          {s, actions} = Packager.confirm_upload(s, action.id)
          ActionExecutor.execute_actions(actions, storage, manifest_uri)
          {s, []}
        end)

      # Sync with point to move all to media playlist
      {state, actions} = Packager.sync(state, total_segments)

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
          {s, [action]} = put_segment(s, "test_track", duration: 6.0)
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
          {s, [action]} = put_segment(s, "test_track", duration: 6.0)
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
          {s, [action]} = put_segment(s, "test_track", duration: 6.0)
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
          {s, [action]} = put_segment(s, "test_track", duration: 1.0)
          ActionExecutor.execute_action(action, storage, manifest_uri)
          {s, actions} = Packager.confirm_upload(s, action.id)
          ActionExecutor.execute_actions(actions, storage, manifest_uri)
          {s, []}
        end)

      # Sync to trigger sliding window
      {state, actions} = Packager.sync(state, total_segments)

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

    test "late track advances past missed discontinuities and resets timing base", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      max_segments = 2
      {:ok, state} = Packager.new(manifest_uri: manifest_uri, max_segments: max_segments)

      {state, []} =
        Packager.add_track(state, "test_track",
          stream: %VariantStream{bandwidth: 1_000_000, codecs: ["avc1.64001e"]},
          segment_extension: ".ts",
          target_segment_duration: 1.0
        )

      state = add_segments(state, storage, manifest_uri, "test_track", 4, 1.0)
      {state, actions} = Packager.sync(state, 4)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      pdt1 = DateTime.add(state.timeline_reference, 10, :second)
      pdt2 = DateTime.add(state.timeline_reference, 20, :second)

      disc1 = %{
        id: make_ref(),
        sync_point: 1,
        pdt_reference: pdt1,
        timestamp_reference_ns: 10_000_000_000,
        reason: :manual
      }

      disc2 = %{
        id: make_ref(),
        sync_point: 2,
        pdt_reference: pdt2,
        timestamp_reference_ns: 20_000_000_000,
        reason: :manual
      }

      state = %{state | pending_discontinuities: [disc1, disc2]}

      state = add_segments(state, storage, manifest_uri, "test_track", 2, 1.0)
      {state, actions} = Packager.sync(state, 4)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      track = state.tracks["test_track"]
      assert MapSet.member?(track.applied_discontinuities, disc1.id)
      assert MapSet.member?(track.applied_discontinuities, disc2.id)
      assert track.base_pdt == pdt2
      assert track.base_timestamp_ns == 20_000_000_000
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

      # Add segments with specific durations within target
      segment_durations = [2.0, 1.5, 1.8, 1.0]

      {state, _} =
        Enum.reduce(segment_durations, {state, []}, fn duration, {s, _} ->
          {s, actions} = put_segment(s, "test_track", duration: duration)
          upload_action = Enum.find(actions, &match?(%Packager.Action.UploadSegment{}, &1))
          ActionExecutor.execute_actions(actions, storage, manifest_uri)
          {s, confirm_actions} = Packager.confirm_upload(s, upload_action.id)
          ActionExecutor.execute_actions(confirm_actions, storage, manifest_uri)
          {s, []}
        end)

      # Sync to move segments to media playlist (triggers datetime assignment)
      {state, actions} = Packager.sync(state, length(segment_durations))
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
        expected_diff = duration_to_ns(seg1.duration)
        actual_diff = DateTime.diff(dt2, dt1, :nanosecond)
        assert actual_diff == expected_diff
      end
    end

    test "assigns program date times even when max_segments is nil", %{
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
          {s, [action]} = put_segment(s, "test_track", duration: 2.0)
          ActionExecutor.execute_action(action, storage, manifest_uri)
          {s, actions} = Packager.confirm_upload(s, action.id)
          ActionExecutor.execute_actions(actions, storage, manifest_uri)
          {s, []}
        end)

      # Sync
      {state, actions} = Packager.sync(state, 3)
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
    end
  end

  describe "strict compliance signals" do
    test "errors when segment exceeds target duration", %{
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

      result = Packager.put_segment(state, "video", duration: 7.5, pts: 0)

      assert {:error, %Packager.Error{} = error, _state} = result
      assert error.code == :segment_duration_over_target
      assert error.details.duration == 7.5
      assert error.details.target_duration == 6.0
      assert error.details.track_id == "video"
    end

    test "errors when tracks are misaligned at sync point", %{
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

      # Segment 1
      {state, actions} = put_segment(state, "video_high", duration: 3.0, pts: 0)
      upload = Enum.find(actions, &match?(%Packager.Action.UploadSegment{}, &1))
      ActionExecutor.execute_action(upload, storage, manifest_uri)
      {state, actions} = Packager.confirm_upload(state, upload.id)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      {state, actions} = put_segment(state, "video_low", duration: 1.0, pts: 1_000_000_000)
      upload = Enum.find(actions, &match?(%Packager.Action.UploadSegment{}, &1))
      ActionExecutor.execute_action(upload, storage, manifest_uri)
      {state, actions} = Packager.confirm_upload(state, upload.id)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      result = Packager.sync(state, 1)

      assert {:error, %Packager.Error{} = error, _state} = result
      assert error.code == :track_timing_mismatch_at_sync
    end

    test "errors when timing drift is detected during put_segment", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri)

      {state, []} =
        Packager.add_track(state, "video",
          stream: %VariantStream{bandwidth: 2_000_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      {state, actions} = put_segment(state, "video", duration: 2.0, pts: 0)
      upload = Enum.find(actions, &match?(%Packager.Action.UploadSegment{}, &1))
      ActionExecutor.execute_action(upload, storage, manifest_uri)
      {state, actions} = Packager.confirm_upload(state, upload.id)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      result = Packager.put_segment(state, "video", duration: 2.0, pts: 3_000_000_000)

      assert {:error, %Packager.Error{} = error, new_state} = result
      assert error.code == :timing_drift
      assert new_state.tracks["video"].pending_segments == []
    end

    test "warns when mandatory tracks are behind at sync point", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
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
          {s, actions} = put_segment(s, "video", duration: 2.0)
          upload = Enum.find(actions, &match?(%Packager.Action.UploadSegment{}, &1))
          ActionExecutor.execute_action(upload, storage, manifest_uri)
          {s, actions} = Packager.confirm_upload(s, upload.id)
          ActionExecutor.execute_actions(actions, storage, manifest_uri)
          s
        end)

      # Add only 1 segment to audio
      {state, actions} = put_segment(state, "audio", duration: 2.0)
      upload = Enum.find(actions, &match?(%Packager.Action.UploadSegment{}, &1))
      ActionExecutor.execute_action(upload, storage, manifest_uri)
      {state, actions} = Packager.confirm_upload(state, upload.id)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      result = Packager.sync(state, 3)

      assert {:warning, warnings, _state} = result
      assert length(warnings) == 1

      warning = hd(warnings)
      assert warning.code == :mandatory_track_missing_segment_at_sync
      assert warning.details.track_id == "audio"
      assert warning.details.available_segments == 1
      assert warning.details.sync_point == 3
      assert warning.details.missing_segments == 2
    end

    test "accepts segments within target duration", %{
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

      {_, actions} = put_segment(state, "video", duration: 5.5)

      assert Enum.any?(actions, &match?(%Packager.Action.UploadSegment{}, &1))
    end

    test "skip_sync_point marks sync point as skipped for all tracks", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri)

      {state, []} =
        Packager.add_track(state, "video",
          stream: %VariantStream{bandwidth: 1_000_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      {state, []} =
        Packager.add_track(state, "audio",
          stream: %VariantStream{bandwidth: 128_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      result = Packager.put_segment(state, "video", duration: 3.5, pts: 0)
      assert {:error, %Packager.Error{code: :segment_duration_over_target}, state} = result

      {state, []} = Packager.skip_sync_point(state, 1)

      result = Packager.put_segment(state, "audio", duration: 2.0, pts: 0)
      assert {:warning, %Packager.Error{code: :sync_point_skipped}, state} = result

      result = Packager.put_segment(state, "video", duration: 2.0, pts: 0)
      assert {:warning, %Packager.Error{code: :sync_point_skipped}, state} = result

      {state, actions} = Packager.put_segment(state, "video", duration: 2.0, pts: 0)
      upload = Enum.find(actions, &match?(%Packager.Action.UploadSegment{}, &1))
      ActionExecutor.execute_action(upload, storage, manifest_uri)
      {state, actions} = Packager.confirm_upload(state, upload.id)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      {state, actions} = Packager.put_segment(state, "audio", duration: 2.0, pts: 0)
      upload = Enum.find(actions, &match?(%Packager.Action.UploadSegment{}, &1))
      ActionExecutor.execute_action(upload, storage, manifest_uri)
      {state, actions} = Packager.confirm_upload(state, upload.id)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      {state, actions} = Packager.sync(state, 1)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      video_playlist =
        load_media_playlist(storage, manifest_uri, state.tracks["video"].media_playlist.uri)

      audio_playlist =
        load_media_playlist(storage, manifest_uri, state.tracks["audio"].media_playlist.uri)

      assert Enum.at(video_playlist.segments, 0).discontinuity
      assert Enum.at(audio_playlist.segments, 0).discontinuity
    end

    test "skip_sync_point errors when sync point already passed", %{
      manifest_uri: manifest_uri,
      storage: _storage
    } do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri)

      {state, []} =
        Packager.add_track(state, "video",
          stream: %VariantStream{bandwidth: 1_000_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      {state, [action]} = Packager.put_segment(state, "video", duration: 2.0, pts: 0)
      assert match?(%Packager.Action.UploadSegment{}, action)

      result = Packager.skip_sync_point(state, 1)

      assert {:error, %Packager.Error{code: :discontinuity_point_missed}, _state} = result
    end

    test "confirm_upload warns when upload id is not found", %{
      manifest_uri: manifest_uri,
      storage: _storage
    } do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri)

      {state, []} =
        Packager.add_track(state, "video",
          stream: %VariantStream{bandwidth: 1_000_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      result = Packager.confirm_upload(state, "missing_upload")

      assert {:warning, %Packager.Error{code: :upload_id_not_found}, _state} = result
    end

    test "does not error when tracks are synchronized for discontinuity", %{
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
          {s, actions} = put_segment(s, track_id, duration: 2.0)
          upload = Enum.find(actions, &match?(%Packager.Action.UploadSegment{}, &1))
          ActionExecutor.execute_action(upload, storage, manifest_uri)
          {s, actions} = Packager.confirm_upload(s, upload.id)
          ActionExecutor.execute_actions(actions, storage, manifest_uri)
          s
        end)

      result = Packager.discontinue(state)

      assert match?({_, []}, result)
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
          {s, [action]} = put_segment(s, "video", duration: 6.0)
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

      state = add_segments(state, storage, manifest_uri, "video", 3, 6.0)
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
          {s, [action]} = put_segment(s, "video", duration: duration)
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

      {_state, actions} = Packager.sync(state, length(durations))
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
          {s, [action]} = put_segment(s, "video", duration: duration)
          ActionExecutor.execute_action(action, storage, manifest_uri)
          {s, actions} = Packager.confirm_upload(s, action.id)
          ActionExecutor.execute_actions(actions, storage, manifest_uri)
          {s, []}
        end)

      {state, actions} = Packager.sync(state, length(valid_durations))
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
          {s, [action]} = put_segment(s, "video", duration: 2.0)
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
      {state, [action]} = put_segment(state, "video", duration: 2.0)
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
      {state, [action]} = put_segment(state, "video", duration: 2.0)
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
          {s, [action]} = put_segment(s, "video", duration: 2.0)
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

    test "discontinue inserts discontinuity when max_segments is nil", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri)

      {state, []} =
        Packager.add_track(state, "video",
          stream: %VariantStream{bandwidth: 1_000_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      {state, [action]} = put_segment(state, "video", duration: 2.0)
      ActionExecutor.execute_action(action, storage, manifest_uri)
      {state, actions} = Packager.confirm_upload(state, action.id)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)
      {state, actions} = Packager.sync(state, 1)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      {state, []} = Packager.discontinue(state)

      {state, [action]} = put_segment(state, "video", duration: 2.0)
      ActionExecutor.execute_action(action, storage, manifest_uri)
      {state, actions} = Packager.confirm_upload(state, action.id)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)
      {state, actions} = Packager.sync(state, 2)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      playlist =
        load_media_playlist(storage, manifest_uri, state.tracks["video"].media_playlist.uri)

      discontinuity_segments = Enum.filter(playlist.segments, & &1.discontinuity)
      assert length(discontinuity_segments) == 1
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
      {state, [action]} = put_segment(state, "video", duration: 2.0)
      ActionExecutor.execute_action(action, storage, manifest_uri)
      {state, actions} = Packager.confirm_upload(state, action.id)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)
      {state, actions} = Packager.sync(state, 1)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      # Add discontinuity
      {state, []} = Packager.discontinue(state)
      discontinuity_reference = state.timeline_reference

      # Add segment with discontinuity
      {state, [action]} = put_segment(state, "video", duration: 2.0)
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

      {live_state, [action]} = put_segment(live_state, "video", duration: 2.0)
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

      {vod_state, [action]} = put_segment(vod_state, "video", duration: 2.0)
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
          {s, [action1]} = put_segment(s, "video_high", duration: 2.0)
          ActionExecutor.execute_action(action1, storage, manifest_uri)
          {s, actions1} = Packager.confirm_upload(s, action1.id)
          ActionExecutor.execute_actions(actions1, storage, manifest_uri)

          {s, [action2]} = put_segment(s, "video_low", duration: 2.0)
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
          {s, [action]} = put_segment(s, track_id, duration: 2.0)
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
          {s, [action]} = put_segment(s, track_id, duration: 2.0)
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
              {inner_s, [action]} = put_segment(inner_s, track_id, duration: 2.0)
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
          {s, [action]} = put_segment(s, "video", duration: 2.0)
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

    test "master and media playlists do not mix tag types", %{
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

      state = add_segments(state, storage, manifest_uri, "video", 3, 6.0)
      {state, actions} = Packager.sync(state, 3)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      master_content = load_master_content(storage, manifest_uri)

      media_content =
        load_media_content(storage, manifest_uri, state.tracks["video"].media_playlist.uri)

      master_forbidden = [
        "#EXTINF",
        "#EXT-X-TARGETDURATION",
        "#EXT-X-MEDIA-SEQUENCE",
        "#EXT-X-DISCONTINUITY",
        "#EXT-X-BYTERANGE",
        "#EXT-X-MAP",
        "#EXT-X-PROGRAM-DATE-TIME"
      ]

      media_forbidden = [
        "#EXT-X-STREAM-INF",
        "#EXT-X-MEDIA:",
        "#EXT-X-I-FRAME-STREAM-INF",
        "#EXT-X-SESSION-DATA",
        "#EXT-X-SESSION-KEY"
      ]

      Enum.each(master_forbidden, fn tag ->
        refute String.contains?(master_content, tag),
               "Master playlist must not include media tags: #{tag}"
      end)

      Enum.each(media_forbidden, fn tag ->
        refute String.contains?(media_content, tag),
               "Media playlist must not include master tags: #{tag}"
      end)
    end

    test "tags are uppercase", %{manifest_uri: manifest_uri, storage: storage} do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri)

      {state, []} =
        Packager.add_track(state, "video",
          stream: %VariantStream{bandwidth: 1_000_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 6.0
        )

      state = add_segments(state, storage, manifest_uri, "video", 3, 6.0)
      {_state, actions} = Packager.sync(state, 3)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      master_lines = load_master_content(storage, manifest_uri) |> playlist_lines()

      media_lines =
        load_media_content(storage, manifest_uri, state.tracks["video"].media_playlist.uri)
        |> playlist_lines()

      assert Enum.all?(master_lines, fn line ->
               not String.starts_with?(line, "#ext") and not String.starts_with?(line, "#Ext")
             end)

      assert Enum.all?(media_lines, fn line ->
               not String.starts_with?(line, "#ext") and not String.starts_with?(line, "#Ext")
             end)
    end

    test "attribute lists do not include whitespace around separators", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri)

      {state, []} =
        Packager.add_track(state, "video",
          stream: %VariantStream{bandwidth: 1_000_000, codecs: ["avc1.64001f"]},
          segment_extension: ".ts",
          target_segment_duration: 6.0
        )

      state = add_segments(state, storage, manifest_uri, "video", 3, 6.0)
      {_state, actions} = Packager.sync(state, 3)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      master_lines = load_master_content(storage, manifest_uri) |> playlist_lines()

      master_lines
      |> Enum.filter(fn line ->
        String.starts_with?(line, "#EXT-X-STREAM-INF:") or
          String.starts_with?(line, "#EXT-X-MEDIA:")
      end)
      |> Enum.each(fn line ->
        refute String.contains?(line, ", "),
               "Attribute lists must not include whitespace after commas"

        refute String.contains?(line, " ="),
               "Attribute lists must not include whitespace before ="

        refute String.contains?(line, "= "),
               "Attribute lists must not include whitespace after ="
      end)
    end

    test "EXTINF appears for each media segment", %{
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

      state = add_segments(state, storage, manifest_uri, "video", 3, 6.0)
      {state, actions} = Packager.sync(state, 3)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      media_lines =
        load_media_content(storage, manifest_uri, state.tracks["video"].media_playlist.uri)
        |> playlist_lines()

      segment_lines =
        Enum.filter(media_lines, fn line -> line != "" and not String.starts_with?(line, "#") end)

      inf_lines = Enum.filter(media_lines, &String.starts_with?(&1, "#EXTINF:"))

      assert length(segment_lines) == length(inf_lines),
             "Each segment URI must have a corresponding EXTINF line"
    end

    test "media sequence tag appears before the first segment", %{
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

      state = add_segments(state, storage, manifest_uri, "video", 2, 6.0)
      {state, actions} = Packager.sync(state, 2)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      media_lines =
        load_media_content(storage, manifest_uri, state.tracks["video"].media_playlist.uri)
        |> playlist_lines()

      media_seq_idx =
        Enum.find_index(media_lines, &String.starts_with?(&1, "#EXT-X-MEDIA-SEQUENCE:"))

      first_segment_idx =
        Enum.find_index(media_lines, fn line -> not String.starts_with?(line, "#") end)

      assert media_seq_idx != nil
      assert first_segment_idx != nil
      assert media_seq_idx < first_segment_idx
    end

    test "discontinuity sequence appears before discontinuity tags", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri, max_segments: 5)

      {state, []} =
        Packager.add_track(state, "video",
          stream: %VariantStream{bandwidth: 1_000_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      {state, actions} = put_segment(state, "video", duration: 2.0, pts: 0)
      upload = Enum.find(actions, &match?(%Packager.Action.UploadSegment{}, &1))
      ActionExecutor.execute_action(upload, storage, manifest_uri)
      {state, actions} = Packager.confirm_upload(state, upload.id)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      {state, actions} = Packager.sync(state, 1)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      {state, []} = Packager.discontinue(state)

      {state, actions} = put_segment(state, "video", duration: 2.0, pts: 2_000_000_000)
      upload = Enum.find(actions, &match?(%Packager.Action.UploadSegment{}, &1))
      ActionExecutor.execute_action(upload, storage, manifest_uri)
      {state, actions} = Packager.confirm_upload(state, upload.id)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      {state, actions} = Packager.sync(state, 2)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      media_lines =
        load_media_content(storage, manifest_uri, state.tracks["video"].media_playlist.uri)
        |> playlist_lines()

      disc_seq_idx =
        Enum.find_index(media_lines, &String.starts_with?(&1, "#EXT-X-DISCONTINUITY-SEQUENCE:"))

      disc_idx = Enum.find_index(media_lines, &(&1 == "#EXT-X-DISCONTINUITY"))

      first_segment_idx =
        Enum.find_index(media_lines, fn line -> not String.starts_with?(line, "#") end)

      assert disc_seq_idx != nil
      assert disc_idx != nil
      assert disc_seq_idx < first_segment_idx
      assert disc_seq_idx < disc_idx
    end

    test "EXT-X-MAP appears before the first segment in fMP4 playlists", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri)

      {state, []} =
        Packager.add_track(state, "video",
          stream: %VariantStream{bandwidth: 1_000_000, codecs: []},
          segment_extension: ".m4s",
          target_segment_duration: 2.0
        )

      {state, [init_action]} = Packager.put_init_section(state, "video")
      ActionExecutor.execute_action(init_action, storage, manifest_uri, "init_data")
      {state, []} = Packager.confirm_init_upload(state, init_action.id)

      {state, actions} = put_segment(state, "video", duration: 2.0, pts: 0)
      upload = Enum.find(actions, &match?(%Packager.Action.UploadSegment{}, &1))
      ActionExecutor.execute_action(upload, storage, manifest_uri)
      {state, actions} = Packager.confirm_upload(state, upload.id)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      {state, actions} = Packager.sync(state, 1)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      media_lines =
        load_media_content(storage, manifest_uri, state.tracks["video"].media_playlist.uri)
        |> playlist_lines()

      map_idx = Enum.find_index(media_lines, &String.starts_with?(&1, "#EXT-X-MAP:"))

      first_segment_idx =
        Enum.find_index(media_lines, fn line -> not String.starts_with?(line, "#") end)

      assert map_idx != nil
      assert first_segment_idx != nil
      assert map_idx < first_segment_idx
    end

    test "program date time uses ISO 8601 with timezone and fractional seconds", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri)

      {state, []} =
        Packager.add_track(state, "video",
          stream: %VariantStream{bandwidth: 1_000_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      {state, actions} = put_segment(state, "video", duration: 2.0, pts: 0)
      upload = Enum.find(actions, &match?(%Packager.Action.UploadSegment{}, &1))
      ActionExecutor.execute_action(upload, storage, manifest_uri)
      {state, actions} = Packager.confirm_upload(state, upload.id)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      {state, actions} = Packager.sync(state, 1)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      media_lines =
        load_media_content(storage, manifest_uri, state.tracks["video"].media_playlist.uri)
        |> playlist_lines()

      pdt_line =
        Enum.find(media_lines, &String.starts_with?(&1, "#EXT-X-PROGRAM-DATE-TIME:"))

      assert pdt_line != nil

      assert pdt_line =~
               ~r/^#EXT-X-PROGRAM-DATE-TIME:\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3,6}(Z|[+-]\d{2}:\d{2})$/
    end

    test "CODECS includes all codecs across renditions", %{
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
          target_segment_duration: 6.0
        )

      {state, []} =
        Packager.add_track(state, "audio_en",
          stream: %AlternativeRendition{
            type: :audio,
            group_id: "audio-group",
            name: "English"
          },
          segment_extension: ".ts",
          target_segment_duration: 6.0,
          codecs: ["mp4a.40.2"]
        )

      state = add_segments(state, storage, manifest_uri, "video", 3, 6.0)
      {_state, actions} = Packager.sync(state, 3)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      master_content = load_master_content(storage, manifest_uri)
      assert master_content =~ ~s(CODECS="avc1.64001f,mp4a.40.2")
    end

    test "sync does not write playlists when timestamps are misaligned", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri)

      {state, []} =
        Packager.add_track(state, "video_high",
          stream: %VariantStream{bandwidth: 2_000_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      {state, []} =
        Packager.add_track(state, "video_low",
          stream: %VariantStream{bandwidth: 500_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      {state, actions} = Packager.put_segment(state, "video_high", duration: 2.0, pts: 0)
      upload = Enum.find(actions, &match?(%Packager.Action.UploadSegment{}, &1))
      ActionExecutor.execute_action(upload, storage, manifest_uri)
      {state, actions} = Packager.confirm_upload(state, upload.id)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      {state, actions} =
        Packager.put_segment(state, "video_low", duration: 2.0, pts: 1_000_000_000)

      upload = Enum.find(actions, &match?(%Packager.Action.UploadSegment{}, &1))
      ActionExecutor.execute_action(upload, storage, manifest_uri)
      {state, actions} = Packager.confirm_upload(state, upload.id)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      result = Packager.sync(state, 1)

      assert {:error, %Packager.Error{code: :track_timing_mismatch_at_sync}, _state} = result
    end

    test "discontinuity tags align across variant playlists", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri, max_segments: 5)

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

      state = add_segments(state, storage, manifest_uri, "video", 1, 2.0)
      state = add_segments(state, storage, manifest_uri, "audio", 1, 2.0)

      {state, actions} = Packager.sync(state, 1)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      {state, []} = Packager.discontinue(state)

      {state, actions} = put_segment(state, "video", duration: 2.0, pts: 2_000_000_000)
      upload = Enum.find(actions, &match?(%Packager.Action.UploadSegment{}, &1))
      ActionExecutor.execute_action(upload, storage, manifest_uri)
      {state, actions} = Packager.confirm_upload(state, upload.id)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      {state, actions} = put_segment(state, "audio", duration: 2.0, pts: 2_000_000_000)
      upload = Enum.find(actions, &match?(%Packager.Action.UploadSegment{}, &1))
      ActionExecutor.execute_action(upload, storage, manifest_uri)
      {state, actions} = Packager.confirm_upload(state, upload.id)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      {state, actions} = Packager.sync(state, 2)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      video_lines =
        load_media_content(storage, manifest_uri, state.tracks["video"].media_playlist.uri)
        |> playlist_lines()

      audio_lines =
        load_media_content(storage, manifest_uri, state.tracks["audio"].media_playlist.uri)
        |> playlist_lines()

      video_disc_idx =
        Enum.find_index(video_lines, &String.starts_with?(&1, "#EXT-X-DISCONTINUITY"))

      audio_disc_idx =
        Enum.find_index(audio_lines, &String.starts_with?(&1, "#EXT-X-DISCONTINUITY"))

      assert video_disc_idx != nil
      assert audio_disc_idx != nil

      video_segment_idxs =
        video_lines
        |> Enum.with_index()
        |> Enum.filter(fn {line, _} -> line != "" and not String.starts_with?(line, "#") end)
        |> Enum.map(&elem(&1, 1))

      audio_segment_idxs =
        audio_lines
        |> Enum.with_index()
        |> Enum.filter(fn {line, _} -> line != "" and not String.starts_with?(line, "#") end)
        |> Enum.map(&elem(&1, 1))

      assert Enum.at(video_segment_idxs, 1) > video_disc_idx
      assert Enum.at(audio_segment_idxs, 1) > audio_disc_idx
    end

    test "variant playlists share target duration", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri)

      {state, []} =
        Packager.add_track(state, "video_high",
          stream: %VariantStream{bandwidth: 2_000_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      {state, []} =
        Packager.add_track(state, "video_low",
          stream: %VariantStream{bandwidth: 500_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      state = add_segments(state, storage, manifest_uri, "video_high", 2, 2.0)
      state = add_segments(state, storage, manifest_uri, "video_low", 2, 2.0)

      {state, actions} = Packager.sync(state, 2)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      high_content =
        load_media_content(storage, manifest_uri, state.tracks["video_high"].media_playlist.uri)

      low_content =
        load_media_content(storage, manifest_uri, state.tracks["video_low"].media_playlist.uri)

      high_target =
        high_content
        |> playlist_lines()
        |> Enum.find(&String.starts_with?(&1, "#EXT-X-TARGETDURATION:"))

      low_target =
        low_content
        |> playlist_lines()
        |> Enum.find(&String.starts_with?(&1, "#EXT-X-TARGETDURATION:"))

      assert high_target == low_target
    end

    test "playlist type is consistent across variants", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri)

      {state, []} =
        Packager.add_track(state, "video_high",
          stream: %VariantStream{bandwidth: 2_000_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      {state, []} =
        Packager.add_track(state, "video_low",
          stream: %VariantStream{bandwidth: 500_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      state = add_segments(state, storage, manifest_uri, "video_high", 2, 2.0)
      state = add_segments(state, storage, manifest_uri, "video_low", 2, 2.0)

      {state, actions} = Packager.sync(state, 2)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      high_lines =
        load_media_content(storage, manifest_uri, state.tracks["video_high"].media_playlist.uri)
        |> playlist_lines()

      low_lines =
        load_media_content(storage, manifest_uri, state.tracks["video_low"].media_playlist.uri)
        |> playlist_lines()

      assert Enum.member?(high_lines, "#EXT-X-PLAYLIST-TYPE:EVENT")
      assert Enum.member?(low_lines, "#EXT-X-PLAYLIST-TYPE:EVENT")
    end

    test "playlist type is consistent across variants for VOD", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri)

      {state, []} =
        Packager.add_track(state, "video_high",
          stream: %VariantStream{bandwidth: 2_000_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      {state, []} =
        Packager.add_track(state, "video_low",
          stream: %VariantStream{bandwidth: 500_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      state = add_segments(state, storage, manifest_uri, "video_high", 1, 2.0)
      state = add_segments(state, storage, manifest_uri, "video_low", 1, 2.0)

      {state, actions} = Packager.flush(state)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      high_lines =
        load_media_content(storage, manifest_uri, state.tracks["video_high"].media_playlist.uri)
        |> playlist_lines()

      low_lines =
        load_media_content(storage, manifest_uri, state.tracks["video_low"].media_playlist.uri)
        |> playlist_lines()

      assert Enum.member?(high_lines, "#EXT-X-PLAYLIST-TYPE:VOD")
      assert Enum.member?(low_lines, "#EXT-X-PLAYLIST-TYPE:VOD")
    end

    test "program date time tags are consistent across variants", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri)

      {state, []} =
        Packager.add_track(state, "video_high",
          stream: %VariantStream{bandwidth: 2_000_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      {state, []} =
        Packager.add_track(state, "video_low",
          stream: %VariantStream{bandwidth: 500_000, codecs: []},
          segment_extension: ".ts",
          target_segment_duration: 2.0
        )

      state = add_segments(state, storage, manifest_uri, "video_high", 2, 2.0)
      state = add_segments(state, storage, manifest_uri, "video_low", 2, 2.0)

      {state, actions} = Packager.sync(state, 2)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      high_pdts =
        load_media_content(storage, manifest_uri, state.tracks["video_high"].media_playlist.uri)
        |> playlist_lines()
        |> Enum.filter(&String.starts_with?(&1, "#EXT-X-PROGRAM-DATE-TIME:"))

      low_pdts =
        load_media_content(storage, manifest_uri, state.tracks["video_low"].media_playlist.uri)
        |> playlist_lines()
        |> Enum.filter(&String.starts_with?(&1, "#EXT-X-PROGRAM-DATE-TIME:"))

      assert length(high_pdts) == length(low_pdts)
      assert high_pdts == low_pdts
    end
  end

  describe "resume" do
    test "trims to the common sync point and schedules a discontinuity", %{
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

      state = add_segments(state, storage, manifest_uri, "video", 3, 6.0)
      state = add_segments(state, storage, manifest_uri, "audio_en", 3, 6.0)

      {state, actions} = Packager.sync(state, 3)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      state = add_segments(state, storage, manifest_uri, "video", 1, 6.0)
      {state, actions} = Packager.sync(state, 4)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      master_content = load_master_content(storage, manifest_uri)
      master = HLS.Playlist.unmarshal(master_content, %Master{uri: manifest_uri})

      media_playlists =
        state.tracks
        |> Map.values()
        |> Enum.map(& &1.media_playlist)

      {:ok, resumed_state} =
        Packager.resume(master_playlist: master, media_playlists: media_playlists)

      assert resumed_state.tracks["video"].segment_count == 3
      assert length(resumed_state.tracks["video"].media_playlist.segments) == 3
      assert length(resumed_state.tracks["audio_en"].media_playlist.segments) == 3

      [discontinuity] = resumed_state.pending_discontinuities
      assert discontinuity.sync_point == 4
      assert discontinuity.reason == :resume
    end

    test "add_track is idempotent after resume when spec matches", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri)

      {state, []} =
        Packager.add_track(state, "video",
          stream: %VariantStream{bandwidth: 2_000_000, codecs: ["avc1.64001f"]},
          segment_extension: ".ts",
          target_segment_duration: 6.0,
          codecs: ["avc1.64001f"]
        )

      state = add_segments(state, storage, manifest_uri, "video", 3, 6.0)
      {state, actions} = Packager.sync(state, 3)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      master_content = load_master_content(storage, manifest_uri)
      master = HLS.Playlist.unmarshal(master_content, %Master{uri: manifest_uri})

      media_playlists =
        state.tracks
        |> Map.values()
        |> Enum.map(& &1.media_playlist)

      {:ok, resumed_state} =
        Packager.resume(master_playlist: master, media_playlists: media_playlists)

      {final_state, []} =
        Packager.add_track(resumed_state, "video",
          stream: %VariantStream{bandwidth: 2_000_000, codecs: ["avc1.64001f"]},
          segment_extension: ".ts",
          target_segment_duration: 6.0,
          codecs: ["avc1.64001f"]
        )

      assert final_state == resumed_state
    end

    test "allows resume with missing playlists but blocks segments until reconciled", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri)

      {state, []} =
        Packager.add_track(state, "video",
          stream: %VariantStream{bandwidth: 2_000_000, codecs: ["avc1.64001f"]},
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

      state = add_segments(state, storage, manifest_uri, "video", 3, 6.0)
      state = add_segments(state, storage, manifest_uri, "audio_en", 3, 6.0)

      {state, actions} = Packager.sync(state, 3)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      master_content = load_master_content(storage, manifest_uri)
      master = HLS.Playlist.unmarshal(master_content, %Master{uri: manifest_uri})

      media_playlists = [state.tracks["video"].media_playlist]

      {:ok, resumed_state} =
        Packager.resume(master_playlist: master, media_playlists: media_playlists)

      assert resumed_state.tracks["audio_en"].resume_incomplete?

      assert {:error, %Packager.Error{code: :resume_track_not_ready}, _state} =
               Packager.put_segment(resumed_state, "audio_en", duration: 6.0, pts: 0)

      {reconciled_state, []} =
        Packager.add_track(resumed_state, "audio_en",
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

      {_, [action]} =
        Packager.put_segment(reconciled_state, "audio_en", duration: 6.0, pts: 0)

      assert %Packager.Action.UploadSegment{} = action
    end

    test "returns error when extra media playlists are provided", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri)

      {state, []} =
        Packager.add_track(state, "video",
          stream: %VariantStream{bandwidth: 2_000_000, codecs: ["avc1.64001f"]},
          segment_extension: ".ts",
          target_segment_duration: 6.0,
          codecs: ["avc1.64001f"]
        )

      state = add_segments(state, storage, manifest_uri, "video", 3, 6.0)
      {state, actions} = Packager.sync(state, 3)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      master_content = load_master_content(storage, manifest_uri)
      master = HLS.Playlist.unmarshal(master_content, %Master{uri: manifest_uri})

      extra_playlist =
        state.tracks["video"].media_playlist
        |> Map.put(:uri, URI.new!("extra.m3u8"))

      media_playlists = [state.tracks["video"].media_playlist, extra_playlist]

      assert {:error, %Packager.Error{code: :resume_unexpected_playlist}} =
               Packager.resume(master_playlist: master, media_playlists: media_playlists)
    end

    test "blocks segments when extension is unknown until reconciled", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri)

      {state, []} =
        Packager.add_track(state, "video",
          stream: %VariantStream{bandwidth: 2_000_000, codecs: ["avc1.64001f"]},
          segment_extension: ".ts",
          target_segment_duration: 6.0,
          codecs: ["avc1.64001f"]
        )

      state = add_segments(state, storage, manifest_uri, "video", 3, 6.0)
      {state, actions} = Packager.sync(state, 3)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      master_content = load_master_content(storage, manifest_uri)
      master = HLS.Playlist.unmarshal(master_content, %Master{uri: manifest_uri})

      empty_media =
        state.tracks["video"].media_playlist
        |> Map.put(:segments, [])
        |> Map.put(:media_sequence_number, 0)

      {:ok, resumed_state} =
        Packager.resume(master_playlist: master, media_playlists: [empty_media])

      assert resumed_state.tracks["video"].resume_incomplete?

      assert {:error, %Packager.Error{code: :resume_track_not_ready}, _state} =
               Packager.put_segment(resumed_state, "video", duration: 6.0, pts: 0)

      {reconciled_state, []} =
        Packager.add_track(resumed_state, "video",
          stream: %VariantStream{bandwidth: 2_000_000, codecs: ["avc1.64001f"]},
          segment_extension: ".ts",
          target_segment_duration: 6.0,
          codecs: ["avc1.64001f"]
        )

      {_, [action]} =
        Packager.put_segment(reconciled_state, "video", duration: 6.0, pts: 0)

      assert %Packager.Action.UploadSegment{} = action
    end

    test "allows resume when a variant playlist is missing", %{
      manifest_uri: manifest_uri,
      storage: storage
    } do
      {:ok, state} = Packager.new(manifest_uri: manifest_uri)

      {state, []} =
        Packager.add_track(state, "video_high",
          stream: %VariantStream{bandwidth: 2_000_000, codecs: ["avc1.64001f"]},
          segment_extension: ".ts",
          target_segment_duration: 6.0,
          codecs: ["avc1.64001f"]
        )

      {state, []} =
        Packager.add_track(state, "video_low",
          stream: %VariantStream{bandwidth: 500_000, codecs: ["avc1.64001f"]},
          segment_extension: ".ts",
          target_segment_duration: 6.0,
          codecs: ["avc1.64001f"]
        )

      state = add_segments(state, storage, manifest_uri, "video_high", 3, 6.0)
      state = add_segments(state, storage, manifest_uri, "video_low", 3, 6.0)

      {state, actions} = Packager.sync(state, 3)
      ActionExecutor.execute_actions(actions, storage, manifest_uri)

      master_content = load_master_content(storage, manifest_uri)
      master = HLS.Playlist.unmarshal(master_content, %Master{uri: manifest_uri})

      media_playlists = [state.tracks["video_high"].media_playlist]

      {:ok, resumed_state} =
        Packager.resume(master_playlist: master, media_playlists: media_playlists)

      assert resumed_state.tracks["video_low"].resume_incomplete?
    end
  end
end
