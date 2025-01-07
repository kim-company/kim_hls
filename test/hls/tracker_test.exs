defmodule HLS.TrackerTest do
  use ExUnit.Case

  alias HLS.Tracker
  alias HLS.Segment
  alias HLS.Storage

  @media_uri URI.new!("file://test/fixtures/mpeg-ts/stream_416x234.m3u8")
  @storage Storage.File.new()

  describe "tracker process livecycle" do
    setup do
      ref = make_ref()

      pid =
        start_link_supervised!(
          {Tracker,
           [
             media_playlist_uri: @media_uri,
             storage: @storage,
             ref: ref,
             owner: self()
           ]}
        )

      %{tracker: pid, ref: ref}
    end

    test "sends one message for each segment in a static track", %{ref: ref} do
      # sequence goes from 1 to 5 as the target playlist starts with a media
      # sequence number of 1.
      Enum.each(1..5, fn seq ->
        assert_receive {:segment, ^ref, %Segment{absolute_sequence: ^seq}}, 1000
      end)

      refute_received {:segment, ^ref, _}, 1000
    end

    test "sends start of track message identifing first sequence number", %{ref: ref} do
      assert_receive {:start_of_track, ^ref, 1}, 1000
    end

    test "sends track termination message when track is finished", %{ref: ref} do
      assert_receive {:end_of_track, ^ref}, 1000
    end
  end

  describe "live playlist" do
    test "Waits for the min number of HLS segments to be available before starting on a live playlist" do
      storage = Support.ControlledReader.new(initial: 1, target_duration: 1, max: 4)
      ref = make_ref()

      _pid =
        start_link_supervised!(
          {Tracker,
           [
             media_playlist_uri: @media_uri,
             storage: storage,
             ref: ref,
             owner: self()
           ]}
        )

      refute_receive {:segment, ^ref, _}, 2000
      assert_receive {:segment, ^ref, %Segment{absolute_sequence: 1}}, 100
      assert_receive {:segment, ^ref, %Segment{absolute_sequence: 2}}, 100
      assert_receive {:segment, ^ref, %Segment{absolute_sequence: 3}}, 100
      assert_receive {:segment, ^ref, %Segment{absolute_sequence: 4}}, 2000
      assert_receive {:end_of_track, ^ref}, 2000
    end

    test "when the playlist is not finished, it does not deliver more than 3 packets at first" do
      storage = Support.ControlledReader.new(initial: 4, target_duration: 1)
      ref = make_ref()

      _pid =
        start_link_supervised!(
          {Tracker,
           [
             media_playlist_uri: @media_uri,
             storage: storage,
             ref: ref,
             owner: self()
           ]}
        )

      assert_receive {:start_of_track, ^ref, 2}, 200

      assert_receive {:segment, ^ref, %Segment{absolute_sequence: 2}}, 200
      assert_receive {:segment, ^ref, %Segment{absolute_sequence: 3}}, 200
      assert_receive {:segment, ^ref, %Segment{absolute_sequence: 4}}, 200

      assert_receive {:segment, ^ref, %Segment{absolute_sequence: 5}}, 2000

      refute_received {:segment, ^ref, _}, 2000
      assert_receive {:end_of_track, ^ref}, 2000
    end
  end
end
