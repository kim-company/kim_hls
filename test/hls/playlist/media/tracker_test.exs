defmodule HLS.Playlist.Media.TrackerTest do
  use ExUnit.Case

  alias HLS.Playlist.Media.Tracker
  alias HLS.Segment
  alias HLS.FS.OS

  @media_uri URI.new!("./test/fixtures/mpeg-ts/stream_416x234.m3u8")

  describe "tracker process" do
    test "starts and exits on demand" do
      assert {:ok, pid} = Tracker.start_link(OS.new())
      assert Process.alive?(pid)
      assert :ok = Tracker.stop(pid)
    end

    test "sends one message for each segment in a static track" do
      {:ok, pid} = Tracker.start_link(OS.new())
      ref = Tracker.follow(pid, @media_uri)

      # sequence goes from 1 to 5 as the target playlist starts with a media
      # sequence number of 1.
      Enum.each(1..5, fn seq ->
        assert_receive {:segment, ^ref, %Segment{absolute_sequence: ^seq}}, 1000
      end)

      refute_received {:segment, ^ref, _}, 1000

      :ok = Tracker.stop(pid)
    end

    #
    test "sends start of track message identifing first sequence number" do
      {:ok, pid} = Tracker.start_link(OS.new())
      ref = Tracker.follow(pid, @media_uri)

      assert_receive {:start_of_track, ^ref, 1}, 1000

      :ok = Tracker.stop(pid)
    end

    test "sends track termination message when track is finished" do
      {:ok, pid} = Tracker.start_link(OS.new())
      ref = Tracker.follow(pid, @media_uri)

      assert_receive {:end_of_track, ^ref}, 1000

      :ok = Tracker.stop(pid)
    end

    test "keeps on sending updates when the playlist does" do
      reader = Support.ControlledReader.new(initial: 1, target_duration: 1)
      {:ok, pid} = Tracker.start_link(reader)
      ref = Tracker.follow(pid, @media_uri)

      assert_receive {:segment, ^ref, %Segment{absolute_sequence: 0}}, 200
      assert_receive {:segment, ^ref, %Segment{absolute_sequence: 1}}, 200

      # The tracker should wait `target_duration` seconds, reload the track
      # afterwards and detect that one more segment has been provied, together
      # with the termination tag.

      assert_receive {:segment, ^ref, %Segment{absolute_sequence: 2}}, 2000
      refute_received {:segment, ^ref, _}, 2000
      assert_receive {:end_of_track, ^ref}, 2000

      :ok = Tracker.stop(pid)
    end

    test "when the playlist is not finished, it does not deliver more than 3 packets at first" do
      reader = Support.ControlledReader.new(initial: 4, target_duration: 1)
      {:ok, pid} = Tracker.start_link(reader)
      ref = Tracker.follow(pid, @media_uri)

      assert_receive {:start_of_track, ^ref, 2}, 200

      assert_receive {:segment, ^ref, %Segment{absolute_sequence: 2}}, 200
      assert_receive {:segment, ^ref, %Segment{absolute_sequence: 3}}, 200
      assert_receive {:segment, ^ref, %Segment{absolute_sequence: 4}}, 200

      assert_receive {:segment, ^ref, %Segment{absolute_sequence: 5}}, 2000

      refute_received {:segment, ^ref, _}, 2000
      assert_receive {:end_of_track, ^ref}, 2000

      :ok = Tracker.stop(pid)
    end

    test "when playlist adds multiple segments, they are retriven in order" do
    end
  end
end
