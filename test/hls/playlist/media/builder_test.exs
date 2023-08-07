defmodule HLS.Playlist.Media.BuilderTest do
  use ExUnit.Case

  alias HLS.Playlist.Media.Builder
  alias HLS.Playlist.Media
  alias HLS.Segment

  setup_all do
    %{
      playlist: Media.new(URI.new!("s3://bucket/media.m3u8"), 1)
    }
  end

  describe "initialization" do
    test "accepts an empty Media playlist", %{playlist: playlist} do
      assert %Builder{} = Builder.new(playlist)
    end

    test "accepts a non-empty Media playlist", %{playlist: playlist} do
      segments = [
        %Segment{uri: URI.new!("0.txt"), relative_sequence: 0},
        %Segment{uri: URI.new!("1.txt"), relative_sequence: 1}
      ]

      playlist = %Media{playlist | segments: segments}
      assert %Builder{} = Builder.new(playlist)
    end

    test "does not accept any playlist type which type is not nil, it is finished or does not contain every segment associated with it",
         %{playlist: p} do
      # That would not give it the freedom to change the playlist at will
      assert_raise(RuntimeError, fn -> Builder.new(%Media{p | finished: true}) end)

      assert_raise(RuntimeError, fn -> Builder.new(%Media{p | media_sequence_number: 3}) end)

      assert_raise(RuntimeError, fn -> Builder.new(%Media{p | type: :vod}) end)
      # We might want to accept the p type event... it does not have many
      # implications if not that segments cannot be removed from it. We're not
      # doing it anyway.
      assert_raise(RuntimeError, fn -> Builder.new(%Media{p | type: :event}) end)
    end
  end

  describe "sync" do
    test "is performed when time is in the future", %{playlist: playlist} do
      builder = Builder.new(playlist)

      assert {_to_upload, _builder} = Builder.sync(builder, 3)
    end

    test "raises when when time is in the past", %{playlist: playlist} do
      segments = [
        %Segment{uri: URI.new!("0.txt"), relative_sequence: 0, duration: 1},
        %Segment{uri: URI.new!("1.txt"), relative_sequence: 1, duration: 1}
      ]

      playlist = %Media{playlist | segments: segments}
      builder = Builder.new(playlist)

      assert_raise(RuntimeError, fn ->
        Builder.sync(builder, 1)
      end)
    end

    test "returns the segments that should be uploaded", %{playlist: playlist} do
      builder = Builder.new(playlist)
      {to_upload, _builder} = Builder.sync(builder, 3)

      assert [0, 1, 2] == Enum.map(to_upload, fn x -> x.relative_sequence end)
    end

    test "syncs with zero", %{playlist: playlist} do
      builder = Builder.new(playlist)
      {to_upload, _builder} = Builder.sync(builder, 0)

      assert [] == Enum.map(to_upload, fn x -> x.relative_sequence end)
    end

    test "works with a pre-filled playlist", %{playlist: playlist} do
      segments = [
        %Segment{uri: URI.new!("0.txt"), relative_sequence: 0, duration: 1, from: 0},
        %Segment{uri: URI.new!("1.txt"), relative_sequence: 1, duration: 1, from: 1}
      ]

      playlist = %Media{playlist | segments: segments}
      builder = Builder.new(playlist)
      {to_upload, _builder} = Builder.sync(builder, 4)
      assert [2, 3] == Enum.map(to_upload, fn x -> x.relative_sequence end)
      assert [2, 3] == Enum.map(to_upload, fn x -> x.from end)
    end
  end

  describe "next_segment" do
    test "when the playlist is empty", %{playlist: playlist} do
      builder = Builder.new(playlist)

      assert {next, _builder} = Builder.next_segment(builder)
      assert next.from == 0
      assert next.relative_sequence == 0
    end

    test "when the playlist has been synced", %{playlist: playlist} do
      builder = Builder.new(playlist)
      {_to_upload, builder} = Builder.sync(builder, 3)
      {next, _builder} = Builder.next_segment(builder)
      assert next.from == 3
      assert next.relative_sequence == 3
    end

    test "returns a sequence of segments", %{playlist: playlist} do
      builder = Builder.new(playlist)

      {left, builder} = Builder.next_segment(builder)
      {right, _builder} = Builder.next_segment(builder)
      assert left.from + left.duration == right.from
      assert left.relative_sequence == right.relative_sequence - 1
    end
  end

  describe "segments are consolidated" do
    test "on positive acknoledgement", %{playlist: playlist} do
      builder = Builder.new(playlist)
      {next, builder} = Builder.next_segment(builder)
      builder = Builder.ack(builder, next.ref)

      assert builder.playlist.segments == [next]
    end

    test "on positive acknoledgement of multiple segments", %{playlist: playlist} do
      {segments, builder} =
        playlist
        |> Builder.new()
        |> Builder.sync(1.5)

      playlist =
        segments
        |> Enum.reduce(builder, fn segment, builder -> Builder.ack(builder, segment.ref) end)
        |> Builder.playlist()

      assert playlist.segments == segments
    end

    test "on positive acknoledgement with previously consolidated segments", %{playlist: playlist} do
      segments = [
        %Segment{uri: URI.new!("0.txt"), relative_sequence: 0, duration: 1},
        %Segment{uri: URI.new!("1.txt"), relative_sequence: 1, duration: 1}
      ]

      playlist = %Media{playlist | segments: segments}

      {new_segments, builder} =
        playlist
        |> Builder.new()
        |> Builder.sync(2.5)

      assert length(new_segments) == 1

      playlist =
        new_segments
        |> Enum.reduce(builder, fn segment, builder -> Builder.ack(builder, segment.ref) end)
        |> Builder.playlist()

      assert playlist.segments == segments ++ new_segments
    end

    test "on negative-acknoledgment, the empty segment is inserted in the playlist", %{
      playlist: playlist
    } do
      {segments, builder} =
        playlist
        |> Builder.new()
        |> Builder.sync(0.5)

      playlist =
        segments
        |> Enum.reduce(builder, fn segment, builder -> Builder.nack(builder, segment.ref) end)
        |> Builder.playlist()

      expected_uri = URI.new!("media/empty.vtt")
      assert [%Segment{uri: ^expected_uri, duration: 1, relative_sequence: 0}] = playlist.segments
    end
  end

  test "returned segments have a proper relative URI", %{playlist: playlist} do
    {next, _builder} =
      playlist
      |> Builder.new()
      |> Builder.next_segment()

    assert next.uri == URI.new!("media/00000.vtt")
  end

  test "raises when we use a reference that does not exist", %{playlist: playlist} do
    builder = Builder.new(playlist)
    assert_raise(KeyError, fn -> Builder.ack(builder, make_ref()) end)
    assert_raise(KeyError, fn -> Builder.nack(builder, make_ref()) end)
  end
end
