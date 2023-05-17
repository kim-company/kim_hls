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

  test "accepts timed buffers when they are in the future", %{playlist: playlist} do
    builder = Builder.new(playlist)
    assert builder = Builder.fit(builder, %{from: 0, to: 1})
    assert builder = Builder.fit(builder, %{from: 2, to: 3})
    # Even though they are out of order we're just concerned that they are
    # not before the segments that are stored in the playlist, which have been
    # already consolidated in the storage.
    assert %Builder{} = Builder.fit(builder, %{from: 1, to: 2})
  end

  test "raises when timed buffers fall in the range of consolidated segments", %{
    playlist: playlist
  } do
    segments = [
      %Segment{uri: URI.new!("0.txt"), relative_sequence: 0, duration: 1},
      %Segment{uri: URI.new!("1.txt"), relative_sequence: 1, duration: 1}
    ]

    playlist = %Media{playlist | segments: segments}
    builder = Builder.new(playlist)

    assert_raise(RuntimeError, fn ->
      Builder.fit(builder, %{from: 0, to: 1})
    end)
  end

  describe "does not return any uploadable" do
    test "when the next segment is not full", %{playlist: playlist} do
      assert {[], %Builder{}} =
               playlist
               |> Builder.new()
               |> Builder.fit(%{from: 0, to: 0.5})
               |> Builder.pop()
    end

    test "when the playlist contains segments and the new segment is not full", %{
      playlist: playlist
    } do
      segments = [
        %Segment{uri: URI.new!("0.txt"), relative_sequence: 0, duration: 1}
      ]

      playlist = %Media{playlist | segments: segments}

      assert {[], %Builder{}} =
               playlist
               |> Builder.new()
               |> Builder.fit(%{from: 1, to: 1.5})
               |> Builder.pop()
    end
  end

  describe "returns complete segments" do
    test "when forced", %{playlist: playlist} do
      buffer = %{from: 0, to: 0.5}

      assert {[%{from: 0, to: 1, payloads: [^buffer]}], %Builder{}} =
               playlist
               |> Builder.new()
               |> Builder.fit(buffer)
               |> Builder.pop(force: true)
    end

    test "when they fill their segment", %{playlist: playlist} do
      buffer = %{from: 0, to: 1}

      assert {[%{from: 0, to: 1, payloads: [^buffer]}], %Builder{}} =
               playlist
               |> Builder.new()
               |> Builder.fit(buffer)
               |> Builder.pop()
    end

    test "when it replaces them with empty the empty segment, if the buffer is empty", %{
      playlist: playlist
    } do
      uri = URI.new!("media/empty.vtt")

      assert {[%{segment: %Segment{uri: ^uri}}], %Builder{}} =
               playlist
               |> Builder.new(replace_empty_segments_uri: true)
               |> Builder.fit(%{from: 0, to: 1})
               |> Builder.pop()
    end

    test "when it replaces them with empty the empty segment, if the buffer is not empty", %{
      playlist: playlist
    } do
      uri = URI.new!("media/00000.vtt")

      assert {[%{segment: %Segment{uri: ^uri}}], %Builder{}} =
               playlist
               |> Builder.new(replace_empty_segments_uri: true)
               |> Builder.fit(%{from: 0, to: 1, payload: "a"})
               |> Builder.pop()
    end

    test "when they fill their segment and more", %{playlist: playlist} do
      buffer = %{from: 0, to: 1.5}

      assert {[%{from: 0, to: 1, payloads: [^buffer]}], %Builder{}} =
               playlist
               |> Builder.new()
               |> Builder.fit(buffer)
               |> Builder.pop()
    end

    test "when they span over multiple segments", %{playlist: playlist} do
      buffer = %{from: 1, to: 2}

      assert {[%{from: 0, to: 1, payloads: []}, %{from: 1, to: 2, payloads: [^buffer]}],
              %Builder{}} =
               playlist
               |> Builder.new()
               |> Builder.fit(buffer)
               |> Builder.pop()
    end

    test "when they span over multiple segments w/o completing the last segment", %{
      playlist: playlist
    } do
      buffer = %{from: 1, to: 1.5}

      assert {[%{from: 0, to: 1, payloads: []}], %Builder{}} =
               playlist
               |> Builder.new()
               |> Builder.fit(buffer)
               |> Builder.pop()
    end
  end

  test "returned segments have a proper relative URI", %{playlist: playlist} do
    buffer = %{from: 0, to: 1}

    assert {[%{segment: segment}], %Builder{}} =
             playlist
             |> Builder.new()
             |> Builder.fit(buffer)
             |> Builder.pop()

    assert segment.uri == URI.new!("media/00000.vtt")
  end

  describe "segments are not consolidated in the playlist" do
    test "when new buffers are fit in the builder", %{playlist: playlist} do
      buffer = %{from: 0, to: 1}

      playlist =
        playlist
        |> Builder.new()
        |> Builder.fit(buffer)
        |> Builder.playlist()

      assert playlist.segments == []
    end

    test "when segments are popped out of the builder", %{playlist: playlist} do
      buffer = %{from: 0, to: 1}

      {uploadables, builder} =
        playlist
        |> Builder.new()
        |> Builder.fit_and_pop(buffer)

      assert length(uploadables) == 1
      assert Builder.playlist(builder).segments == []
    end

    test "when we use a reference that does not exist", %{playlist: playlist} do
      builder = Builder.new(playlist)
      assert_raise(KeyError, fn -> Builder.ack(builder, make_ref()) end)
      assert_raise(KeyError, fn -> Builder.nack(builder, make_ref()) end)
    end
  end

  describe "segments are consolidated" do
    test "on positive acknoledgement", %{playlist: playlist} do
      buffer = %{from: 0, to: 1}

      {[%{ref: ref, segment: segment}], builder} =
        playlist
        |> Builder.new()
        |> Builder.fit_and_pop(buffer)

      playlist =
        builder
        |> Builder.ack(ref)
        |> Builder.playlist()

      assert playlist.segments == [segment]
    end

    test "on positive acknoledgement of multiple segments", %{playlist: playlist} do
      buffer = %{from: 0, to: 2}

      {uploadables, builder} =
        playlist
        |> Builder.new()
        |> Builder.fit_and_pop(buffer)

      playlist =
        uploadables
        |> Enum.reduce(builder, fn %{ref: ref}, builder -> Builder.ack(builder, ref) end)
        |> Builder.playlist()

      assert playlist.segments == Enum.map(uploadables, fn %{segment: x} -> x end)
    end

    test "on positive acknoledgement with previously consolidated segments", %{playlist: playlist} do
      segments = [
        %Segment{uri: URI.new!("0.txt"), relative_sequence: 0, duration: 1},
        %Segment{uri: URI.new!("1.txt"), relative_sequence: 1, duration: 1}
      ]

      playlist = %Media{playlist | segments: segments}

      {uploadables, builder} =
        playlist
        |> Builder.new()
        |> Builder.fit_and_pop(%{from: 2, to: 3})

      playlist =
        uploadables
        |> Enum.reduce(builder, fn %{ref: ref}, builder -> Builder.ack(builder, ref) end)
        |> Builder.playlist()

      assert playlist.segments == segments ++ Enum.map(uploadables, fn %{segment: x} -> x end)
    end

    test "on negative-acknoledgment, the empty segment is inserted in the playlist", %{
      playlist: playlist
    } do
      buffer = %{from: 0, to: 1, payload: "hello"}

      {[%{ref: ref}], builder} =
        playlist
        |> Builder.new()
        |> Builder.fit_and_pop(buffer)

      playlist =
        builder
        |> Builder.nack(ref)
        |> Builder.playlist()

      expected_uri = URI.new!("media/empty.vtt")
      assert [%Segment{uri: ^expected_uri, duration: 1}] = playlist.segments
    end
  end
end
