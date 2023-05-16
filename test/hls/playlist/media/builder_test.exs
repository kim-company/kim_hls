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

  describe "does not return any segment" do
    test "when the next segment is not full", %{playlist: playlist} do
      assert {[], %Builder{}} =
               playlist
               |> Builder.new()
               |> Builder.fit(%{from: 0, to: 0.5})
               |> Builder.pop()
    end
  end

  describe "returns complete segments" do
    test "when forced", %{playlist: playlist} do
      buffer = %{from: 0, to: 0.5}

      expected_payload = %{
        uri: URI.new!("s3://bucket/media/00000.vtt"),
        from: 0,
        to: 1,
        buffers: [buffer]
      }

      assert {[{_, ^expected_payload}], %Builder{}} =
               playlist
               |> Builder.new()
               |> Builder.fit(buffer)
               |> Builder.pop(force: true)
    end
  end

  #   describe "take uploadables" do
  #   end

  #   describe "without flushing" do
  #     test "flushes when the payload finishes in the next segment window" do
  #       playlist = Media.new(URI.new!("s3://bucket/media.m3u8"), 1)

  #       {uploadables, builder} =
  #         playlist
  #         |> Builder.new(".ts")
  #         # Buffers are allowed to start in a segment and finish in the other one.
  #         |> Builder.fit(%{from: 0, to: 1.5, payload: <<>>})
  #         |> Builder.take_uploadables()

  #       playlist = Builder.playlist(builder)
  #       segments = Media.segments(playlist)

  #       assert length(segments) == 1
  #       assert length(uploadables) == 1
  #     end

  #     test "take uploadables" do
  #       playlist = Media.new(URI.new!("http://example.com/data/media.m3u8"), 3)

  #       {uploadables, builder} =
  #         playlist
  #         |> Builder.new(".ts")
  #         # Buffers are allowed to start in a segment and finish in the other one.
  #         |> Builder.fit(%{from: 1, to: 2, payload: "a"})
  #         |> Builder.fit(%{from: 2, to: 3, payload: "b"})
  #         # This buffer triggers a segment window switch forward, hence the previous
  #         # one is considered complete.
  #         |> Builder.fit(%{from: 3, to: 5, payload: "c"})
  #         |> Builder.fit(%{from: 5, to: 7, payload: "d"})
  #         |> Builder.fit(%{from: 8, to: 8.5, payload: "e"})
  #         |> Builder.take_uploadables()

  #       assert length(uploadables) == 2

  #       assert [
  #                %{
  #                  buffers: [%{from: 1, to: 2, payload: "a"}, %{from: 2, to: 3, payload: "b"}],
  #                  uri: URI.new!("http://example.com/data/media/00000.ts"),
  #                  from: 0,
  #                  to: 3
  #                },
  #                %{
  #                  buffers: [%{from: 3, to: 5, payload: "c"}, %{from: 5, to: 7, payload: "d"}],
  #                  uri: URI.new!("http://example.com/data/media/00001.ts"),
  #                  from: 3,
  #                  to: 6
  #                }
  #              ] == uploadables

  #       playlist = Builder.playlist(builder)
  #       segments = Media.segments(playlist)

  #       # The other one is still pending.
  #       assert length(segments) == 2
  #     end

  #     test "produces empty segments if cues span more than one segment" do
  #       playlist = Media.new(URI.new!("http://example.com/data/media.m3u8"), 1)

  #       {_uploadables, builder} =
  #         playlist
  #         |> Builder.new(".ts")
  #         # Buffers are allowed to start in a segment and finish in the other one.
  #         |> Builder.fit(%{from: 0, to: 3, payload: "a"})
  #         |> Builder.fit(%{from: 3, to: 4, payload: "b"})
  #         |> Builder.take_uploadables()

  #       segments =
  #         builder
  #         |> Builder.playlist()
  #         |> Media.segments()

  #       assert length(segments) == 4
  #     end

  #     test "does not produce uploadables if the first timed buffer creates a list of trailing empty uploadables" do
  #       # This case covers the situation in which the Builder is used to recover
  #       # a playlist in the middle of a stream. If it would emit all empty
  #       # uploadables they would override previous segments, probably filled with
  #       # content.
  #       playlist = Media.new(URI.new!("http://example.com/data/media.m3u8"), 1)

  #       {uploadables, builder} =
  #         playlist
  #         |> Builder.new(".ts")
  #         # Buffers are allowed to start in a segment and finish in the other one.
  #         |> Builder.fit(%{from: 3, to: 4, payload: "a"})
  #         |> Builder.take_uploadables()

  #       assert [
  #                %{
  #                  buffers: [%{from: 3, to: 4, payload: "a"}],
  #                  uri: URI.new!("http://example.com/data/media/00003.ts"),
  #                  from: 3,
  #                  to: 4
  #                }
  #              ] == uploadables

  #       # Even though we require just one upload, the playlist should contain
  #       # all segments up to that point.
  #       segments =
  #         builder
  #         |> Builder.playlist()
  #         |> Media.segments()

  #       assert length(segments) == 4
  #     end
  #   end

  #   describe "flushing" do
  #     test "fits one payload in the future" do
  #       playlist = Media.new(URI.new!("http://example.com/data/media.m3u8"), 3)

  #       builder =
  #         playlist
  #         |> Builder.new(".ts")
  #         # Buffers are allowed to start in a segment and finish in the other one.
  #         |> Builder.fit(%{from: 4, to: 5, payload: <<>>})
  #         |> Builder.flush()

  #       playlist = Builder.playlist(builder)
  #       segments = Media.segments(playlist)

  #       assert length(segments) == 2
  #       assert Media.compute_playlist_duration(playlist) == 6
  #       assert Enum.map(segments, fn %Segment{absolute_sequence: x} -> x end) == [0, 1]
  #       refute Enum.any?(segments, fn %Segment{uri: x} -> x == nil end)
  #     end

  #     test "does not produce empty trailing segments if no data has been received for them" do
  #       playlist = Media.new(URI.new!("s3://bucket/media.m3u8"), 1)

  #       {uploadables, builder} =
  #         playlist
  #         |> Builder.new(".ts")
  #         |> Builder.fit(%{from: 0, to: 2.5, payload: <<>>})
  #         |> Builder.flush()
  #         |> Builder.take_uploadables()

  #       playlist = Builder.playlist(builder)
  #       segments = Media.segments(playlist)

  #       assert length(segments) == 2
  #       assert length(uploadables) == 2
  #     end
  #   end
  # end
end
