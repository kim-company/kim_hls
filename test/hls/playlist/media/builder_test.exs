defmodule HLS.Playlist.Media.BuilderTest do
  use ExUnit.Case

  alias HLS.Playlist.Media.Builder
  alias HLS.Playlist.Media
  alias HLS.Segment

  test "fits one payload in the future" do
    playlist = Media.new(URI.new!("http://example.com/data/media.m3u8"), 3)

    builder =
      playlist
      |> Builder.new(".ts")
      # Buffers are allowed to start in a segment and finish in the other one.
      |> Builder.fit(%{from: 4, to: 5, payload: <<>>})
      |> Builder.flush()

    playlist = Builder.playlist(builder)
    segments = Media.segments(playlist)

    assert length(segments) == 2
    assert Media.compute_playlist_duration(playlist) == 6
    assert Enum.map(segments, fn %Segment{absolute_sequence: x} -> x end) == [0, 1]
    refute Enum.any?(segments, fn %Segment{uri: x} -> x == nil end)
  end

  test "flushes when the payload finishes in the next segment window" do
    playlist = Media.new(URI.new!("s3://bucket/media.m3u8"), 1)

    {uploadables, builder} =
      playlist
      |> Builder.new(".ts")
      # Buffers are allowed to start in a segment and finish in the other one.
      |> Builder.fit(%{from: 0, to: 1.5, payload: <<>>})
      |> Builder.take_uploadables()

    playlist = Builder.playlist(builder)
    segments = Media.segments(playlist)

    assert length(segments) == 1
    assert length(uploadables) == 1
  end

  test "take uploadables" do
    playlist = Media.new(URI.new!("http://example.com/data/media.m3u8"), 3)

    {uploadables, builder} =
      playlist
      |> Builder.new(".ts")
      # Buffers are allowed to start in a segment and finish in the other one.
      |> Builder.fit(%{from: 1, to: 2, payload: "a"})
      |> Builder.fit(%{from: 2, to: 3, payload: "b"})
      # This buffer triggers a segment window switch forward, hence the previous
      # one is considered complete.
      |> Builder.fit(%{from: 3, to: 5, payload: "c"})
      |> Builder.fit(%{from: 5, to: 7, payload: "d"})
      |> Builder.fit(%{from: 8, to: 8.5, payload: "e"})
      |> Builder.take_uploadables()

    assert length(uploadables) == 2

    assert [
             %{
               payload: [%{from: 1, to: 2, payload: "a"}, %{from: 2, to: 3, payload: "b"}],
               uri: URI.new!("http://example.com/data/media/00000.ts")
             },
             %{
               payload: [%{from: 3, to: 5, payload: "c"}, %{from: 5, to: 7, payload: "d"}],
               uri: URI.new!("http://example.com/data/media/00001.ts")
             }
           ] == uploadables

    playlist = Builder.playlist(builder)
    segments = Media.segments(playlist)

    # The other one is still pending.
    assert length(segments) == 2
  end
end
