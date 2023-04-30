defmodule HLS.ReaderTest do
  use ExUnit.Case

  alias HLS.Reader
  alias HLS.Playlist.{Master, Media}

  @store Reader.new(URI.new!("./test/fixtures/mpeg-ts/stream.m3u8"))

  describe "ready?/1" do
    test "returns false when location is invalid" do
      reader = Reader.new(URI.new!("invalid/location"))
      refute Reader.ready?(reader)
    end

    test "returns true when the location exists" do
      assert Reader.ready?(@store)
    end
  end

  describe "Load playlist from disk" do
    test "fails when manifest location is invalid" do
      reader = Reader.new(URI.new!("invalid/location"))
      assert {:error, _reason} = Reader.read_master_playlist(reader)
    end

    test "gets valid master playlist" do
      assert {:ok, %Master{}} = Reader.read_master_playlist(@store)
    end

    test "gets media playlists from variant streams" do
      seen =
        @store
        |> Reader.read_master_playlist!()
        |> Master.variant_streams()
        |> Enum.reduce(0, fn stream, seen ->
          assert {:ok, %Media{}} = Reader.read_media_playlist(@store, stream.uri)
          seen + 1
        end)

      assert seen == 2
    end

    test "gets segments" do
      seen =
        @store
        |> Reader.read_master_playlist!()
        |> Master.variant_streams()
        |> Enum.reduce(0, fn stream, seen ->
          segments_seen =
            @store
            |> Reader.read_media_playlist!(stream.uri)
            |> Media.segments()
            |> Enum.reduce(0, fn segment, seen ->
              assert {:ok, _data} = Reader.read_segment(@store, segment.uri)
              seen + 1
            end)

          segments_seen + seen
        end)

      assert seen == 10
    end
  end
end
