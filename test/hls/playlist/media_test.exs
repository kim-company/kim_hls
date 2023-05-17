defmodule HLS.Playlist.MediaTest do
  use ExUnit.Case
  alias HLS.Playlist.Media

  test "build_segment_uri/2" do
    playlist_uri = URI.new!("s3://bucket/media.m3u8")

    assert URI.new!("s3://bucket/media/1.ts") ==
             Media.build_segment_uri(playlist_uri, URI.new!("media/1.ts"))
  end
end
