defmodule HLS.Playlist.MasterTest do
  use ExUnit.Case, async: true

  alias HLS.Playlist

  @playlist_file "test/fixtures/master_playlists/stream_with_audio_tracks.m3u8"

  test "can unmarshal playlist" do
    playlist = File.read!(@playlist_file)
    assert %Playlist.Master{} = Playlist.unmarshal(playlist, %Playlist.Master{})
  end

  test "unmarshal and marshal results in the same playlist" do
    raw_playlist = File.read!(@playlist_file)

		assert %Playlist.Master{} = playlist = Playlist.unmarshal(raw_playlist, %Playlist.Master{})
		marshaled_playlist = Playlist.marshal(playlist)
		assert is_binary(marshaled_playlist)
		assert Playlist.unmarshal(marshaled_playlist, %Playlist.Master{}) == playlist
  end
end
