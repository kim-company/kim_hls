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

  test "adds alternative renditions" do
    raw_playlist = File.read!(@playlist_file)
    playlist = Playlist.unmarshal(raw_playlist, %Playlist.Master{})

    alternative = %HLS.AlternativeRendition{
      type: :audio,
      group_id: "program_audio_96k",
      name: "English 1",
      uri: %URI{
        scheme: nil,
        userinfo: nil,
        host: nil,
        port: nil,
        path: "stream_audio_abc_96k.m3u8",
        query: nil,
        fragment: nil
      },
      language: "eng",
      assoc_language: nil,
      autoselect: false,
      default: false,
      forced: nil,
      instream_id: nil,
      channels: nil,
      characteristics: nil,
      attributes: %{}
    }

    master = Playlist.Master.add_alternative_renditions(playlist, [alternative])

    lines =
      master
      |> Playlist.marshal()
      |> String.split("\n")

    assert List.last(lines) ==
             ~s/#EXT-X-MEDIA:AUTOSELECT=NO,DEFAULT=NO,GROUP-ID="program_audio_96k",LANGUAGE="eng",NAME="English 1",TYPE=AUDIO,URI="stream_audio_abc_96k.m3u8"/
  end
end
