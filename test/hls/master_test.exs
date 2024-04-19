defmodule HLS.Playlist.MasterTest do
  use ExUnit.Case, async: true

  alias HLS.Playlist

  @playlist_file "test/fixtures/master_playlists/stream_with_audio_tracks.m3u8"

  test "extracts media relative uri" do
    master = URI.new!("https://v.t/4fafW0nnol6i/stream.m3u8")
    media = URI.new!("https://v.t/4fafW0nnol6i/stream_Afg.m3u8")
    expected = URI.new!("stream_Afg.m3u8")

    assert Playlist.Master.extract_relative_uri(master, media) == expected
  end

  test "builds media absolute uri" do
    master = URI.new!("https://v.t/4fafW0nnol6i/stream.m3u8")
    media = URI.new!("stream_Afg.m3u8")
    expected = URI.new!("https://v.t/4fafW0nnol6i/stream_Afg.m3u8")

    assert Playlist.Master.build_media_uri(master, media) == expected
  end

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

  describe "add_alternative_rendition/2" do
    test "when to group_id was not specified yet" do
      raw = """
      #EXTM3U
      #EXT-X-VERSION:3
      #EXT-X-INDEPENDENT-SEGMENTS
      #EXT-X-STREAM-INF:BANDWIDTH=334400,AVERAGE-BANDWIDTH=325600,CODECS="avc1.42c01e,mp4a.40.2",RESOLUTION=416x234,FRAME-RATE=15.000
      stream_416x234.m3u8
      """

      master = Playlist.unmarshal(raw, %Playlist.Master{})

      master =
        Playlist.Master.add_alternative_rendition(master, %HLS.AlternativeRendition{
          uri: URI.new!("alt.m3u8"),
          name: "Sub",
          type: :subtitles
        })

      [alt] = master.alternative_renditions
      assert alt.group_id == HLS.AlternativeRendition.default_group_for_type(alt.type)

      [stream] = master.streams
      assert stream.subtitles == alt.group_id
    end

    test "when there is already a group_id for this alternative type" do
      raw = """
      #EXTM3U
      #EXT-X-VERSION:3
      #EXT-X-INDEPENDENT-SEGMENTS
      #EXT-X-STREAM-INF:BANDWIDTH=334400,AVERAGE-BANDWIDTH=325600,CODECS="avc1.42c01e,mp4a.40.2",RESOLUTION=416x234,FRAME-RATE=15.000,SUBTITLES="SUBTITLES"
      stream_416x234.m3u8
      #EXT-X-MEDIA:GROUP-ID="SUBTITLES",NAME="Sub",TYPE=SUBTITLES,URI="first.m3u8"
      """

      master = Playlist.unmarshal(raw, %Playlist.Master{})

      master =
        Playlist.Master.add_alternative_rendition(master, %HLS.AlternativeRendition{
          uri: URI.new!("second.m3u8"),
          name: "Another Sub",
          type: :subtitles
        })

      [sub1, sub2] = master.alternative_renditions
      assert sub1.group_id == sub2.group_id
      [stream] = master.streams
      assert stream.subtitles == sub1.group_id
      assert stream.subtitles == sub2.group_id
    end

    test "adds more renditions in case there are multiple streams" do
      raw = """
      #EXTM3U
      #EXT-X-VERSION:3
      #EXT-X-INDEPENDENT-SEGMENTS
      #EXT-X-STREAM-INF:BANDWIDTH=371056,AVERAGE-BANDWIDTH=326462,CODECS="avc1.64000c,mp4a.40.2",RESOLUTION=416x234,FRAME-RATE=15.000,AUDIO="program_audio_96k",SUBTITLES="subtitles"
      stream_416x234.m3u8
      #EXT-X-STREAM-INF:BANDWIDTH=7921349,AVERAGE-BANDWIDTH=6890633,CODECS="avc1.640028,mp4a.40.2",RESOLUTION=1920x1080,FRAME-RATE=30.000,AUDIO="program_audio_160k",SUBTITLES="subtitles"
      stream_1920x1080.m3u8
      #EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="program_audio_96k",LANGUAGE="eng",NAME="English",AUTOSELECT=YES,DEFAULT=YES,URI="stream_audio_0_96k.m3u8"
      #EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="program_audio_160k",LANGUAGE="eng",NAME="English",AUTOSELECT=YES,DEFAULT=YES,URI="stream_audio_0_160k.m3u8"
      """

      master = Playlist.unmarshal(raw, %Playlist.Master{})

      master =
        Playlist.Master.add_alternative_rendition(master, %HLS.AlternativeRendition{
          uri: URI.new!("alt.m3u8"),
          name: "Dubbed Content",
          type: :audio
        })

      # We add one rendition for each group.
      assert length(master.alternative_renditions) == 4

      new_alts =
        master.alternative_renditions
        |> Enum.filter(fn x -> x.name == "Dubbed Content" end)

      assert length(new_alts) == 2
      group_ids = Enum.map(new_alts, fn x -> x.group_id end) |> Enum.uniq()
      assert length(group_ids) == 2
    end

    test "if group_id is specified, simply adds the rendition" do
      raw = """
      #EXTM3U
      #EXT-X-VERSION:3
      #EXT-X-INDEPENDENT-SEGMENTS
      #EXT-X-STREAM-INF:BANDWIDTH=334400,AVERAGE-BANDWIDTH=325600,CODECS="avc1.42c01e,mp4a.40.2",RESOLUTION=416x234,FRAME-RATE=15.000
      stream_416x234.m3u8
      """

      master = Playlist.unmarshal(raw, %Playlist.Master{})

      master =
        Playlist.Master.add_alternative_rendition(master, %HLS.AlternativeRendition{
          uri: URI.new!("alt.m3u8"),
          name: "Sub",
          type: :subtitles,
          group_id: "OTHER"
        })

      [alt] = master.alternative_renditions
      assert alt.group_id == "OTHER"

      [stream] = master.streams
      assert stream.subtitles == alt.group_id
    end

    test "if multiple streams are present with no group_id, only one rendition is added" do
      raw = """
      #EXTM3U
      #EXT-X-VERSION:4
      #EXT-X-INDEPENDENT-SEGMENTS
      #EXT-X-STREAM-INF:AUDIO="program_audio_96k",AVERAGE-BANDWIDTH=326266,BANDWIDTH=350451,CODECS="avc1.64000c,mp4a.40.2",FRAME-RATE=15.000,RESOLUTION=416x234,SUBTITLES="SUBTITLES"
      stream_416x234.m3u8
      #EXT-X-STREAM-INF:AUDIO="program_audio_96k",AVERAGE-BANDWIDTH=944170,BANDWIDTH=1038982,CODECS="avc1.640016,mp4a.40.2",FRAME-RATE=15.000,RESOLUTION=640x360,SUBTITLES="SUBTITLES"
      stream_640x360.m3u8
      #EXT-X-STREAM-INF:AUDIO="program_audio_96k",AVERAGE-BANDWIDTH=1370855,BANDWIDTH=1445212,CODECS="avc1.64001f,mp4a.40.2",FRAME-RATE=30.000,RESOLUTION=854x480,SUBTITLES="SUBTITLES"
      stream_854x480.m3u8
      #EXT-X-MEDIA:AUTOSELECT=YES,CHARACTERISTICS="vt.track.original",DEFAULT=YES,GROUP-ID="program_audio_96k",LANGUAGE="de",NAME="German Audio Track",TYPE=AUDIO,URI="stream_audio_0_96k.m3u8"
      """

      master = Playlist.unmarshal(raw, %Playlist.Master{})

      master =
        Playlist.Master.add_alternative_rendition(master, %HLS.AlternativeRendition{
          uri: URI.new!("alt.m3u8"),
          name: "Sub",
          type: :subtitles
        })

      assert length(master.alternative_renditions) == 2
    end

    test "does not add the rendition multiple times" do
      raw = """
      #EXTM3U
      #EXT-X-VERSION:3
      #EXT-X-INDEPENDENT-SEGMENTS
      #EXT-X-STREAM-INF:BANDWIDTH=334400,AVERAGE-BANDWIDTH=325600,CODECS="avc1.42c01e,mp4a.40.2",RESOLUTION=416x234,FRAME-RATE=15.000
      stream_416x234.m3u8
      """

      master =
        raw
        |> Playlist.unmarshal(%Playlist.Master{})
        |> Playlist.Master.add_alternative_rendition(%HLS.AlternativeRendition{
          uri: URI.new!("alt.m3u8"),
          name: "Sub",
          type: :subtitles,
          group_id: "OTHER"
        })
        |> Playlist.Master.add_alternative_rendition(%HLS.AlternativeRendition{
          uri: URI.new!("alt.m3u8?v=1"),
          name: "Sub",
          type: :subtitles,
          group_id: "OTHER"
        })

      assert [alt] = master.alternative_renditions
      assert alt.group_id == "OTHER"
      assert alt.uri == URI.new!("alt.m3u8?v=1")

      [stream] = master.streams
      assert stream.subtitles == alt.group_id
    end

    test "if multiple groups are present, adds the new rendition to each group" do
      raw = """
      #EXTM3U
      #EXT-X-VERSION:3
      #EXT-X-INDEPENDENT-SEGMENTS
      #EXT-X-STREAM-INF:AUDIO="program_audio_96k",AVERAGE-BANDWIDTH=326266,BANDWIDTH=350451,CODECS="avc1.64000c,mp4a.40.2",FRAME-RATE=15.000,RESOLUTION=416x234
      stream_416x234.m3u8
      #EXT-X-STREAM-INF:AUDIO="program_audio_128k",AVERAGE-BANDWIDTH=944170,BANDWIDTH=1038982,CODECS="avc1.640016,mp4a.40.2",FRAME-RATE=15.000,RESOLUTION=640x360
      stream_640x360.m3u8
      #EXT-X-MEDIA:AUTOSELECT=YES,DEFAULT=YES,GROUP-ID="program_audio_96k",LANGUAGE="de",NAME="German Audio Track",TYPE=AUDIO,URI="stream_audio_0_96k.m3u8"
      #EXT-X-MEDIA:AUTOSELECT=YES,DEFAULT=YES,GROUP-ID="program_audio_128k",LANGUAGE="de",NAME="German Audio Track",TYPE=AUDIO,URI="stream_audio_0_128k.m3u8"
      """

      master =
        raw
        |> Playlist.unmarshal(%Playlist.Master{})
        |> Playlist.Master.add_alternative_rendition(%HLS.AlternativeRendition{
          uri: URI.new!("alt.m3u8?v=1"),
          name: "Sub",
          type: :audio
        })

      assert 4 = length(master.alternative_renditions)
    end
  end
end
