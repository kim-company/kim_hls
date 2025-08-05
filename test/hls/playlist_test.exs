defmodule HLS.PlaylistTest do
  use ExUnit.Case

  alias HLS.{Playlist, VariantStream, AlternativeRendition, Segment}
  alias HLS.Playlist.{Master, Media}

  describe "Marshal Media Playlist" do
    setup do
      segments =
        [
          %Segment{
            duration: 3.0
          },
          %Segment{
            duration: 2.0
          }
        ]
        |> Enum.with_index()
        |> Enum.map(fn {seg, index} ->
          %Segment{
            seg
            | uri: URI.new!("data/#{index}.ts"),
              absolute_sequence: index,
              relative_sequence: index
          }
        end)

      playlist = %Media{
        version: 7,
        target_segment_duration: 3.0,
        media_sequence_number: 0,
        uri: URI.new!("https://example.com/data.m3u8"),
        segments: segments
      }

      %{playlist: playlist}
    end

    test "when the playlist is finished", %{playlist: playlist} do
      marshaled = """
        #EXTM3U
        #EXT-X-VERSION:7
        #EXT-X-TARGETDURATION:3
        #EXT-X-MEDIA-SEQUENCE:0
        #EXTINF:3.0,
        data/0.ts
        #EXTINF:2.0,
        data/1.ts
        #EXT-X-ENDLIST
      """

      playlist = %Media{playlist | finished: true}
      assert Playlist.marshal(playlist) == String.replace(marshaled, " ", "", global: true)
    end

    test "when the playlist is not finished", %{playlist: playlist} do
      marshaled = """
        #EXTM3U
        #EXT-X-VERSION:7
        #EXT-X-TARGETDURATION:3
        #EXT-X-MEDIA-SEQUENCE:0
        #EXTINF:3.0,
        data/0.ts
        #EXTINF:2.0,
        data/1.ts
      """

      # If the playlist is not finished it is assumed to be an EVENT
      # playlist, meaning that segments MAY be removed in the order
      # they appeared.
      playlist = %Media{playlist | finished: false}
      assert Playlist.marshal(playlist) == String.replace(marshaled, " ", "", global: true)
    end

    test "when the playlist is EVENT, but finished", %{playlist: playlist} do
      marshaled = """
        #EXTM3U
        #EXT-X-VERSION:7
        #EXT-X-PLAYLIST-TYPE:EVENT
        #EXT-X-TARGETDURATION:3
        #EXT-X-MEDIA-SEQUENCE:0
        #EXTINF:3.0,
        data/0.ts
        #EXTINF:2.0,
        data/1.ts
        #EXT-X-ENDLIST
      """

      # It means that the playlist may remove segments.
      playlist = %Media{playlist | type: :event, finished: true}
      assert Playlist.marshal(playlist) == String.replace(marshaled, " ", "", global: true)
    end

    test "when the playlist is VOD", %{playlist: playlist} do
      marshaled = """
        #EXTM3U
        #EXT-X-VERSION:7
        #EXT-X-PLAYLIST-TYPE:VOD
        #EXT-X-TARGETDURATION:3
        #EXT-X-MEDIA-SEQUENCE:0
        #EXTINF:3.0,
        data/0.ts
        #EXTINF:2.0,
        data/1.ts
        #EXT-X-ENDLIST
      """

      # it means that the playlist is both finished (no more segments will be
      # added), and it is VOD, hence no segments will be removed. The playlist
      # is now static.
      playlist = %Media{playlist | finished: true, type: :vod}
      assert Playlist.marshal(playlist) == String.replace(marshaled, " ", "", global: true)
    end

    test "with EXT-X-DISCONTINUITY", %{playlist: playlist} do
      marshaled = """
      #EXTM3U
      #EXT-X-VERSION:7
      #EXT-X-TARGETDURATION:3
      #EXT-X-MEDIA-SEQUENCE:0
      #EXTINF:3.0,
      data/0.ts
      #EXT-X-DISCONTINUITY
      #EXTINF:2.0,
      data/1.ts
      """

      [first, second] = playlist.segments

      playlist = %Media{playlist | segments: [first, %{second | discontinuity: true}]}
      assert Playlist.marshal(playlist) == String.replace(marshaled, " ", "", global: true)
    end

    test "with EXT-X-MAP tags", %{playlist: playlist} do
      marshaled = """
        #EXTM3U
        #EXT-X-VERSION:7
        #EXT-X-TARGETDURATION:3
        #EXT-X-MEDIA-SEQUENCE:0
        #EXT-X-MAP:URI="main_1.mp4",BYTERANGE="719@0"
        #EXTINF:3.0,
        #EXT-X-BYTERANGE:1508000@719
        data/0.ts
        #EXTINF:2.0,
        #EXT-X-BYTERANGE:1510244@1508719
        data/1.ts
        #EXT-X-MAP:URI="main_2.mp4",BYTERANGE="1510244@1508719"
        #EXTINF:2.0,
        data/2.ts
      """

      segments =
        [
          %Segment{
            duration: 3.0,
            init_section: %{uri: "main_1.mp4", byterange: %{offset: 0, length: 719}},
            byterange: %{offset: 719, length: 1_508_000}
          },
          %Segment{
            duration: 2.0,
            init_section: %{uri: "main_1.mp4", byterange: %{offset: 0, length: 719}},
            byterange: %{offset: 1_508_719, length: 1_510_244}
          },
          %Segment{
            duration: 2.0,
            init_section: %{uri: "main_2.mp4", byterange: %{offset: 1_508_719, length: 1_510_244}}
          }
        ]
        |> Enum.with_index()
        |> Enum.map(fn {seg, index} ->
          %Segment{
            seg
            | uri: URI.new!("data/#{index}.ts"),
              absolute_sequence: index,
              relative_sequence: index
          }
        end)

      playlist = %{playlist | segments: segments}

      assert Playlist.marshal(playlist) == String.replace(marshaled, " ", "", global: true)
    end

    test "with program date time tags", %{playlist: playlist} do
      {:ok, datetime1, _} = DateTime.from_iso8601("2024-08-19T12:08:16.015Z")
      {:ok, datetime2, _} = DateTime.from_iso8601("2024-08-19T12:08:20.015Z")

      # Update segments with program date time
      segments =
        playlist.segments
        |> List.update_at(0, fn seg -> %{seg | program_date_time: datetime1} end)
        |> List.update_at(1, fn seg -> %{seg | program_date_time: datetime2} end)

      playlist_with_pdt = %{playlist | segments: segments, finished: true}

      # Marshal and unmarshal
      marshaled = Playlist.marshal(playlist_with_pdt)
      unmarshaled = Playlist.unmarshal(marshaled, %Media{})

      # Verify segments still have program date time
      [first_seg, second_seg] = unmarshaled.segments
      assert first_seg.program_date_time == datetime1
      assert second_seg.program_date_time == datetime2

      # Verify it contains the expected tags in the marshaled output
      assert String.contains?(marshaled, "#EXT-X-PROGRAM-DATE-TIME:2024-08-19T12:08:16.015Z")
      assert String.contains?(marshaled, "#EXT-X-PROGRAM-DATE-TIME:2024-08-19T12:08:20.015Z")
    end
  end

  describe "Marshal Master Playlist" do
    test "with streams" do
      content = """
      #EXTM3U
      #EXT-X-VERSION:7
      #EXT-X-STREAM-INF:BANDWIDTH=1187651,CODECS="avc1.42e00a"
      muxed_video_480x270.m3u8
      #EXT-X-STREAM-INF:BANDWIDTH=609514,CODECS="avc1.42e00a"
      muxed_video_540x360.m3u8
      #EXT-X-STREAM-INF:BANDWIDTH=863865,CODECS="avc1.42e00a"
      muxed_video_720x480.m3u8
      """

      manifest = Playlist.unmarshal(content, %Master{})
      marshaled = Playlist.marshal(manifest)
      manifest = Playlist.unmarshal(marshaled, %Master{})
      assert Enum.count(Master.variant_streams(manifest)) == 3
    end

    test "with alternative renditions" do
      content = """
      #EXTM3U
      #EXT-X-VERSION:7
      #EXT-X-STREAM-INF:BANDWIDTH=1187651,CODECS="avc1.42e00a,SUBTITLES="subtitles"
      muxed_video_480x270.m3u8
      #EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="subtitles",NAME="German (Germany)",DEFAULT=NO,AUTOSELECT=NO,FORCED=NO,LANGUAGE="de-DE",URI="subtitles.m3u8"
      """

      manifest = Playlist.unmarshal(content, %Master{})
      marshaled = Playlist.marshal(manifest)
      manifest = Playlist.unmarshal(marshaled, %Master{})
      assert Enum.count(Master.variant_streams(manifest)) == 1
      assert Enum.count(manifest.alternative_renditions) == 1
    end

    test "with alternative renditions, characteristics as []" do
      content = """
      #EXTM3U
      #EXT-X-VERSION:7
      #EXT-X-STREAM-INF:BANDWIDTH=1187651,CODECS="avc1.42e00a
      muxed_video_480x270.m3u8
      """

      lines =
        content
        |> Playlist.unmarshal(%Master{})
        |> Playlist.Master.add_alternative_rendition(%HLS.AlternativeRendition{
          uri: URI.new!("subtitles.m3u8"),
          type: :subtitles,
          name: "test",
          characteristics: []
        })
        |> Playlist.marshal()
        |> String.split("\n")

      assert lines == [
               "#EXTM3U",
               "#EXT-X-VERSION:7",
               "#EXT-X-STREAM-INF:BANDWIDTH=1187651,CODECS=\"avc1.42e00a\",SUBTITLES=\"SUBTITLES\"",
               "muxed_video_480x270.m3u8",
               "#EXT-X-MEDIA:GROUP-ID=\"SUBTITLES\",NAME=\"test\",TYPE=SUBTITLES,URI=\"subtitles.m3u8\""
             ]
    end
  end

  describe "Unmarshal Master Playlist" do
    test "fails with empty content" do
      [
        fn -> Playlist.unmarshal("", %Master{}) end,
        fn -> Playlist.unmarshal("some invalid content", %Master{}) end
      ]
      |> Enum.each(fn t -> assert_raise ArgumentError, t end)
    end

    test "parses manifest version" do
      version = 3

      content = """
      #EXTM3U
      #EXT-X-VERSION:#{version}
      """

      manifest = Playlist.unmarshal(content, %Master{})
      assert manifest.version == version
    end

    test "parses manifest without version as version 1" do
      content = """
      #EXTM3U
      """

      manifest = Playlist.unmarshal(content, %Master{})
      assert manifest.version == 1
    end

    test "collects all variant streams" do
      content = """
      #EXTM3U
      #EXT-X-VERSION:7
      #EXT-X-STREAM-INF:BANDWIDTH=1187651,CODECS="avc1.42e00a"
      muxed_video_480x270.m3u8
      #EXT-X-STREAM-INF:BANDWIDTH=609514,CODECS="avc1.42e00a"
      muxed_video_540x360.m3u8
      #EXT-X-STREAM-INF:BANDWIDTH=863865,CODECS="avc1.42e00a"
      muxed_video_720x480.m3u8
      """

      manifest = Playlist.unmarshal(content, %Master{})
      assert Enum.count(Master.variant_streams(manifest)) == 3
    end

    test "collects variant stream configuration" do
      content = """
      #EXTM3U
      #EXT-X-VERSION:3
      #EXT-X-STREAM-INF:BANDWIDTH=1478400,AVERAGE-BANDWIDTH=1425600,CODECS="avc1.4d4029,mp4a.40.2",RESOLUTION=854x480,FRAME-RATE=30.000
      stream_854x480.m3u8
      """

      manifest = Playlist.unmarshal(content, %Master{})
      assert %VariantStream{} = stream = List.first(Master.variant_streams(manifest))

      [
        uri: %URI{path: "stream_854x480.m3u8"},
        bandwidth: 1_478_400,
        average_bandwidth: 1_425_600,
        codecs: ["avc1.4d4029", "mp4a.40.2"],
        resolution: {854, 480},
        frame_rate: 30.0
      ]
      |> Enum.each(fn {key, val} ->
        have = Map.get(stream, key)

        assert have == val,
               "expected #{inspect(val)} on key #{inspect(key)}, have #{inspect(have)}"
      end)
    end

    test "handles integer frame-rate values" do
      content = """
      #EXTM3U
      #EXT-X-VERSION:3
      #EXT-X-STREAM-INF:BANDWIDTH=1478400,AVERAGE-BANDWIDTH=1425600,CODECS="avc1.4d4029,mp4a.40.2",RESOLUTION=854x480,FRAME-RATE=30
      stream_854x480.m3u8
      """

      manifest = Playlist.unmarshal(content, %Master{})
      assert %VariantStream{} = stream = List.first(Master.variant_streams(manifest))

      [
        uri: %URI{path: "stream_854x480.m3u8"},
        bandwidth: 1_478_400,
        average_bandwidth: 1_425_600,
        codecs: ["avc1.4d4029", "mp4a.40.2"],
        resolution: {854, 480},
        frame_rate: 30.0
      ]
      |> Enum.each(fn {key, val} ->
        have = Map.get(stream, key)

        assert have == val,
               "expected #{inspect(val)} on key #{inspect(key)}, have #{inspect(have)}"
      end)
    end

    test "handels complex uri specifications" do
      content = """
      #EXTM3U
      #EXT-X-VERSION:3
      #EXT-X-STREAM-INF:BANDWIDTH=1478400,AVERAGE-BANDWIDTH=1425600,CODECS="avc1.4d4029,mp4a.40.2",RESOLUTION=854x480,FRAME-RATE=30.000
      stream_with_token.m3u8?t=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE2NTc5MTYzMDcsImlhdCI6MTY1Nzg3MzEwNywiaXNzIjoiY2RwIiwia2VlcF9zZWdtZW50cyI6bnVsbCwia2luZCI6ImNoaWxkIiwicGFyZW50IjoiNmhReUhyUGRhRTNuL3N0cmVhbS5tM3U4Iiwic3ViIjoiNmhReUhyUGRhRTNuL3N0cmVhbV82NDB4MzYwXzgwMGsubTN1OCIsInRyaW1fZnJvbSI6NTIxLCJ0cmltX3RvIjpudWxsLCJ1c2VyX2lkIjoiMzA2IiwidXVpZCI6bnVsbCwidmlzaXRvcl9pZCI6ImI0NGFlZjYyLTA0MTYtMTFlZC04NTRmLTBhNThhOWZlYWMwMiJ9.eVrBzEBbjHxDcg6xnZXfXy0ZoNoj_seaZwaja_WDwuc
      """

      manifest = Playlist.unmarshal(content, %Master{})
      stream = List.first(Master.variant_streams(manifest))

      assert stream.uri == %URI{
               path: "stream_with_token.m3u8",
               query:
                 "t=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE2NTc5MTYzMDcsImlhdCI6MTY1Nzg3MzEwNywiaXNzIjoiY2RwIiwia2VlcF9zZWdtZW50cyI6bnVsbCwia2luZCI6ImNoaWxkIiwicGFyZW50IjoiNmhReUhyUGRhRTNuL3N0cmVhbS5tM3U4Iiwic3ViIjoiNmhReUhyUGRhRTNuL3N0cmVhbV82NDB4MzYwXzgwMGsubTN1OCIsInRyaW1fZnJvbSI6NTIxLCJ0cmltX3RvIjpudWxsLCJ1c2VyX2lkIjoiMzA2IiwidXVpZCI6bnVsbCwidmlzaXRvcl9pZCI6ImI0NGFlZjYyLTA0MTYtMTFlZC04NTRmLTBhNThhOWZlYWMwMiJ9.eVrBzEBbjHxDcg6xnZXfXy0ZoNoj_seaZwaja_WDwuc"
             }
    end

    test "collects and aggregates alternative subtitle rendition" do
      content = """
      #EXTM3U
      #EXT-X-VERSION:3
      #EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="subtitles",NAME="German (Germany)",DEFAULT=NO,AUTOSELECT=NO,FORCED=NO,LANGUAGE="de-DE",URI="subtitles.m3u8?t=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE2NTc5MTY0MjYsImlhdCI6MTY1Nzg3MzIyNiwiaXNzIjoiY2RwIiwia2VlcF9zZWdtZW50cyI6bnVsbCwia2luZCI6ImNoaWxkIiwicGFyZW50IjoiNmhReUhyUGRhRTNuL3N0cmVhbS5tM3U4Iiwic3ViIjoiNmhReUhyUGRhRTNuL3N1YnRpdGxlcy5tM3U4IiwidHJpbV9mcm9tIjo1MjEsInRyaW1fdG8iOm51bGwsInVzZXJfaWQiOiIzMDYiLCJ1dWlkIjpudWxsLCJ2aXNpdG9yX2lkIjoiZmI0NDRlYjgtMDQxNi0xMWVkLTgxODAtMGE1OGE5ZmVhYzAyIn0.hZBdfremVP_T7XRcVLz-vmDfgyP_sXZhyK_liv4ekho"
      #EXT-X-STREAM-INF:BANDWIDTH=299147,AVERAGE-BANDWIDTH=290400,CODECS="avc1.66.30,mp4a.40.2",RESOLUTION=416x234,FRAME-RATE=14.985,AUDIO="PROGRAM_AUDIO",SUBTITLES="subtitles"
      stream_854x480.m3u8
      """

      manifest = Playlist.unmarshal(content, %Master{})

      assert [%AlternativeRendition{} = rendition] =
               manifest.alternative_renditions

      [
        group_id: "subtitles",
        name: "German (Germany)",
        default: false,
        autoselect: false,
        forced: false,
        language: "de-DE",
        uri: %URI{
          path: "subtitles.m3u8",
          query:
            "t=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE2NTc5MTY0MjYsImlhdCI6MTY1Nzg3MzIyNiwiaXNzIjoiY2RwIiwia2VlcF9zZWdtZW50cyI6bnVsbCwia2luZCI6ImNoaWxkIiwicGFyZW50IjoiNmhReUhyUGRhRTNuL3N0cmVhbS5tM3U4Iiwic3ViIjoiNmhReUhyUGRhRTNuL3N1YnRpdGxlcy5tM3U4IiwidHJpbV9mcm9tIjo1MjEsInRyaW1fdG8iOm51bGwsInVzZXJfaWQiOiIzMDYiLCJ1dWlkIjpudWxsLCJ2aXNpdG9yX2lkIjoiZmI0NDRlYjgtMDQxNi0xMWVkLTgxODAtMGE1OGE5ZmVhYzAyIn0.hZBdfremVP_T7XRcVLz-vmDfgyP_sXZhyK_liv4ekho"
        }
      ]
      |> Enum.each(fn {key, val} ->
        assert Map.get(rendition, key) == val, "expected #{inspect(val)} on key #{inspect(key)}"
      end)
    end
  end

  describe "Unmarshal Media Playlist" do
    test "fails with empty content" do
      [
        fn -> Playlist.unmarshal("", %Media{}) end,
        fn -> Playlist.unmarshal("some invalid content", %Media{}) end
      ]
      |> Enum.each(fn t -> assert_raise ArgumentError, t end)
    end

    test "collects manifest header" do
      version = 3
      duration = 7
      sequence = 662

      content = """
      #EXTM3U
      #EXT-X-VERSION:#{version}
      #EXT-X-TARGETDURATION:#{duration}
      #EXT-X-MEDIA-SEQUENCE:#{sequence}
      #EXTINF:6.00000,
      a/stream_1280x720/00000/stream_1280x720_00662.ts?t=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE2NTgwMjYwMjIsImlhdCI6MTY1Nzk2NzgyMiwiaXNzIjoiY2RwIiwic3ViIjoiZzM5azZLSjNLZ1UwL2Evc3RyZWFtXzEyODB4NzIwIiwidXNlcl9pZCI6IjEiLCJ2aXNpdG9yX2lkIjoiM2FhNjY1MGEtMDRmMy0xMWVkLWIzOGYtMGE1OGE5ZmVhYzAyIn0.DNMBbZPLE0yc0GnGjV5hG_eX_uQ5hzriLk0ZPe8w2AI
      #EXTINF:6.00000,
      a/stream_1280x720/00000/stream_1280x720_00663.ts?t=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE2NTgwMjYwMjIsImlhdCI6MTY1Nzk2NzgyMiwiaXNzIjoiY2RwIiwic3ViIjoiZzM5azZLSjNLZ1UwL2Evc3RyZWFtXzEyODB4NzIwIiwidXNlcl9pZCI6IjEiLCJ2aXNpdG9yX2lkIjoiM2FhNjY1MGEtMDRmMy0xMWVkLWIzOGYtMGE1OGE5ZmVhYzAyIn0.DNMBbZPLE0yc0GnGjV5hG_eX_uQ5hzriLk0ZPe8w2AI
      """

      manifest = Playlist.unmarshal(content, %Media{})
      assert manifest.version == version
      assert manifest.target_segment_duration == duration
      assert manifest.media_sequence_number == sequence
      refute manifest.finished
    end

    test "collects segments" do
      content = """
      #EXTM3U
      #EXT-X-VERSION:7
      #EXT-X-TARGETDURATION:3
      #EXT-X-MEDIA-SEQUENCE:0
      #EXTINF:2.020136054,
      audio_segment_0_audio_track.m4s
      #EXTINF:2.020136054,
      audio_segment_1_audio_track.m4s
      #EXTINF:2.020136054,
      audio_segment_2_audio_track.m4s
      #EXTINF:2.020136054,
      audio_segment_3_audio_track.m4s
      #EXTINF:1.95047619,
      audio_segment_4_audio_track.m4s
      """

      manifest = Playlist.unmarshal(content, %Media{})
      assert Enum.count(Media.segments(manifest)) == 5
    end

    test "detects when track is finished" do
      content = """
      #EXTM3U
      #EXT-X-VERSION:7
      #EXT-X-TARGETDURATION:10
      #EXT-X-MEDIA-SEQUENCE:0
      #EXTINF:10.0,
      video_segment_0_video_track.ts
      #EXTINF:2.0,
      video_segment_1_video_track.ts
      #EXT-X-ENDLIST
      """

      manifest = Playlist.unmarshal(content, %Media{})
      assert manifest.finished
    end

    test "reads media playlist type" do
      content = """
      #EXTM3U
      #EXT-X-VERSION:7
      #EXT-X-PLAYLIST-TYPE:VOD
      #EXT-X-TARGETDURATION:10
      #EXT-X-MEDIA-SEQUENCE:0
      #EXTINF:10.0,
      video_segment_0_video_track.ts
      """

      manifest = Playlist.unmarshal(content, %Media{})
      assert manifest.type == :vod

      content = """
      #EXTM3U
      #EXT-X-VERSION:7
      #EXT-X-PLAYLIST-TYPE:EVENT
      #EXT-X-TARGETDURATION:10
      #EXT-X-MEDIA-SEQUENCE:0
      #EXTINF:10.0,
      video_segment_0_video_track.ts
      """

      manifest = Playlist.unmarshal(content, %Media{})
      assert manifest.type == :event
    end

    test "collects segment tags" do
      content = """
      #EXTM3U
      #EXT-X-VERSION:7
      #EXT-X-TARGETDURATION:10
      #EXT-X-MEDIA-SEQUENCE:0
      #EXTINF:9.56,
      video_segment_0_video_track.ts
      #EXTINF:2.020136054,
      a/stream_1280x720_3300k/00000/stream_1280x720_3300k_00522.ts?t=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE2NTc5MTYzMDEsImlhdCI6MTY1Nzg3MzEwMSwiaXNzIjoiY2RwIiwic3ViIjoiNmhReUhyUGRhRTNuL2Evc3RyZWFtXzEyODB4NzIwXzMzMDBrIiwidXNlcl9pZCI6IjMwNiIsInZpc2l0b3JfaWQiOiJiMGMyMGVkZS0wNDE2LTExZWQtYTYyMS0wYTU4YTlmZWFjMDIifQ.Fj7CADyZeoWtpaqiZLPodNHMWhlGeKjxLwpMR7lygqk
      """

      manifest = Playlist.unmarshal(content, %Media{})
      last = List.last(Media.segments(manifest))
      assert last.duration == 2.020136054
      assert last.uri.path == "a/stream_1280x720_3300k/00000/stream_1280x720_3300k_00522.ts"

      assert last.uri.query ==
               "t=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE2NTc5MTYzMDEsImlhdCI6MTY1Nzg3MzEwMSwiaXNzIjoiY2RwIiwic3ViIjoiNmhReUhyUGRhRTNuL2Evc3RyZWFtXzEyODB4NzIwXzMzMDBrIiwidXNlcl9pZCI6IjMwNiIsInZpc2l0b3JfaWQiOiJiMGMyMGVkZS0wNDE2LTExZWQtYTYyMS0wYTU4YTlmZWFjMDIifQ.Fj7CADyZeoWtpaqiZLPodNHMWhlGeKjxLwpMR7lygqk"
    end

    test "recognizes discontinuity tag" do
      content = """
      #EXTM3U
      #EXT-X-VERSION:7
      #EXT-X-TARGETDURATION:10
      #EXT-X-MEDIA-SEQUENCE:0
      #EXTINF:10.0,
      video_segment_0_video_track.ts
      #EXT-X-DISCONTINUITY
      #EXTINF:2.0,
      video_segment_1_video_track.ts
      #EXTINF:3.0,
      video_segment_2_video_track.ts
      #EXT-X-ENDLIST
      """

      manifest = Playlist.unmarshal(content, %Media{})
      segments = Media.segments(manifest)
      first = Enum.at(segments, 0)
      second = Enum.at(segments, 1)
      third = Enum.at(segments, 2)

      assert first.discontinuity == false
      assert second.discontinuity == true
      assert third.discontinuity == false
    end

    test "recognizes EXT-X-MAP tag" do
      content = """
      #EXTM3U
      #EXT-X-VERSION:7
      #EXT-X-TARGETDURATION:5
      #EXT-X-MEDIA-SEQUENCE:0
      #EXT-X-DISCONTINUITY-SEQUENCE:0
      #EXT-X-MAP:URI="muxed_header_video_track_part_0.mp4"
      #EXT-X-PROGRAM-DATE-TIME:2024-08-19T12:08:16.015Z
      #EXTINF:4.026666666,
      muxed_segment_0_video_track.m4s
      #EXT-X-PROGRAM-DATE-TIME:2024-08-19T12:08:16.781Z
      #EXTINF:3.994666667,
      muxed_segment_1_video_track.m4s
      #EXT-X-MAP:URI="muxed_header_video_track_part_1.mp4"
      #EXTINF:3.994666667,
      muxed_segment_1_video_track.m4s
      """

      manifest = Playlist.unmarshal(content, %Media{})
      segments = Media.segments(manifest)
      first = Enum.at(segments, 0)
      second = Enum.at(segments, 1)
      third = Enum.at(segments, 2)

      assert first.init_section == %{uri: "muxed_header_video_track_part_0.mp4"}
      assert second.init_section == %{uri: "muxed_header_video_track_part_0.mp4"}
      assert third.init_section == %{uri: "muxed_header_video_track_part_1.mp4"}

      # Test program date time parsing
      {:ok, expected_first_datetime, _} = DateTime.from_iso8601("2024-08-19T12:08:16.015Z")
      {:ok, expected_second_datetime, _} = DateTime.from_iso8601("2024-08-19T12:08:16.781Z")

      assert first.program_date_time == expected_first_datetime
      assert second.program_date_time == expected_second_datetime
      assert third.program_date_time == nil
    end

    test "recognizes byteranges in EXT-X-MAP and EXT-X-BYTERANGE tags" do
      content = """
      #EXTM3U
      #EXT-X-TARGETDURATION:6
      #EXT-X-VERSION:7
      #EXT-X-MEDIA-SEQUENCE:1
      #EXT-X-PLAYLIST-TYPE:VOD
      #EXT-X-INDEPENDENT-SEGMENTS
      #EXT-X-MAP:URI="main.mp4",BYTERANGE="719@0"
      #EXTINF:6.00000,	
      #EXT-X-BYTERANGE:1508000@719
      main.mp4
      #EXTINF:6.00000,	
      #EXT-X-BYTERANGE:1510244@1508719
      main.mp4
      """

      manifest = Playlist.unmarshal(content, %Media{})
      segments = Media.segments(manifest)
      first = Enum.at(segments, 0)
      second = Enum.at(segments, 1)

      assert first.init_section == %{uri: "main.mp4", byterange: %{length: 719, offset: 0}}
      assert first.byterange == %{length: 1_508_000, offset: 719}
      assert second.init_section == %{uri: "main.mp4", byterange: %{length: 719, offset: 0}}
      assert second.byterange == %{length: 1_510_244, offset: 1_508_719}
    end
  end

  describe "build_absolute_uri/2" do
    test "when the child is a relative path" do
      master = URI.new!("https://v.t/4fafW0nnol6i/stream.m3u8")
      media = URI.new!("stream_Afg.m3u8")
      expected = URI.new!("https://v.t/4fafW0nnol6i/stream_Afg.m3u8")
      assert Playlist.build_absolute_uri(master, media) == expected
    end

    test "when the child is an absolute uri" do
      master = URI.new!("https://v.t/4fafW0nnol6i/stream.m3u8")
      media = URI.new!("https://v.t/4fafW0nnol6i/stream_Afg.m3u8")
      expected = URI.new!("https://v.t/4fafW0nnol6i/stream_Afg.m3u8")
      assert Playlist.build_absolute_uri(master, media) == expected

      media = URI.new!("https://a.t/abc/stream_Afg.m3u8")
      expected = URI.new!("https://a.t/abc/stream_Afg.m3u8")
      assert Playlist.build_absolute_uri(master, media) == expected
    end

    test "bbc" do
      master =
        URI.new!(
          "http://a.files.bbci.co.uk/media/live/manifesto/audio/simulcast/hls/nonuk/sbr_low/ak/bbc_world_service.m3u8"
        )

      media =
        URI.new!(
          "http://as-hls-ww-live.akamaized.net/pool_904/live/ww/bbc_world_service/bbc_world_service.isml/bbc_world_service-audio%3d96000.norewind.m3u8"
        )

      assert Playlist.build_absolute_uri(master, media) == media

      segment = URI.new!("bbc_world_service-audio=96000-268386135.ts")

      expected =
        URI.new!(
          "http://as-hls-ww-live.akamaized.net/pool_904/live/ww/bbc_world_service/bbc_world_service.isml/bbc_world_service-audio=96000-268386135.ts"
        )

      assert Playlist.build_absolute_uri(media, segment) == expected
    end
  end

  describe "extract_relative_uri/2" do
    test "when child is realtive to master" do
      master = URI.new!("https://v.t/4fafW0nnol6i/stream.m3u8")
      media = URI.new!("https://v.t/4fafW0nnol6i/stream_Afg.m3u8")
      expected = URI.new!("stream_Afg.m3u8")
      assert Playlist.extract_relative_uri(master, media) == expected
    end

    test "when child is absolute" do
      master = URI.new!("https://v.t/4fafW0nnol6i/stream.m3u8")
      media = URI.new!("https://a.t/abc/stream_Afg.m3u8")
      expected = URI.new!("https://a.t/abc/stream_Afg.m3u8")
      assert Playlist.extract_relative_uri(master, media) == expected
    end
  end
end
