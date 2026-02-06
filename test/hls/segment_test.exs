defmodule HLS.SegmentTest do
  use ExUnit.Case

  alias HLS.{Playlist, Segment}
  alias HLS.Playlist.{Media, Tag}

  test "encodes a valid Segment" do
  end

  describe "Segment.from accumulates across discontinuities" do
    test "from does not reset at discontinuity boundary" do
      content = """
      #EXTM3U
      #EXT-X-VERSION:7
      #EXT-X-TARGETDURATION:6
      #EXT-X-MEDIA-SEQUENCE:0
      #EXT-X-DISCONTINUITY-SEQUENCE:0
      #EXTINF:4.00000,
      seg0.ts
      #EXTINF:5.00000,
      seg1.ts
      #EXT-X-DISCONTINUITY
      #EXTINF:3.00000,
      seg2.ts
      #EXTINF:6.00000,
      seg3.ts
      """

      playlist = Playlist.unmarshal(content, %Media{})
      [s0, s1, s2, s3] = playlist.segments

      assert s0.from == 0
      assert s1.from == 4.0
      # Does NOT reset at discontinuity — accumulates through
      assert s2.from == 9.0
      assert s3.from == 12.0
    end

    test "from starts at 0 for the first segment" do
      content = """
      #EXTM3U
      #EXT-X-VERSION:7
      #EXT-X-TARGETDURATION:6
      #EXT-X-MEDIA-SEQUENCE:100
      #EXTINF:6.0,
      seg100.ts
      """

      playlist = Playlist.unmarshal(content, %Media{})
      [seg] = playlist.segments
      assert seg.from == 0
    end

    test "from with high-precision durations accumulates correctly" do
      content = """
      #EXTM3U
      #EXT-X-VERSION:7
      #EXT-X-TARGETDURATION:6
      #EXT-X-MEDIA-SEQUENCE:0
      #EXTINF:2.020136054,
      seg0.ts
      #EXTINF:3.003000000,
      seg1.ts
      #EXTINF:2.020136054,
      seg2.ts
      """

      playlist = Playlist.unmarshal(content, %Media{})
      [s0, s1, s2] = playlist.segments

      assert s0.from == 0
      assert_in_delta s1.from, 2.020136054, 1.0e-9
      assert_in_delta s2.from, 2.020136054 + 3.003000000, 1.0e-9
    end
  end

  describe "Segment.from_tags/1" do
    test "raises when tags belong to different sequences" do
      tags = [
        %Tag{id: :extinf, class: :media_segment, sequence: 0, value: 6.0},
        %Tag{id: :uri, class: :media_segment, sequence: 1, value: URI.new!("seg.ts")}
      ]

      assert_raise ArgumentError, ~r/different sequence windows/, fn ->
        Segment.from_tags(tags)
      end
    end
  end
end
