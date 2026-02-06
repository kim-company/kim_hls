defmodule HLS.Playlist.Tag.KeyTest do
  use ExUnit.Case, async: true

  alias HLS.Playlist.Tag
  alias HLS.Playlist.Tag.Key

  describe "tag behaviour" do
    test "has correct tag id" do
      assert Key.id() == :ext_x_key
    end

    test "is not multiline" do
      refute Key.is_multiline?()
    end

    test "does not have URI" do
      refute Key.has_uri?()
    end

    test "matches correct tag prefix" do
      assert Key.match?("#EXT-X-KEY:METHOD=AES-128")
      assert Key.match?("#EXT-X-KEY:METHOD=NONE")
      refute Key.match?("#EXT-X-MAP:URI=init.mp4")
      refute Key.match?("#EXTINF:6.0,")
    end

    test "has correct classification" do
      assert Tag.class_from_id(:ext_x_key) == :media_segment
    end
  end

  describe "unmarshal/1" do
    test "parses METHOD only" do
      result = Key.unmarshal("#EXT-X-KEY:METHOD=NONE")
      assert result == %{method: "NONE"}
    end

    test "parses METHOD and URI" do
      result = Key.unmarshal(~s/#EXT-X-KEY:METHOD=AES-128,URI="key.bin"/)
      assert result.method == "AES-128"
      assert result.uri == "key.bin"
    end

    test "parses all attributes" do
      line = ~s/#EXT-X-KEY:METHOD=AES-128,URI="key.bin",IV=0x01,KEYFORMAT="identity",KEYFORMATVERSIONS="1"/
      result = Key.unmarshal(line)
      assert result.method == "AES-128"
      assert result.uri == "key.bin"
      assert result.iv == "0x01"
      assert result.keyformat == "identity"
      assert result.keyformatversions == "1"
    end
  end

  describe "marshal_attributes/1" do
    test "formats all fields" do
      attrs = %{
        method: "AES-128",
        uri: "key.bin",
        iv: "0x01",
        keyformat: "identity",
        keyformatversions: "1"
      }

      result = Key.marshal_attributes(attrs)
      assert result =~ "METHOD=AES-128"
      assert result =~ ~s(URI="key.bin")
      assert result =~ "IV=0x01"
      assert result =~ ~s(KEYFORMAT="identity")
      assert result =~ ~s(KEYFORMATVERSIONS="1")
    end

    test "omits nil fields" do
      result = Key.marshal_attributes(%{method: "NONE"})
      assert result == "METHOD=NONE"
      refute result =~ "URI"
      refute result =~ "IV"
    end

    test "accepts keyword list (via Enum.into)" do
      result = Key.marshal_attributes([method: "AES-128", uri: "k.bin"])
      assert result =~ "METHOD=AES-128"
      assert result =~ ~s(URI="k.bin")
    end
  end
end
