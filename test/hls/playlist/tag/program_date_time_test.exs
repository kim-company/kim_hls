defmodule HLS.Playlist.Tag.ProgramDateTimeTest do
  use ExUnit.Case
  alias HLS.Playlist.Tag.ProgramDateTime

  describe "unmarshal/1" do
    test "parses ISO 8601 date-time with UTC timezone" do
      line = "#EXT-X-PROGRAM-DATE-TIME:2024-08-19T12:08:16.015Z"
      result = ProgramDateTime.unmarshal(line)

      assert %DateTime{} = result
      assert result.year == 2024
      assert result.month == 8
      assert result.day == 19
      assert result.hour == 12
      assert result.minute == 8
      assert result.second == 16
      assert result.microsecond == {15000, 3}
      assert result.time_zone == "Etc/UTC"
    end

    test "parses ISO 8601 date-time with positive timezone offset" do
      line = "#EXT-X-PROGRAM-DATE-TIME:2024-08-19T14:08:16.015+02:00"
      result = ProgramDateTime.unmarshal(line)

      assert %DateTime{} = result
      # Should be converted to UTC
      assert result.year == 2024
      assert result.month == 8
      assert result.day == 19
      # 14:08 +02:00 = 12:08 UTC
      assert result.hour == 12
      assert result.minute == 8
      assert result.second == 16
    end

    test "parses ISO 8601 date-time with negative timezone offset" do
      line = "#EXT-X-PROGRAM-DATE-TIME:2024-08-19T07:08:16.015-05:00"
      result = ProgramDateTime.unmarshal(line)

      assert %DateTime{} = result
      # Should be converted to UTC
      assert result.year == 2024
      assert result.month == 8
      assert result.day == 19
      # 07:08 -05:00 = 12:08 UTC
      assert result.hour == 12
      assert result.minute == 8
      assert result.second == 16
    end

    test "parses ISO 8601 date-time without fractional seconds" do
      line = "#EXT-X-PROGRAM-DATE-TIME:2024-08-19T12:08:16Z"
      result = ProgramDateTime.unmarshal(line)

      assert %DateTime{} = result
      assert result.microsecond == {0, 0}
    end

    test "parses ISO 8601 date-time with millisecond precision" do
      line = "#EXT-X-PROGRAM-DATE-TIME:2024-08-19T12:08:16.123Z"
      result = ProgramDateTime.unmarshal(line)

      assert %DateTime{} = result
      assert result.microsecond == {123_000, 3}
    end

    test "parses ISO 8601 date-time with microsecond precision" do
      line = "#EXT-X-PROGRAM-DATE-TIME:2024-08-19T12:08:16.123456Z"
      result = ProgramDateTime.unmarshal(line)

      assert %DateTime{} = result
      assert result.microsecond == {123_456, 6}
    end

    test "raises ArgumentError for invalid date-time format" do
      line = "#EXT-X-PROGRAM-DATE-TIME:invalid-date"

      assert_raise ArgumentError, ~r/Invalid ISO 8601 date-time format/, fn ->
        ProgramDateTime.unmarshal(line)
      end
    end

    test "raises ArgumentError for malformed date" do
      line = "#EXT-X-PROGRAM-DATE-TIME:2024-13-32T25:70:70Z"

      assert_raise ArgumentError, ~r/Invalid ISO 8601 date-time format/, fn ->
        ProgramDateTime.unmarshal(line)
      end
    end
  end

  describe "marshal_datetime/1" do
    test "marshals DateTime back to ISO 8601 format" do
      {:ok, datetime, _} = DateTime.from_iso8601("2024-08-19T12:08:16.015Z")
      result = ProgramDateTime.marshal_datetime(datetime)

      assert result == "2024-08-19T12:08:16.015Z"
    end

    test "marshals DateTime with timezone offset" do
      {:ok, datetime, _} = DateTime.from_iso8601("2024-08-19T14:08:16.015+02:00")
      result = ProgramDateTime.marshal_datetime(datetime)

      # Should be converted to UTC when marshaling
      assert result == "2024-08-19T12:08:16.015Z"
    end

    test "marshals DateTime without fractional seconds" do
      {:ok, datetime, _} = DateTime.from_iso8601("2024-08-19T12:08:16Z")
      result = ProgramDateTime.marshal_datetime(datetime)

      assert result == "2024-08-19T12:08:16Z"
    end
  end

  describe "tag behavior" do
    test "has correct tag id" do
      assert ProgramDateTime.id() == :ext_x_program_date_time
    end

    test "is not multiline" do
      assert ProgramDateTime.is_multiline?() == false
    end

    test "does not have URI" do
      assert ProgramDateTime.has_uri?() == false
    end

    test "matches correct tag prefix" do
      assert ProgramDateTime.match?("#EXT-X-PROGRAM-DATE-TIME:2024-08-19T12:08:16.015Z") == true
      assert ProgramDateTime.match?("#EXT-X-DISCONTINUITY") == false
      assert ProgramDateTime.match?("#EXTINF:3.0,") == false
    end
  end
end
