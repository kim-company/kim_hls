defmodule HLS.Playlist.Tag.ProgramDateTime do
  @moduledoc """
  EXT-X-PROGRAM-DATE-TIME tag implementation.

  Associates the first sample of a Media Segment with an absolute date and/or time.
  Uses ISO/IEC 8601:2004 date/time representation with optional timezone and
  fractional seconds.

  See RFC 8216, section 4.3.2.6
  """

  use HLS.Playlist.Tag, id: :ext_x_program_date_time

  @impl true
  def unmarshal(data) do
    capture_value!(data, ~s/.*/, fn raw ->
      case DateTime.from_iso8601(String.trim(raw)) do
        {:ok, datetime, _offset} ->
          datetime

        {:error, reason} ->
          raise ArgumentError,
                "Invalid ISO 8601 date-time format: #{inspect(raw)}, reason: #{inspect(reason)}"
      end
    end)
  end

  @impl true
  def init(%DateTime{} = datetime, sequence) do
    %HLS.Playlist.Tag{
      id: id(),
      class: HLS.Playlist.Tag.class_from_id(id()),
      sequence: sequence,
      value: datetime
    }
  end

  @doc """
  Marshals a DateTime back to ISO 8601 format for playlist output.
  """
  def marshal_datetime(%DateTime{} = datetime) do
    DateTime.to_iso8601(datetime)
  end
end
