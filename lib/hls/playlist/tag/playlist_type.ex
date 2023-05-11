defmodule HLS.Playlist.Tag.PlaylistType do
  use HLS.Playlist.Tag, id: :ext_x_playlist_type

  @type type :: :event | :vod

  @impl true
  def unmarshal(data), do: capture_value!(data, ~s/\\S+/, &parse_playlist_type/1)

  defp parse_playlist_type("EVENT"), do: :event
  defp parse_playlist_type("VOD"), do: :vod

  def marshal_playlist_type(:event), do: "EVENT"
  def marshal_playlist_type(:vod), do: "VOD"
end
