defmodule HLS.Storage do
  alias HLS.Playlist
  alias HLS.Playlist.{Master, Media}
  alias HLS.Storage.Driver

  @opaque t :: %__MODULE__{driver: Driver.t()}
  defstruct [:driver]

  def new(url = "http" <> _rest) do
    %__MODULE__{driver: HLS.Storage.HTTP.new(url)}
  end

  def new(path) when is_binary(path) do
    %__MODULE__{driver: HLS.Storage.FS.new(path)}
  end

  @spec get_master_playlist(t) :: {:ok, Master.t()} | Driver.error_t()
  def get_master_playlist(%__MODULE__{driver: driver}) do
    with {:ok, content} <- Driver.get(driver) do
      {:ok, Playlist.unmarshal(content, %Master{})}
    end
  end

  @spec get_master_playlist!(t) :: Master.t()
  def get_master_playlist!(storage) do
    {:ok, playlist} = get_master_playlist(storage)
    playlist
  end

  @spec get_media_playlist(t, URI.t()) :: {:ok, Media.t()} | Driver.error_t()
  def get_media_playlist(%__MODULE__{driver: driver}, uri) do
    with {:ok, content} <- Driver.get(driver, uri) do
      {:ok, Playlist.unmarshal(content, %Media{})}
    end
  end

  @spec get_media_playlist!(t, URI.t()) :: Media.t()
  def get_media_playlist!(storage, uri) do
    {:ok, playlist} = get_media_playlist(storage, uri)
    playlist
  end

  @spec get_segment(t, URI.t()) :: Driver.callback_result_t()
  def get_segment(%__MODULE__{driver: driver}, uri), do: Driver.get(driver, uri)

  @spec get_segment!(t, URI.t()) :: Driver.content_t()
  def get_segment!(storage, uri) do
    {:ok, content} = get_segment(storage, uri)
    content
  end

  @spec ready?(t) :: boolean()
  def ready?(%__MODULE__{driver: driver}), do: Driver.ready?(driver)
end
