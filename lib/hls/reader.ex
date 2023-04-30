defmodule HLS.Reader do
  alias HLS.Playlist
  alias HLS.Playlist.{Master, Media}
  alias HLS.Storage

  @opaque t :: %__MODULE__{storage: Storage.t()}
  defstruct [:storage, :master_playlist_uri]

  def new(uri = %URI{scheme: "http"}) do
    %__MODULE__{storage: HLS.Storage.HTTP.new(), master_playlist_uri: uri}
  end

  def new(uri = %URI{scheme: nil}) do
    %__MODULE__{storage: HLS.Storage.FS.new(), master_playlist_uri: uri}
  end

  def ready?(%__MODULE__{storage: storage, master_playlist_uri: uri}) do
    Storage.exists?(storage, uri)
  end

  @spec read_master_playlist(t) :: {:ok, Master.t()} | {:error, any()}
  def read_master_playlist(%__MODULE__{storage: storage, master_playlist_uri: uri}) do
    with {:ok, content} <- Storage.read(storage, uri) do
      {:ok, Playlist.unmarshal(content, %Master{})}
    end
  end

  @spec read_master_playlist!(t) :: Master.t()
  def read_master_playlist!(storage) do
    {:ok, playlist} = read_master_playlist(storage)
    playlist
  end

  def expand_relative_path(%URI{path: base_path}, uri = %URI{path: rel}) do
    %URI{uri | path: Path.join([Path.dirname(base_path), rel])}
  end

  @spec read_media_playlist(t, URI.t()) :: {:ok, Media.t()} | {:error, any()}
  def read_media_playlist(
        %__MODULE__{storage: storage, master_playlist_uri: master_uri},
        relative_uri
      ) do
    uri = expand_relative_path(master_uri, relative_uri)

    with {:ok, content} <- Storage.read(storage, uri) do
      {:ok, Playlist.unmarshal(content, %Media{})}
    end
  end

  @spec read_media_playlist!(t, URI.t()) :: Media.t()
  def read_media_playlist!(storage, uri) do
    {:ok, playlist} = read_media_playlist(storage, uri)
    playlist
  end

  @spec read_segment(t, URI.t()) :: {:ok, binary()} | {:error, any()}
  def read_segment(%__MODULE__{storage: storage, master_playlist_uri: master_uri}, relative_uri) do
    uri = expand_relative_path(master_uri, relative_uri)
    Storage.read(storage, uri)
  end

  @spec read_segment!(t, URI.t()) :: binary
  def read_segment!(storage, uri) do
    {:ok, content} = read_segment(storage, uri)
    content
  end
end
