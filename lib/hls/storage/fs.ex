defmodule HLS.Storage.FS do
  @behaviour HLS.Storage

  @enforce_keys [:location]
  defstruct @enforce_keys ++ [:dirname, :basename]

  @impl true
  def init(config = %__MODULE__{location: location}) do
    basename = Path.basename(location)
    dirname = Path.dirname(location)
    %__MODULE__{config | basename: basename, dirname: dirname}
  end

  @impl true
  def get(%__MODULE__{dirname: dir, basename: manifest}), do: load([dir, manifest])

  @impl true
  def get(%__MODULE__{dirname: dir}, %URI{path: rel}), do: load([dir, rel])

  @impl true
  def ready?(%__MODULE__{dirname: dir, basename: manifest}) do
    dir
    |> Path.join(manifest)
    |> File.exists?()
  end

  defp load(path) when is_list(path) do
    path
    |> Path.join()
    |> load()
  end

  defp load(path) when is_binary(path) do
    File.read(path)
  end
end
