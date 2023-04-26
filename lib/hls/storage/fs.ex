defmodule HLS.Storage.FS do
  defstruct [:location, :dirname, :basename]

  def new(location) do
    basename = Path.basename(location)
    dirname = Path.dirname(location)
    %__MODULE__{basename: basename, dirname: dirname}
  end
end

defimpl HLS.Storage.Driver, for: HLS.Storage.FS do
  @impl true
  def get(%HLS.Storage.FS{dirname: dir, basename: manifest}), do: load([dir, manifest])

  @impl true
  def get(%HLS.Storage.FS{dirname: dir}, %URI{path: rel}), do: load([dir, rel])

  @impl true
  def ready?(%HLS.Storage.FS{dirname: dir, basename: manifest}) do
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
