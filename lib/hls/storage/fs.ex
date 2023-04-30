defmodule HLS.Storage.FS do
  defstruct []

  def new() do
    %__MODULE__{}
  end
end

defimpl HLS.Storage, for: HLS.Storage.FS do
  @impl true
  def read(_, %URI{path: path}, _), do: File.read(path)

  @impl true
  def exists?(_, %URI{path: path}), do: File.exists?(path)

  @impl true
  def write(_, %URI{path: path}, data, _opts), do: File.write!(path, data)
end
