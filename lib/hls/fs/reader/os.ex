defmodule HLS.FS.OS do
  defstruct []

  def new() do
    %__MODULE__{}
  end
end

defimpl HLS.FS.Reader, for: HLS.FS.OS do
  @impl true
  def read(_, %URI{path: path}, _), do: File.read(path)

  @impl true
  def exists?(_, %URI{path: path}), do: File.exists?(path)
end

defimpl HLS.FS.Writer, for: HLS.FS.OS do
  @impl true
  def write(_, %URI{path: path}, payload, _) do
    :ok =
      path
      |> Path.dirname()
      |> File.mkdir_p()

    File.write(path, payload)
  end
end
