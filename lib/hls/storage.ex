defprotocol HLS.Storage do
  @spec get(HLS.Storage.t(), URI.t()) :: {:ok, binary()} | {:error, :not_found}
  def get(storage, uri)

  @spec put(HLS.Storage.t(), URI.t(), binary()) :: :ok | {:error, any()}
  def put(storage, uri, binary)
end

defmodule HLS.Storage.File do
  defstruct []

  def new(), do: %__MODULE__{}

  defimpl HLS.Storage do
    def get(_storage, uri) do
      case File.read(to_path(uri)) do
        {:ok, binary} -> {:ok, binary}
        {:error, :enoent} -> {:error, :not_found}
        {:error, code} -> raise "TODO custom exception #{inspect(code)}"
      end
    end

    def put(_storage, uri, binary) do
      path = to_path(uri)

      with :ok <- File.mkdir_p(Path.dirname(path)) do
        File.write(path, binary)
      end
    end

    defp to_path(%URI{scheme: "file"} = uri) do
      [uri.host, uri.path]
      |> Enum.reject(&is_nil/1)
      |> Path.join()
    end
  end
end
