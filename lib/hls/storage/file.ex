defmodule HLS.Storage.File do
  defstruct base_dir: nil

  def new(opts \\ []) do
    %__MODULE__{
      base_dir: Keyword.get(opts, :base_dir)
    }
  end

  defimpl HLS.Storage do
    def get(storage, uri, _opts) do
      case File.read(to_path(storage, uri)) do
        {:ok, binary} ->
          {:ok, binary}

        {:error, :enoent} ->
          {:error, :not_found}

        {:error, code} ->
          raise "#{__MODULE__}.get/2 of uri #{to_string(uri)} failed with #{inspect(code)}."
      end
    end

    def put(storage, uri, binary, _opts) do
      path = to_path(storage, uri)

      with :ok <- File.mkdir_p(Path.dirname(path)) do
        File.write(path, binary)
      end
    end

    def delete(storage, uri, _opts) do
      case File.rm(to_path(storage, uri)) do
        :ok -> :ok
        {:error, :enoent} -> :ok
        other -> other
      end
    end

    defp to_path(_storage, %URI{scheme: "file"} = uri) do
      [uri.host, uri.path]
      |> Enum.reject(&(is_nil(&1) or &1 == ""))
      |> Path.join()
    end

    # Handle relative URIs (no scheme) - resolve against base_dir if provided
    defp to_path(storage, %URI{scheme: nil, path: path}) when is_binary(path) do
      if storage.base_dir do
        Path.join(storage.base_dir, path)
      else
        path
      end
    end
  end
end
