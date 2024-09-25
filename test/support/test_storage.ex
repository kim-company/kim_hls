defmodule HLS.Storage.Test do
  defstruct [:pid]

  def new(pid), do: %__MODULE__{pid: pid}

  defimpl HLS.Storage do
    def get(_storage, _uri), do: {:error, :not_found}

    def put(storage, uri, binary) do
      send(storage.pid, {:put, uri, binary})
      :ok
    end

    def delete(storage, uri) do
      send(storage.pid, {:delete, uri})
      :ok
    end
  end
end
