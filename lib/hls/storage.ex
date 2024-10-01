defprotocol HLS.Storage do
  @spec get(HLS.Storage.t(), URI.t()) :: {:ok, binary()} | {:error, :not_found}
  def get(storage, uri)

  @spec put(HLS.Storage.t(), URI.t(), binary()) :: :ok | {:error, any()}
  def put(storage, uri, binary)

  @spec delete(HLS.Storage.t(), URI.t()) :: :ok | {:error, any()}
  def delete(storage, uri)
end
