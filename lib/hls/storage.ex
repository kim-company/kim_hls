defprotocol HLS.Storage do
  @type option :: {:max_retries, non_neg_integer()}
  @type options :: [option()]

  @spec get(HLS.Storage.t(), URI.t(), options()) ::
          {:ok, binary()} | {:error, :not_found} | {:error, any()}
  def get(storage, uri, opts \\ [])

  @spec put(HLS.Storage.t(), URI.t(), binary(), options()) :: :ok | {:error, any()}
  def put(storage, uri, binary, opts \\ [])

  @spec delete(HLS.Storage.t(), URI.t(), options()) :: :ok | {:error, any()}
  def delete(storage, uri, opts \\ [])
end
