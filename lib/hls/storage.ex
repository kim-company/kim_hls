defprotocol HLS.Storage do
  @spec read(t(), URI.t(), Keyword.t()) :: {:ok, binary()} | {:error, any}
  def read(driver, uri, opts \\ [])

  @spec write(t(), URI.t(), binary(), Keyword.t()) :: :ok | {:error, any}
  def write(driver, uri, data, opts \\ [])

  @spec exists?(t(), URI.t()) :: boolean()
  def exists?(driver, uri)
end
