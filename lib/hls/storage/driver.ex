defprotocol HLS.Storage.Driver do
  @type content_t :: String.t() | binary()
  @type ok_t :: {:ok, content_t}
  @type error_t :: {:error, any}
  @type callback_result_t :: ok_t | error_t

  @spec get(t()) :: callback_result_t
  def get(driver)

  @spec get(t(), URI.t()) :: callback_result_t
  def get(driver, uri)

  @spec put(t(), URI.t(), binary(), Keyword.t()) :: :ok | error_t
  def put(driver, uri, data, opts)

  @spec ready?(t()) :: boolean()
  def ready?(driver)
end
