defmodule HLS.FS.HTTP do
  defstruct [:client, follow_redirects?: false]

  def new(follow_redirects? \\ false) do
    middleware = [
      {Tesla.Middleware.Retry,
       delay: 100,
       max_retries: 10,
       max_delay: 1_000,
       should_retry: fn
         {:ok, %{status: status}} when status >= 400 and status <= 500 -> true
         {:error, _} -> true
         {:ok, _} -> false
       end}
    ]

    middleware =
      if follow_redirects? do
        middleware ++ [Tesla.Middleware.FollowRedirects]
      else
        middleware
      end

    %__MODULE__{client: Tesla.client(middleware), follow_redirects?: follow_redirects?}
  end
end

defimpl HLS.FS.Reader, for: HLS.FS.HTTP do
  alias HLS.FS.HTTP

  @impl true
  def read(%HTTP{client: client}, uri = %URI{query: query}, _) do
    query = decode_query(query)
    uri = %URI{uri | query: nil}

    client
    |> Tesla.get(URI.to_string(uri), query: query)
    |> handle_response()
  end

  @impl true
  def exists?(%HTTP{follow_redirects?: follow_redirects?}, uri) do
    middleware = if follow_redirects?, do: [Tesla.Middleware.FollowRedirects], else: []
    client = Tesla.client(middleware)

    case Tesla.head(client, URI.to_string(uri)) do
      {:ok, %{status: 200}} -> true
      _other -> false
    end
  end

  defp decode_query(nil), do: []

  defp decode_query(raw) when is_binary(raw) do
    raw
    |> URI.decode_query()
    |> Enum.into([])
  end

  defp handle_response({:ok, %Tesla.Env{body: body, status: 200}}), do: {:ok, body}
  defp handle_response({:ok, %Tesla.Env{status: status}}), do: {:error, {:status, status}}
  defp handle_response(err = {:error, _reason}), do: err
end
