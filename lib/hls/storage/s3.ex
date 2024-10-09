if Code.ensure_loaded?(ReqS3) do
  defmodule HLS.Storage.S3 do
    defstruct [:req]

    def new(opts) do
      opts =
        Keyword.validate!(opts, [
          :access_key_id,
          :secret_access_key,
          :region
        ])

      req =
        Req.new(
          retry: :transient,
          retry_delay: &retry_delay/1
        )
        |> ReqS3.attach(aws_sigv4: opts)

      %__MODULE__{req: req}
    end

    defp retry_delay(n) do
      delay =
        (Integer.pow(2, n) * 750)
        |> min(6000)

      jitter = 1 - 0.1 * :rand.uniform()

      trunc(delay * jitter)
    end

    defimpl HLS.Storage do
      def get(storage, uri, opts) do
        opts = Keyword.validate!(opts, max_retries: 3)

        case Req.get(storage.req,
               url: uri,
               max_retries: opts[:max_retries]
             ) do
          {:ok, %Req.Response{status: 200, body: binary}} ->
            {:ok, binary}

          {:ok, %Req.Response{status: 404}} ->
            {:error, :not_found}

          {:ok, %Req.Response{status: status}} ->
            {:error, "Status code #{inspect(status)}"}

          {:error, error} ->
            {:error, error}
        end
      end

      def put(storage, uri, binary, opts) do
        opts = Keyword.validate!(opts, max_retries: 3)

        case Req.put(storage.req,
               url: uri,
               body: binary,
               max_retries: opts[:max_retries]
             ) do
          {:ok, %Req.Response{status: 200}} ->
            :ok

          {:ok, %Req.Response{status: status}} ->
            {:error, "Status code #{inspect(status)}"}

          {:error, error} ->
            {:error, error}
        end
      end

      def delete(storage, uri, opts) do
        opts = Keyword.validate!(opts, max_retries: 3)

        case Req.delete(storage.req,
               url: uri,
               max_retries: opts[:max_retries]
             ) do
          {:ok, %Req.Response{status: 204}} ->
            :ok

          {:ok, %Req.Response{status: status}} ->
            {:error, "Status code #{inspect(status)}"}

          {:error, error} ->
            {:error, error}
        end
      end
    end
  end
end
