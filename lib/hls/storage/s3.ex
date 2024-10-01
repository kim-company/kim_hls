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

      %__MODULE__{req: ReqS3.attach(Req.new(), aws_sigv4: opts)}
    end

    defimpl HLS.Storage do
      def get(storage, uri) do
        case Req.get(storage.req, url: uri) do
          {:ok, %Req.Response{status: 200, body: binary}} ->
            {:ok, binary}

          {:ok, %Req.Response{status: 404}} ->
            {:error, :not_found}

          {:ok, %Req.Response{status: status}} ->
            raise "#{__MODULE__}.get/2 of uri #{to_string(uri)} failed with status code #{status}."

          {:error, error} ->
            raise "#{__MODULE__}.get/2 of uri #{to_string(uri)} failed with #{inspect(error)}."
        end
      end

      def put(storage, uri, binary) do
        case Req.put(storage.req, url: uri, body: binary) do
          {:ok, %Req.Response{status: 200}} ->
            :ok

          {:ok, %Req.Response{status: status}} ->
            raise "#{__MODULE__}.put/3 of uri #{to_string(uri)} failed with status code #{status}."

          {:error, error} ->
            raise "#{__MODULE__}.put/3 of uri #{to_string(uri)} failed with #{inspect(error)}."
        end
      end

      def delete(storage, uri) do
        case Req.delete(storage.req, url: uri) do
          {:ok, %Req.Response{status: 204}} ->
            :ok

          {:ok, %Req.Response{status: status}} ->
            raise "#{__MODULE__}.delete/2 of uri #{to_string(uri)} failed with status code #{status}."

          {:error, error} ->
            raise "#{__MODULE__}.delete/2 of uri #{to_string(uri)} failed with #{inspect(error)}."
        end
      end
    end
  end
end
