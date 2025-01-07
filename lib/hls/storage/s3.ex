if Code.ensure_loaded?(ReqS3) do
  defmodule HLS.Storage.S3 do
    def new(opts) do
      opts =
        Keyword.validate!(opts, [
          :access_key_id,
          :secret_access_key,
          :region
        ])

      HLS.Storage.HTTP.new()
      |> update_in([Access.key!(:req)], fn req ->
        ReqS3.attach(req, aws_sigv4: opts)
      end)
    end
  end
end
