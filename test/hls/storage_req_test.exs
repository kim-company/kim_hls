if Code.ensure_loaded?(Req) and Code.ensure_loaded?(Plug.Test) do
  defmodule HLS.StorageReqTest do
    use ExUnit.Case

    alias HLS.Storage

    setup do
      Req.Test.stub(__MODULE__, fn conn ->
        case {conn.method, conn.request_path} do
          {"GET", "/segment.ts"} ->
            Req.Test.text(conn, "segment-data")

          {"PUT", "/segment.ts"} ->
            Req.Test.text(conn, "")

          {"DELETE", "/segment.ts"} ->
            conn
            |> Plug.Conn.put_status(204)
            |> Req.Test.text("")

          _ ->
            conn
            |> Plug.Conn.put_status(404)
            |> Req.Test.text("")
        end
      end)

      req = Req.new(base_url: "http://storage.test", plug: {Req.Test, __MODULE__})
      %{storage: HLS.Storage.Req.new(req)}
    end

    test "get/put/delete use Req.Test stub", %{storage: storage} do
      uri = URI.new!("http://storage.test/segment.ts")

      assert {:ok, "segment-data"} = Storage.get(storage, uri)
      assert :ok = Storage.put(storage, uri, "segment-data")
      assert :ok = Storage.delete(storage, uri)
    end

    test "not found maps to error", %{storage: storage} do
      uri = URI.new!("http://storage.test/missing.ts")

      assert {:error, :not_found} = Storage.get(storage, uri)
    end
  end
end
