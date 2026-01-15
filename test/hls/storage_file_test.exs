defmodule HLS.StorageFileTest do
  use ExUnit.Case, async: true

  alias HLS.Storage

  setup do
    base_dir =
      System.tmp_dir!()
      |> Path.join("hls_storage_#{System.unique_integer([:positive])}")

    File.mkdir_p!(base_dir)

    on_exit(fn -> File.rm_rf(base_dir) end)

    %{storage: HLS.Storage.File.new(base_dir: base_dir)}
  end

  test "put/get/delete round trip", %{storage: storage} do
    uri = URI.new!("segments/seg_00001.ts")

    assert :ok = Storage.put(storage, uri, "segment-data")
    assert {:ok, "segment-data"} = Storage.get(storage, uri)

    assert :ok = Storage.delete(storage, uri)
    assert {:error, :not_found} = Storage.get(storage, uri)
  end
end
