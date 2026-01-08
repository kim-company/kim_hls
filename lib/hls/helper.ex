defmodule HLS.Helper do
  def merge_uri(nil, uri), do: uri

  def merge_uri(base = %URI{host: nil}, rel) do
    %URI{} = uri =
      URI.new!("http://example.com")
      |> URI.merge(base)
      |> merge_uri(rel)

    %URI{uri | scheme: base.scheme, host: nil, port: base.port}
  end

  def merge_uri(base, rel) do
    URI.merge(base, rel)
  end
end
