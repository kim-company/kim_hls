defmodule HLS.Helper do
  def merge_uri(nil, uri), do: uri

  def merge_uri(base = %URI{host: nil}, rel) do
    uri = merge_uri(%URI{base | host: "x"}, rel)
    %URI{uri | host: nil}
  end

  def merge_uri(base, rel) do
    URI.merge(base, rel)
  end
end
