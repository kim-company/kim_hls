defmodule HLSTest do
  use ExUnit.Case
  doctest HLS

  test "greets the world" do
    assert HLS.hello() == :world
  end
end
