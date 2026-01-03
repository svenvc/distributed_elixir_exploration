defmodule GswebTest do
  use ExUnit.Case
  doctest Gsweb

  test "greets the world" do
    assert Gsweb.hello() == :world
  end
end
