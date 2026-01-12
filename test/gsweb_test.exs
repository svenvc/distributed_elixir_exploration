defmodule GswebTest do
  use ExUnit.Case
  require Logger

  test "key-value genserver" do
    assert(Req.post!("http://localhost:4000/process/kv1/call", json: ["set", "foo", 101]).body)
    assert(Req.post!("http://localhost:4000/process/kv1/call", json: ["get", "foo"]).body == 101)

    assert(
      Enum.member?(Req.post!("http://localhost:4000/process/kv1/call", json: "keys").body, "foo")
    )

    assert(
      Req.post!("http://localhost:4000/process/kv1/call", json: ["error"]).body ==
        "not_implemented"
    )
  end

  test "pub-sub genserver" do
    Process.spawn(
      fn ->
        Process.sleep(100)

        assert(
          Req.post!("http://localhost:4000/process/ps1/call",
            json: ["broadcast", "topic1", ["msg", 101]]
          ).body
        )
      end,
      []
    )

    Req.post!(
      "http://localhost:4000/process/ps1/call-receive",
      json: ["subscribe", "topic1"],
      into: fn {:data, data}, {req, resp} ->
        Logger.debug("received: #{data}")

        if String.starts_with?(data, "data:") do
          assert(String.slice(data, 6..-1//1) |> JSON.decode!() == ["msg", 101])
          {:halt, {req, resp}}
        else
          {:cont, {req, resp}}
        end
      end
    )
  end
end
