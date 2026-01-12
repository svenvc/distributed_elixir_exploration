defmodule GsTest do
  use ExUnit.Case

  test "key-value genserver" do
    {:ok, kv} = GenServer.start(KV, nil, [])
    assert(GenServer.call(kv, {:set, :foo, 123}))
    assert(GenServer.call(kv, {:get, :foo}) == 123)
    assert(GenServer.call(kv, :keys) == [:foo])
    assert(GenServer.call(kv, {:bogus}) == :not_implemented)
    Process.exit(kv, :kill)
  end

  test "pub-sub genserver" do
    {:ok, ps} = GenServer.start(PS, nil, [])
    assert(GenServer.call(ps, {:subscribe, :topic1}))
    assert(GenServer.call(ps, {:subscribers, :topic1}) == [self()])
    Process.spawn(fn -> GenServer.call(ps, {:broadcast, :topic1, {:msg, :foo}}) end, [])

    receive do
      {:msg, :foo} -> assert(true)
      _ -> assert(false)
    end

    assert(GenServer.call(ps, {:unsubscribe, :topic1}))
    assert(GenServer.call(ps, {:subscribers, :topic1}) == [])
    assert(GenServer.call(ps, {:bogus}) == :not_implemented)
    Process.exit(ps, :kill)
  end
end
