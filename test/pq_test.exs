defmodule PQTest do
  use ExUnit.Case, async: false

  test "initial state" do
    File.rm_rf("pq")
    {:ok, pq} = PQ.start([])
    assert(PQ.head(pq) == nil)
    assert(PQ.count(pq) == 0)
    assert(PQ.empty?(pq))
    PQ.stop(pq)
  end

  test "simple enqueue/dequeue" do
    File.rm_rf("pq")
    {:ok, pq} = PQ.start([])
    1..3 |> Enum.each(fn n -> PQ.enqueue(pq, %{"n" => n}) end)
    assert(PQ.count(pq) == 3)
    1..3 |> Enum.each(fn n -> assert(PQ.dequeue(pq) |> Map.get("n") == n) end)
    assert(PQ.empty?(pq))
    PQ.stop(pq)
  end
end
