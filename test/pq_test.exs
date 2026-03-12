defmodule PQTest do
  use ExUnit.Case, async: true

  test "initial state", %{line: line} = _context do
    pq = start_unique(line)
    assert(PQ.head(pq) == nil)
    assert(PQ.count(pq) == 0)
    assert(PQ.empty?(pq))
    PQ.stop(pq)
  end

  test "simple enqueue/dequeue", %{line: line} = _context do
    pq = start_unique(line)
    1..3 |> Enum.each(fn n -> PQ.enqueue(pq, %{"n" => n}) end)
    assert(PQ.count(pq) == 3)
    assert(PQ.head(pq) == %{"n" => 1})
    1..3 |> Enum.each(fn n -> assert(PQ.dequeue(pq) |> Map.get("n") == n) end)
    assert(PQ.empty?(pq))
    stop(pq)
  end

  test "load state", %{line: line} = _context do
    pq = start_unique(line)
    1..3 |> Enum.each(fn n -> PQ.enqueue(pq, %{"n" => n}) end)
    PQ.dequeue(pq)
    assert(PQ.count(pq) == 2)
    assert(PQ.head(pq) == %{"n" => 2})
    stop(pq, false)
    pq = start_unique(line, false)
    assert(PQ.count(pq) == 2)
    assert(PQ.head(pq) == %{"n" => 2})
    stop(pq)
  end

  test "multiple segments", %{line: line} = _context do
    pq = start_unique(line)
    1..32 |> Enum.each(fn n -> PQ.enqueue(pq, %{"n" => n}) end)
    1..16 |> Enum.each(fn n -> assert(PQ.dequeue(pq) |> Map.get("n") == n) end)
    assert(PQ.head(pq) == %{"n" => 17})
    assert(PQ.count(pq) == 16)
    stop(pq)
  end

  test "load state multiple segments", %{line: line} = _context do
    pq = start_unique(line)
    1..32 |> Enum.each(fn n -> PQ.enqueue(pq, %{"n" => n}) end)
    1..16 |> Enum.each(fn n -> assert(PQ.dequeue(pq) |> Map.get("n") == n) end)
    assert(PQ.head(pq) == %{"n" => 17})
    assert(PQ.count(pq) == 16)
    stop(pq, false)
    pq = start_unique(line, false)
    assert(PQ.head(pq) == %{"n" => 17})
    assert(PQ.count(pq) == 16)
    stop(pq)
  end

  defp start_unique(id, clean \\ true) do
    name = "test-pq-#{id}"
    clean && File.rm_rf!(name)
    {:ok, pq} = PQ.start(name: String.to_atom(name))
    pq
  end

  defp stop(pq, clean \\ true) do
    clean && :sys.get_state(pq) |> PQ.queue_base_dir() |> File.rm_rf!()
    PQ.stop(pq)
  end
end
