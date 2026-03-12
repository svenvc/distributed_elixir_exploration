defmodule PQTest do
  use ExUnit.Case, async: true

  test "initial state", %{line: line} = _context do
    pq = start_unique(line)
    assert(PQ.head(pq) == nil)
    assert(PQ.count(pq) == 0)
    assert(PQ.empty?(pq))
    stop(pq)
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

  test "multiple segments 32/16", %{line: line} = _context do
    pq = start_unique(line)
    1..32 |> Enum.each(fn n -> PQ.enqueue(pq, %{"n" => n}) end)
    1..16 |> Enum.each(fn n -> assert(PQ.dequeue(pq) |> Map.get("n") == n) end)
    assert(PQ.head(pq) == %{"n" => 17})
    assert(PQ.count(pq) == 16)
    stop(pq)
  end

  test "multiple segments 40", %{line: line} = _context do
    pq = start_unique(line)
    1..40 |> Enum.each(fn n -> PQ.enqueue(pq, %{"n" => n}) end)
    assert(PQ.count(pq) == 40)
    1..40 |> Enum.each(fn n -> assert(PQ.dequeue(pq) == %{"n" => n}) end)
    assert(PQ.empty?(pq))
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

  test "full", %{line: line} = _context do
    pq = start_unique(line)
    1..50 |> Enum.each(fn n -> assert(PQ.enqueue(pq, %{"n" => n})) end)
    refute(PQ.empty?(pq))
    assert(PQ.count(pq) == 50)
    refute(PQ.enqueue(pq, %{"n" => 51}))
    stop(pq)
  end

  test "gc", %{line: line} = _context do
    pq = start_unique(line)

    0..4
    |> Enum.each(fn round ->
      1..40 |> Enum.each(fn i -> assert(PQ.enqueue(pq, %{"n" => round * 40 + i})) end)
      assert(PQ.count(pq) == 40)
      1..40 |> Enum.each(fn i -> assert(PQ.dequeue(pq) == %{"n" => round * 40 + i}) end)
      assert(PQ.empty?(pq))
    end)

    assert(Enum.empty?(queue_base_dir(pq) |> File.ls!()))

    stop(pq)
  end

  test "harmonica", %{line: line} = _context do
    pq = start_unique(line)

    [{40, 10}, {10, 30}, {20, 10}, {10, 30}, {20, 20}, {15, 5}, {15, 5}, {10, 15}, {10, 25}]
    |> Enum.reduce({0, 0}, fn {to_enqueue, to_dequeue}, {total_enqueued, total_dequeued} ->
      total_enqueued_next = total_enqueued + to_enqueue

      total_enqueued..(total_enqueued_next - 1)
      |> Enum.each(fn n -> assert(PQ.enqueue(pq, %{"n" => n})) end)

      total_dequeued_next = total_dequeued + to_dequeue

      total_dequeued..(total_dequeued_next - 1)
      |> Enum.each(fn n -> assert(PQ.dequeue(pq) == %{"n" => n}) end)

      {total_enqueued_next, total_dequeued_next}
    end)

    assert(Enum.empty?(queue_base_dir(pq) |> File.ls!()))
    stop(pq)
  end

  # support

  defp start_unique(id, clean \\ true) do
    name = "test-pq-#{id}"
    clean && File.rm_rf!(name)
    {:ok, pq} = PQ.start(name: String.to_atom(name))
    pq
  end

  defp stop(pq, clean \\ true) do
    clean && queue_base_dir(pq) |> File.rm_rf!()
    PQ.stop(pq)
  end

  defp queue_base_dir(pq) do
    :sys.get_state(pq) |> PQ.queue_base_dir()
  end
end
