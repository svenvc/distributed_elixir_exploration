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
    stop(pq, clean: false)
    pq = start_unique(line, clean: false)
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
    stop(pq, clean: false)
    pq = start_unique(line, clean: false)
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

  test "dequeue with id", %{line: line} = _context do
    pq = start_unique(line)
    100..105 |> Enum.each(fn n -> {:ok, _record} = PQ.enqueue_r(pq, %{"code" => n}) end)
    {:ok, _record} = PQ.dequeue_r(pq)
    {:ok, %{"id" => id} = _record} = PQ.head_r(pq)
    {:ok, _record} = PQ.dequeue_r(pq, id: id)
    {:error, :mismatch} = PQ.dequeue_r(pq, id: id)
    1..4 |> Enum.each(fn i -> {:ok, _record} = PQ.dequeue_r(pq, id: id + i) end)
    {:error, :empty} = PQ.dequeue_r(pq)
    {:error, :empty} = PQ.head_r(pq)
    stop(pq)
  end

  test "delegate receives dequeued", %{line: line} = _context do
    pq = start_unique(line, delegate: self())
    PQ.enqueue(pq, %{"test" => 123})
    assert_receive :enqueued
    stop(pq)
  end

  test "simple worker", %{line: line} = _context do
    queue_name = String.to_atom(unique_name_for_test(line))
    worker_name = :"test-worker-#{line}"
    test_process = self()

    {:ok, worker} =
      PQWorker.start(
        queue_name: queue_name,
        name: worker_name,
        handler_function: fn msg ->
          send(test_process, msg)
          :ack
        end
      )

    pq = start_unique(line, delegate: worker)
    PQ.enqueue(pq, %{"test" => 123})
    assert_receive %{"test" => 123}
    stop(pq)
    PQWorker.stop(worker)
  end

  # support

  defp unique_name_for_test(id), do: "test-pq-#{id}"

  defp start_unique(id, opts \\ [clean: true]) do
    name = unique_name_for_test(id)
    Keyword.get(opts, :clean, true) && File.rm_rf!(name)
    {:ok, pq} = PQ.start([name: String.to_atom(name)] ++ opts)
    pq
  end

  defp stop(pq, opts \\ [clean: true]) do
    Keyword.get(opts, :clean, true) && queue_base_dir(pq) |> File.rm_rf!()
    PQ.stop(pq)
  end

  defp queue_base_dir(pq) do
    :sys.get_state(pq) |> PQ.queue_base_dir()
  end
end
