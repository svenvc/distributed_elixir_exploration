defmodule KV do
  use GenServer

  @moduledoc """
  A simple key-value server.
  """

  @impl true
  def init(_) do
    {:ok, %{}}
  end

  @impl true
  def handle_call({:get, key}, _, state) do
    {:reply, Map.get(state, key), state}
  end

  @impl true
  def handle_call(["get", key], from, state) do
    handle_call({:get, key}, from, state)
  end

  @impl true
  def handle_call({:set, key, value}, _, state) do
    {:reply, true, Map.put(state, key, value)}
  end

  @impl true
  def handle_call(["set", key, value], from, state) do
    handle_call({:set, key, value}, from, state)
  end

  @impl true
  def handle_call(:keys, _, state) do
    {:reply, Map.keys(state), state}
  end

  @impl true
  def handle_call("keys", from, state) do
    handle_call(:keys, from, state)
  end
end
