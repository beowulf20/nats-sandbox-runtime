import { useCallback, useEffect, useMemo, useState } from "react";
import type { Dispatch, SetStateAction } from "react";
import { FiRefreshCw, FiSave } from "react-icons/fi";
import { listWorkers, setWorkerCount, workerEventsURL } from "../api";
import Panel from "../components/Panel";
import StatusPill from "../components/StatusPill";
import type { RuntimeWorker } from "../types";

type WorkerSnapshot = {
  desired_count: number;
  workers: RuntimeWorker[];
};

export default function Workers() {
  const [workers, setWorkers] = useState<RuntimeWorker[]>([]);
  const [desiredCount, setDesiredCount] = useState("1");
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");

  const sortedWorkers = useMemo(
    () => [...workers].sort((a, b) => a.id.localeCompare(b.id)),
    [workers]
  );
  const busyCount = workers.filter((worker) => worker.status === "busy").length;

  const refresh = useCallback(async () => {
    try {
      setError("");
      const response = await listWorkers();
      applySnapshot(response, setWorkers, setDesiredCount);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to load workers");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  useEffect(() => {
    const events = new EventSource(workerEventsURL());
    events.addEventListener("workers", (event) => {
      try {
        applySnapshot(JSON.parse(event.data) as WorkerSnapshot, setWorkers, setDesiredCount);
        setError("");
        setLoading(false);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Unable to read worker event");
      }
    });
    events.onerror = () => {
      setError("Worker event stream disconnected");
    };
    return () => events.close();
  }, []);

  async function applyWorkerCount() {
    setError("");
    setMessage("");
    const parsed = Number(desiredCount);
    if (!Number.isInteger(parsed) || parsed < 1) {
      setError("Worker count must be an integer of at least 1");
      return;
    }
    try {
      setSaving(true);
      const response = await setWorkerCount({ count: parsed });
      applySnapshot(response, setWorkers, setDesiredCount);
      setMessage(`Worker count set to ${response.desired_count}`);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to set worker count");
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="space-y-5">
      <div className="grid gap-5 xl:grid-cols-[.9fr_1.1fr]">
        <Panel>
          <div className="flex flex-col justify-between gap-3 md:flex-row md:items-start">
            <div>
              <h2 className="font-poppins text-xl font-bold text-navy-700">
                Worker pool
              </h2>
              <p className="mt-1 text-sm font-medium text-gray-600">
                Set desired execution capacity; the runtime reconciles workers on demand.
              </p>
            </div>
            <button
              className="inline-flex w-fit items-center gap-2 rounded-lg bg-lightPrimary px-4 py-3 text-sm font-bold text-brand-500 transition-colors hover:bg-brand-50"
              onClick={refresh}
            >
              <FiRefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
              Refresh
            </button>
          </div>

          <div className="mt-6 grid grid-cols-4 gap-4">
            <Metric label="Desired" value={desiredCount || "1"} />
            <Metric label="Total" value={String(workers.length)} />
            <Metric label="Idle" value={String(workers.length - busyCount)} />
            <Metric label="Busy" value={String(busyCount)} />
          </div>

          {error ? (
            <p className="mt-5 rounded-lg bg-red-50 px-4 py-3 text-sm font-bold text-red-700">
              {error}
            </p>
          ) : null}
          {message ? (
            <p className="mt-5 rounded-lg bg-green-50 px-4 py-3 text-sm font-bold text-green-700">
              {message}
            </p>
          ) : null}
        </Panel>

        <Panel>
          <h2 className="font-poppins text-xl font-bold text-navy-700">Desired workers</h2>
          <p className="mt-1 text-sm font-medium text-gray-600">
            Increasing the count creates workers immediately. Decreasing it removes idle workers first and lets busy workers finish.
          </p>
          <div className="mt-5 flex flex-col gap-3 md:flex-row md:items-end">
            <label className="block md:w-48">
              <span className="text-sm font-bold text-navy-700">Worker count</span>
              <input
                className="mt-2 w-full rounded-lg border border-gray-200 px-4 py-3 text-sm font-medium text-navy-700 outline-none transition-colors focus:border-brand-500"
                min={1}
                step={1}
                type="number"
                value={desiredCount}
                onChange={(event) => setDesiredCount(event.target.value)}
              />
            </label>
            <button
              className="inline-flex w-fit items-center gap-2 rounded-lg bg-brand-500 px-4 py-3 text-sm font-bold text-white transition-colors hover:bg-brand-600 disabled:cursor-not-allowed disabled:opacity-60"
              onClick={applyWorkerCount}
              disabled={saving}
            >
              <FiSave className="h-4 w-4" />
              Apply
            </button>
          </div>
        </Panel>
      </div>

      <Panel>
        <div className="overflow-x-auto">
          <table className="w-full min-w-[820px] text-left">
            <thead>
              <tr className="border-b border-gray-200 text-xs font-bold uppercase text-gray-600">
                <th className="px-3 py-3">Worker</th>
                <th className="px-3 py-3">Status</th>
                <th className="px-3 py-3">Snapshot</th>
              </tr>
            </thead>
            <tbody>
              {sortedWorkers.map((worker) => (
                <tr key={worker.id} className="border-b border-gray-100 last:border-0">
                  <td className="px-3 py-4 font-mono text-sm font-bold text-navy-700">
                    {worker.id}
                  </td>
                  <td className="px-3 py-4">
                    <StatusPill
                      active={worker.status === "idle"}
                      activeLabel="Idle"
                      inactiveLabel={worker.status}
                    />
                  </td>
                  <td className="max-w-[320px] px-3 py-4 font-mono text-xs font-medium text-gray-600">
                    <span className="block truncate" title={worker.snapshot_dir}>
                      {worker.snapshot_dir}
                    </span>
                  </td>
                </tr>
              ))}
              {sortedWorkers.length === 0 ? (
                <tr>
                  <td colSpan={3} className="px-3 py-8 text-sm font-medium text-gray-600">
                    No workers are registered.
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </Panel>
    </div>
  );
}

function applySnapshot(
  snapshot: WorkerSnapshot,
  setWorkers: Dispatch<SetStateAction<RuntimeWorker[]>>,
  setDesiredCount: Dispatch<SetStateAction<string>>
) {
  setWorkers(snapshot.workers);
  setDesiredCount(String(snapshot.desired_count || snapshot.workers.length || 1));
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <p className="text-xs font-bold uppercase text-gray-600">{label}</p>
      <p className="mt-1 font-poppins text-2xl font-bold text-navy-700">{value}</p>
    </div>
  );
}
