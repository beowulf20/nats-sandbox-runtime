import { useCallback, useEffect, useMemo, useState } from "react";
import { FiRefreshCw, FiRotateCcw } from "react-icons/fi";
import { listSnapshots, resetSnapshot, workerEventsURL } from "../api";
import Panel from "../components/Panel";
import type { RuntimeSnapshotStatus, RuntimeWorker } from "../types";

type WorkerSnapshot = {
  workers: RuntimeWorker[];
};

const snapshotFiles = [
  { key: "snapshot", label: "State" },
  { key: "memory", label: "Memory" },
  { key: "version", label: "Version" },
  { key: "swap", label: "Swap" },
];

export default function Snapshots() {
  const [snapshots, setSnapshots] = useState<RuntimeSnapshotStatus[]>([]);
  const [loading, setLoading] = useState(true);
  const [resetting, setResetting] = useState("");
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");

  const sortedSnapshots = useMemo(
    () => [...snapshots].sort((a, b) => a.worker_id.localeCompare(b.worker_id)),
    [snapshots]
  );

  const readyCount = snapshots.filter((snapshot) => snapshot.ok).length;
  const busyCount = snapshots.filter((snapshot) => snapshot.busy).length;

  const refresh = useCallback(async () => {
    try {
      setError("");
      const response = await listSnapshots();
      setSnapshots(response.snapshots);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to load snapshots");
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
        const snapshot = JSON.parse(event.data) as WorkerSnapshot;
        setSnapshots((current) => mergeWorkerBusy(current, snapshot.workers));
      } catch {
        // Manual refresh remains available if a transient event cannot be parsed.
      }
    });
    return () => events.close();
  }, []);

  async function clearSnapshot(workerID: string) {
    setError("");
    setMessage("");
    try {
      setResetting(workerID);
      await resetSnapshot(workerID);
      setMessage(`${workerID} snapshot reset`);
      await refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to reset snapshot");
    } finally {
      setResetting("");
    }
  }

  return (
    <div className="space-y-5">
      <Panel>
        <div className="flex flex-col justify-between gap-3 md:flex-row md:items-start">
          <div>
            <h2 className="font-poppins text-xl font-bold text-navy-700">
              VM snapshots
            </h2>
            <p className="mt-1 text-sm font-medium text-gray-600">
              Snapshot state for each runtime worker.
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

        <div className="mt-6 grid grid-cols-3 gap-4">
          <Metric label="Workers" value={String(snapshots.length)} />
          <Metric label="Ready" value={String(readyCount)} />
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
        <div className="overflow-x-auto">
          <table className="w-full min-w-[980px] text-left">
            <thead>
              <tr className="border-b border-gray-200 text-xs font-bold uppercase text-gray-600">
                <th className="px-3 py-3">Worker</th>
                <th className="px-3 py-3">Status</th>
                <th className="px-3 py-3">Files</th>
                <th className="px-3 py-3">Version</th>
                <th className="px-3 py-3">Snapshot dir</th>
                <th className="px-3 py-3 text-right">Actions</th>
              </tr>
            </thead>
            <tbody>
              {sortedSnapshots.map((snapshot) => (
                <tr key={snapshot.worker_id} className="border-b border-gray-100 last:border-0">
                  <td className="px-3 py-4 font-mono text-sm font-bold text-navy-700">
                    {snapshot.worker_id}
                  </td>
                  <td className="px-3 py-4">
                    <StoragePill
                      tone={snapshot.busy ? "busy" : snapshot.ok ? "ready" : "invalid"}
                      label={snapshot.busy ? "Busy" : snapshot.ok ? "Ready" : "Invalid"}
                    />
                  </td>
                  <td className="px-3 py-4">
                    <div className="grid gap-2 text-xs font-bold text-navy-700 md:grid-cols-2">
                      {snapshotFiles.map((item) => {
                        const file = snapshot.files[item.key];
                        return (
                          <span key={item.key} className="whitespace-nowrap">
                            {item.label}:{" "}
                            <span className="font-medium text-gray-600">
                              {file?.exists ? formatBytes(file.size_bytes) : "missing"}
                            </span>
                          </span>
                        );
                      })}
                    </div>
                  </td>
                  <td className="max-w-[220px] px-3 py-4 text-sm font-bold text-navy-700">
                    <span className="block truncate" title={snapshot.version || snapshot.reason || "-"}>
                      {snapshot.version || snapshot.reason || "-"}
                    </span>
                  </td>
                  <td className="max-w-[280px] px-3 py-4 font-mono text-xs font-medium text-gray-600">
                    <span className="block truncate" title={snapshot.snapshot_dir}>
                      {snapshot.snapshot_dir}
                    </span>
                  </td>
                  <td className="px-3 py-4 text-right">
                    <button
                      className="inline-flex items-center gap-2 rounded-lg bg-lightPrimary px-4 py-2 text-sm font-bold text-gray-700 transition-colors hover:bg-gray-100 disabled:cursor-not-allowed disabled:opacity-60"
                      onClick={() => clearSnapshot(snapshot.worker_id)}
                      disabled={snapshot.busy || resetting === snapshot.worker_id}
                    >
                      <FiRotateCcw className="h-4 w-4" />
                      Reset
                    </button>
                  </td>
                </tr>
              ))}
              {sortedSnapshots.length === 0 ? (
                <tr>
                  <td colSpan={6} className="px-3 py-8 text-sm font-medium text-gray-600">
                    No worker snapshots are registered.
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

function mergeWorkerBusy(
  snapshots: RuntimeSnapshotStatus[],
  workers: RuntimeWorker[]
) {
  const statuses = new Map(workers.map((worker) => [worker.id, worker.status]));
  return snapshots.map((snapshot) => ({
    ...snapshot,
    busy: statuses.get(snapshot.worker_id) === "busy",
  }));
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <p className="text-xs font-bold uppercase text-gray-600">{label}</p>
      <p className="mt-1 font-poppins text-2xl font-bold text-navy-700">{value}</p>
    </div>
  );
}

function StoragePill({ tone, label }: { tone: "ready" | "busy" | "invalid"; label: string }) {
  const classes = {
    ready: "bg-green-50 text-green-700",
    busy: "bg-yellow-50 text-yellow-700",
    invalid: "bg-red-50 text-red-600",
  };
  const dotClasses = {
    ready: "bg-green-500",
    busy: "bg-yellow-500",
    invalid: "bg-red-500",
  };
  return (
    <span className={`inline-flex items-center rounded-full px-3 py-1 text-xs font-bold ${classes[tone]}`}>
      <span className={`mr-2 h-2 w-2 rounded-full ${dotClasses[tone]}`} />
      {label}
    </span>
  );
}

function formatBytes(value?: number) {
  if (typeof value !== "number") return "-";
  if (value === 0) return "0 B";
  const units = ["B", "KiB", "MiB", "GiB"];
  let size = value;
  let index = 0;
  while (size >= 1024 && index < units.length - 1) {
    size /= 1024;
    index += 1;
  }
  return `${size >= 10 || index === 0 ? size.toFixed(0) : size.toFixed(1)} ${units[index]}`;
}
