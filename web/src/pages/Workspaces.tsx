import { useCallback, useEffect, useMemo, useState } from "react";
import { FiRefreshCw, FiRotateCcw } from "react-icons/fi";
import { listWorkspaces, resetWorkspace } from "../api";
import Panel from "../components/Panel";
import type { RuntimeWorkspaceStatus } from "../types";

export default function Workspaces() {
  const [workspaces, setWorkspaces] = useState<RuntimeWorkspaceStatus[]>([]);
  const [loading, setLoading] = useState(true);
  const [resetting, setResetting] = useState("");
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");

  const sortedWorkspaces = useMemo(
    () => [...workspaces].sort((a, b) => a.key.localeCompare(b.key)),
    [workspaces]
  );

  const readyCount = workspaces.filter((workspace) => workspace.file.exists).length;
  const busyCount = workspaces.filter((workspace) => workspace.busy).length;

  const refresh = useCallback(async () => {
    try {
      setError("");
      const response = await listWorkspaces();
      setWorkspaces(Array.isArray(response.workspaces) ? response.workspaces : []);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to load workspaces");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
    const timer = window.setInterval(refresh, 5000);
    return () => window.clearInterval(timer);
  }, [refresh]);

  async function clearWorkspace(key: string) {
    setError("");
    setMessage("");
    try {
      setResetting(key);
      await resetWorkspace(key);
      setMessage(`${key} workspace reset`);
      await refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to reset workspace");
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
              Workspace ext4
            </h2>
            <p className="mt-1 text-sm font-medium text-gray-600">
              Persistent workspace images keyed by thread ID.
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
          <Metric label="Workspaces" value={String(workspaces.length)} />
          <Metric label="Existing" value={String(readyCount)} />
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
          <table className="w-full min-w-[960px] text-left">
            <thead>
              <tr className="border-b border-gray-200 text-xs font-bold uppercase text-gray-600">
                <th className="px-3 py-3">Key</th>
                <th className="px-3 py-3">Status</th>
                <th className="px-3 py-3">Configured</th>
                <th className="px-3 py-3">Size</th>
                <th className="px-3 py-3">Modified</th>
                <th className="px-3 py-3">Expires</th>
                <th className="px-3 py-3">Path</th>
                <th className="px-3 py-3 text-right">Actions</th>
              </tr>
            </thead>
            <tbody>
              {sortedWorkspaces.map((workspace) => (
                <tr key={workspace.key} className="border-b border-gray-100 last:border-0">
                  <td className="px-3 py-4 font-mono text-sm font-bold text-navy-700">
                    {workspace.key}
                  </td>
                  <td className="px-3 py-4">
                    <StoragePill
                      tone={workspace.busy ? "busy" : workspace.file.exists ? "ready" : "missing"}
                      label={workspace.busy ? "Busy" : workspace.file.exists ? "Ready" : "Missing"}
                    />
                  </td>
                  <td className="px-3 py-4 text-sm font-bold text-navy-700">
                    {workspace.workspace_mib} MiB
                  </td>
                  <td className="px-3 py-4 text-sm font-bold text-navy-700">
                    {workspace.file.exists ? formatBytes(workspace.file.size_bytes) : "-"}
                  </td>
                  <td className="px-3 py-4 text-sm font-medium text-gray-600">
                    {formatDate(workspace.file.modified_at)}
                  </td>
                  <td className="px-3 py-4 text-sm font-medium text-gray-600">
                    {formatDate(workspace.expires_at)}
                  </td>
                  <td className="max-w-[320px] px-3 py-4 font-mono text-xs font-medium text-gray-600">
                    <span className="block truncate" title={workspace.file.path}>
                      {workspace.file.path}
                    </span>
                  </td>
                  <td className="px-3 py-4 text-right">
                    <button
                      className="inline-flex items-center gap-2 rounded-lg bg-lightPrimary px-4 py-2 text-sm font-bold text-gray-700 transition-colors hover:bg-gray-100 disabled:cursor-not-allowed disabled:opacity-60"
                      onClick={() => clearWorkspace(workspace.key)}
                      disabled={workspace.busy || resetting === workspace.key}
                    >
                      <FiRotateCcw className="h-4 w-4" />
                      Reset
                    </button>
                  </td>
                </tr>
              ))}
              {sortedWorkspaces.length === 0 ? (
                <tr>
                  <td colSpan={8} className="px-3 py-8 text-sm font-medium text-gray-600">
                    No workspaces are registered.
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

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <p className="text-xs font-bold uppercase text-gray-600">{label}</p>
      <p className="mt-1 font-poppins text-2xl font-bold text-navy-700">{value}</p>
    </div>
  );
}

function StoragePill({
  tone,
  label,
}: {
  tone: "ready" | "busy" | "missing";
  label: string;
}) {
  const classes = {
    ready: "bg-green-50 text-green-700",
    busy: "bg-yellow-50 text-yellow-700",
    missing: "bg-gray-100 text-gray-600",
  };
  const dotClasses = {
    ready: "bg-green-500",
    busy: "bg-yellow-500",
    missing: "bg-gray-400",
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

function formatDate(value?: string) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}
