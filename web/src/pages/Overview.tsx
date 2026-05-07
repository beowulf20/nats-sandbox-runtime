import { useCallback, useEffect, useState } from "react";
import type { ReactNode } from "react";
import { FiCpu, FiDatabase, FiRefreshCw, FiServer, FiZap } from "react-icons/fi";
import { getOverview, workerEventsURL } from "../api";
import Panel from "../components/Panel";
import StatusPill from "../components/StatusPill";
import type { RuntimeOverview } from "../types";
import type { RuntimeWorker } from "../types";

type WorkerSnapshot = {
  desired_count: number;
  workers: RuntimeWorker[];
};

const emptyOverview: RuntimeOverview = {
  nats: { url: "", connected: false, jetstream: false },
  runtime: {
    service_name: "python-runtime",
    service_version: "",
    online: false,
    endpoints: [],
  },
  config: { bucket: "" },
  workers: { desired: 0, total: 0, idle: 0, busy: 0 },
  checked_at: "",
};

export default function Overview() {
  const [overview, setOverview] = useState<RuntimeOverview>(emptyOverview);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const refresh = useCallback(async () => {
    try {
      setError("");
      const next = await getOverview();
      setOverview(next);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to load overview");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
    const timer = window.setInterval(refresh, 5000);
    return () => window.clearInterval(timer);
  }, [refresh]);

  useEffect(() => {
    const events = new EventSource(workerEventsURL());
    events.addEventListener("workers", (event) => {
      try {
        const snapshot = JSON.parse(event.data) as WorkerSnapshot;
        setOverview((current) => ({
          ...current,
          workers: workerStatusFromSnapshot(snapshot),
        }));
      } catch {
        // The periodic overview refresh remains the fallback.
      }
    });
    return () => events.close();
  }, []);

  return (
    <div className="space-y-5">
      <div className="flex flex-col justify-between gap-3 md:flex-row md:items-end">
        <div>
          <p className="text-sm font-medium text-gray-600">
            {overview.checked_at ? `Last checked ${formatDate(overview.checked_at)}` : "Waiting for status"}
          </p>
        </div>
        <button
          className="inline-flex w-fit items-center gap-2 rounded-lg bg-brand-500 px-4 py-2 text-sm font-bold text-white transition-colors hover:bg-brand-600"
          onClick={refresh}
        >
          <FiRefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
          Refresh
        </button>
      </div>

      {error ? (
        <div className="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm font-bold text-red-700">
          {error}
        </div>
      ) : null}

      <div className="grid gap-5 xl:grid-cols-3">
        <StatusWidget
          icon={<FiServer className="h-6 w-6" />}
          title="NATS connection"
          value={overview.nats.connected ? "Connected" : "Offline"}
          status={
            <StatusPill
              active={overview.nats.connected}
              activeLabel="Connected"
              inactiveLabel="Disconnected"
            />
          }
          detail={overview.nats.connected_url || overview.nats.url || "No connection"}
        />
        <StatusWidget
          icon={<FiCpu className="h-6 w-6" />}
          title="Runtime status"
          value={overview.runtime.online ? "Online" : "Unavailable"}
          status={
            <StatusPill
              active={overview.runtime.online}
              activeLabel="Registered"
              inactiveLabel="Missing"
            />
          }
          detail={overview.runtime.id || overview.runtime.service_name}
        />
        <StatusWidget
          icon={<FiZap className="h-6 w-6" />}
          title="Worker pool"
          value={`${overview.workers?.total ?? 0} active`}
          status={
            <StatusPill
              active={(overview.workers?.total ?? 0) > 0}
              activeLabel={`${overview.workers?.desired ?? 0} desired`}
              inactiveLabel="Empty"
            />
          }
          detail={`${overview.workers?.idle ?? 0} idle, ${overview.workers?.busy ?? 0} busy`}
        />
      </div>

      <div className="grid gap-5 xl:grid-cols-[1.1fr_.9fr]">
        <Panel>
          <div className="mb-5 flex items-center gap-3">
            <div className="rounded-lg bg-lightPrimary p-3 text-brand-500">
              <FiDatabase className="h-5 w-5" />
            </div>
            <div>
              <h2 className="font-poppins text-xl font-bold text-navy-700">Runtime details</h2>
              <p className="text-sm font-medium text-gray-600">
                Service registration and local execution defaults.
              </p>
            </div>
          </div>
          <dl className="grid gap-4 md:grid-cols-2">
            <Fact label="NATS URL" value={overview.nats.url || "-"} />
            <Fact label="Server" value={overview.nats.server_name || "-"} />
            <Fact label="Server version" value={overview.nats.server_version || "-"} />
            <Fact label="JetStream" value={overview.nats.jetstream ? "Available" : "Unavailable"} />
            <Fact label="Bucket" value={overview.config.bucket || "-"} />
            <Fact label="Desired workers" value={String(overview.workers?.desired ?? "-")} />
            <Fact label="Active workers" value={String(overview.workers?.total ?? "-")} />
          </dl>
        </Panel>

        <Panel>
          <h2 className="font-poppins text-xl font-bold text-navy-700">Endpoints</h2>
          <div className="mt-4 space-y-3">
            {(overview.runtime.endpoints || []).map((endpoint) => (
              <div
                key={`${endpoint.name}:${endpoint.subject}`}
                className="rounded-lg border border-gray-200 px-4 py-3"
              >
                <p className="text-sm font-bold text-navy-700">{endpoint.name}</p>
                <p className="mt-1 font-mono text-xs text-gray-600">{endpoint.subject}</p>
              </div>
            ))}
            {(overview.runtime.endpoints || []).length === 0 ? (
              <p className="text-sm font-medium text-gray-600">No endpoints reported.</p>
            ) : null}
          </div>
        </Panel>
      </div>
    </div>
  );
}

function workerStatusFromSnapshot(snapshot: WorkerSnapshot) {
  const busy = snapshot.workers.filter((worker) => worker.status === "busy").length;
  return {
    desired: snapshot.desired_count,
    total: snapshot.workers.length,
    idle: snapshot.workers.length - busy,
    busy,
  };
}

function StatusWidget(props: {
  icon: ReactNode;
  title: string;
  value: string;
  detail: string;
  status: ReactNode;
}) {
  return (
    <Panel>
      <div className="flex items-start justify-between gap-4">
        <div className="rounded-lg bg-lightPrimary p-3 text-brand-500">{props.icon}</div>
        {props.status}
      </div>
      <p className="mt-5 text-sm font-bold text-gray-600">{props.title}</p>
      <p className="mt-1 font-poppins text-3xl font-bold text-navy-700">{props.value}</p>
      <p className="mt-2 break-all text-sm font-medium text-gray-600">{props.detail}</p>
    </Panel>
  );
}

function Fact({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt className="text-xs font-bold uppercase text-gray-600">{label}</dt>
      <dd className="mt-1 break-all text-sm font-bold text-navy-700">{value}</dd>
    </div>
  );
}

function formatDate(value: string) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleTimeString();
}
