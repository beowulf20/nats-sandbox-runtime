import type {
  RuntimeOverview,
  RuntimeSnapshotStatus,
  RuntimeWorker,
  RuntimeWorkerSetRequest,
  RuntimeWorkspaceStatus,
  Setting,
} from "./types";

async function requestJSON<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(init?.headers || {}) },
    ...init,
  });
  if (!response.ok) {
    let message = `${response.status} ${response.statusText}`;
    try {
      const body = (await response.json()) as { error?: string };
      if (body.error) message = body.error;
    } catch {
      // Keep the status text if the response is not JSON.
    }
    throw new Error(message);
  }
  return response.json() as Promise<T>;
}

export function getOverview() {
  return requestJSON<RuntimeOverview>("/api/overview");
}

export function listSettings() {
  return requestJSON<{ settings: Setting[] }>("/api/settings");
}

export function listWorkers() {
  return requestJSON<{ desired_count: number; workers: RuntimeWorker[] }>("/api/workers");
}

export function listSnapshots() {
  return requestJSON<{ snapshots: RuntimeSnapshotStatus[] }>("/api/snapshots");
}

export function resetSnapshot(workerID: string) {
  return requestJSON<{ worker_id: string; status: string }>(
    `/api/snapshots/workers/${encodeURIComponent(workerID)}`,
    { method: "DELETE" }
  );
}

export function listWorkspaces() {
  return requestJSON<{ workspaces: RuntimeWorkspaceStatus[] }>("/api/workspaces");
}

export function resetWorkspace(key: string) {
  return requestJSON<{ key: string; status: string }>(
    `/api/workspaces/${encodeURIComponent(key)}`,
    { method: "DELETE" }
  );
}

export function workerEventsURL() {
  return "/api/workers/events";
}

export function setWorkerCount(payload: RuntimeWorkerSetRequest) {
  return requestJSON<{ status: string; desired_count: number; workers: RuntimeWorker[] }>("/api/workers", {
    method: "PUT",
    body: JSON.stringify(payload),
  });
}

export function setSetting(key: string, value: unknown) {
  return requestJSON<{ key: string; status: string }>(
    `/api/settings/${encodeURIComponent(key)}`,
    {
      method: "PUT",
      body: JSON.stringify({ value }),
    }
  );
}

export function deleteSetting(key: string) {
  return requestJSON<{ key: string; status: string }>(
    `/api/settings/${encodeURIComponent(key)}`,
    { method: "DELETE" }
  );
}
