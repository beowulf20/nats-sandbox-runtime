export type RuntimeOverview = {
  nats: {
    url: string;
    connected: boolean;
    connected_url?: string;
    server_id?: string;
    server_name?: string;
    server_version?: string;
    jetstream: boolean;
    error?: string;
  };
  runtime: {
    service_name: string;
    service_version: string;
    online: boolean;
    id?: string;
    endpoints?: Array<{ name: string; subject: string }>;
    error?: string;
  };
  config: {
    bucket: string;
  };
  workers?: {
    desired: number;
    total: number;
    idle: number;
    busy: number;
  };
  checked_at: string;
};

export type Setting = {
  key: string;
  label?: string;
  description?: string;
  type?: "integer" | "duration" | string;
  value: unknown;
  default_value?: unknown;
  source?: "default" | "override" | string;
  min?: number;
};

export type RuntimeWorker = {
  id: string;
  status: "idle" | "busy" | string;
  snapshot_dir: string;
};

export type RuntimeWorkerSetRequest = {
  count: number;
};
