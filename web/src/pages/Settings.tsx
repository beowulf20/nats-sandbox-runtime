import { useCallback, useEffect, useMemo, useState } from "react";
import { FiRefreshCw, FiRotateCcw, FiSave } from "react-icons/fi";
import { deleteSetting, listSettings, setSetting } from "../api";
import Panel from "../components/Panel";
import type { Setting } from "../types";

type Drafts = Record<string, string>;

export default function Settings() {
  const [settings, setSettings] = useState<Setting[]>([]);
  const [drafts, setDrafts] = useState<Drafts>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");

  const sortedSettings = useMemo(
    () => [...settings].sort((a, b) => a.key.localeCompare(b.key)),
    [settings]
  );

  const refresh = useCallback(async () => {
    try {
      setError("");
      const response = await listSettings();
      setSettings(response.settings);
      setDrafts((current) => {
        const next = { ...current };
        for (const setting of response.settings) {
          if (!(setting.key in next)) {
            next[setting.key] = draftValue(setting);
          }
        }
        return next;
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to load settings");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  async function save(setting: Setting) {
    setError("");
    setMessage("");
    let parsed: unknown;
    try {
      parsed = parseDraft(setting, drafts[setting.key] ?? "");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Invalid setting value");
      return;
    }
    try {
      await setSetting(setting.key, parsed);
      setMessage(`${setting.label || setting.key} saved`);
      await refreshSettingDraft(setting.key);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to save setting");
    }
  }

  async function reset(setting: Setting) {
    setError("");
    setMessage("");
    try {
      await deleteSetting(setting.key);
      setMessage(`${setting.label || setting.key} reset`);
      await refreshSettingDraft(setting.key);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to reset setting");
    }
  }

  async function refreshSettingDraft(key?: string) {
    const response = await listSettings();
    setSettings(response.settings);
    setDrafts((current) => {
      const next = { ...current };
      for (const setting of response.settings) {
        if (!key || setting.key === key) {
          next[setting.key] = draftValue(setting);
        }
      }
      return next;
    });
  }

  return (
    <div className="space-y-5">
      <Panel>
        <div className="flex flex-col justify-between gap-3 md:flex-row md:items-center">
          <div>
            <h2 className="font-poppins text-xl font-bold text-navy-700">
              Runtime defaults
            </h2>
            <p className="text-sm font-medium text-gray-600">
              Only settings with a runtime effect are available here.
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
        {error ? (
          <p className="mt-4 rounded-lg bg-red-50 px-4 py-3 text-sm font-bold text-red-700">
            {error}
          </p>
        ) : null}
        {message ? (
          <p className="mt-4 rounded-lg bg-green-50 px-4 py-3 text-sm font-bold text-green-700">
            {message}
          </p>
        ) : null}
      </Panel>

      <div className="grid gap-5 xl:grid-cols-2">
        {sortedSettings.map((setting) => (
          <Panel key={setting.key}>
            <div className="mb-5 flex items-start justify-between gap-4">
              <div>
                <div className="flex flex-wrap items-center gap-2">
                  <h3 className="font-poppins text-lg font-bold text-navy-700">
                    {setting.label || setting.key}
                  </h3>
                  <span
                    className={`rounded-full px-3 py-1 text-xs font-bold ${
                      setting.source === "override"
                        ? "bg-brand-50 text-brand-600"
                        : "bg-gray-100 text-gray-600"
                    }`}
                  >
                    {setting.source === "override" ? "Override" : "Default"}
                  </span>
                </div>
                <p className="mt-1 break-all font-mono text-xs text-gray-500">
                  {setting.key}
                </p>
                <p className="mt-3 text-sm font-medium text-gray-600">
                  {setting.description}
                </p>
              </div>
            </div>

            <label className="block">
              <span className="text-sm font-bold text-navy-700">
                Effective value
              </span>
              <input
                className="mt-2 w-full rounded-lg border border-gray-200 px-4 py-3 text-sm font-medium text-navy-700 outline-none transition-colors focus:border-brand-500"
                type={setting.type === "integer" ? "number" : "text"}
                min={setting.min}
                value={drafts[setting.key] ?? draftValue(setting)}
                onChange={(event) =>
                  setDrafts((current) => ({
                    ...current,
                    [setting.key]: event.target.value,
                  }))
                }
              />
            </label>

            <div className="mt-4 grid gap-3 text-sm md:grid-cols-2">
              <Fact label="Startup default" value={String(setting.default_value ?? "")} />
              <Fact label="Type" value={setting.type || "json"} />
              {typeof setting.min === "number" ? (
                <Fact label="Minimum" value={String(setting.min)} />
              ) : null}
            </div>

            <div className="mt-5 flex flex-wrap gap-3">
              <button
                className="inline-flex items-center gap-2 rounded-lg bg-brand-500 px-4 py-3 text-sm font-bold text-white transition-colors hover:bg-brand-600"
                onClick={() => save(setting)}
              >
                <FiSave className="h-4 w-4" />
                Save
              </button>
              <button
                className="inline-flex items-center gap-2 rounded-lg bg-lightPrimary px-4 py-3 text-sm font-bold text-gray-700 transition-colors hover:bg-gray-100"
                onClick={() => reset(setting)}
              >
                <FiRotateCcw className="h-4 w-4" />
                Reset
              </button>
            </div>
          </Panel>
        ))}
      </div>
    </div>
  );
}

function draftValue(setting: Setting) {
  if (setting.type === "duration" && typeof setting.value === "string") {
    return setting.value;
  }
  return String(setting.value ?? "");
}

function parseDraft(setting: Setting, value: string) {
  if (setting.type === "integer") {
    const parsed = Number(value);
    if (!Number.isInteger(parsed)) {
      throw new Error(`${setting.label || setting.key} must be an integer`);
    }
    if (typeof setting.min === "number" && parsed < setting.min) {
      throw new Error(`${setting.label || setting.key} must be at least ${setting.min}`);
    }
    return parsed;
  }
  if (setting.type === "duration") {
    if (!value.trim()) {
      throw new Error(`${setting.label || setting.key} must not be empty`);
    }
    return value.trim();
  }
  return JSON.parse(value);
}

function Fact({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <p className="text-xs font-bold uppercase text-gray-600">{label}</p>
      <p className="mt-1 break-all text-sm font-bold text-navy-700">{value}</p>
    </div>
  );
}
