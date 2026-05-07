import { NavLink, Outlet, useLocation } from "react-router-dom";
import { FiActivity, FiCpu, FiMenu, FiSettings, FiX } from "react-icons/fi";
import { useState } from "react";

const navItems = [
  { label: "Overview", path: "/overview", icon: FiActivity },
  { label: "Workers", path: "/workers", icon: FiCpu },
  { label: "Settings", path: "/settings", icon: FiSettings },
];

export default function Shell() {
  const [open, setOpen] = useState(false);
  const location = useLocation();
  const active = navItems.find((item) => location.pathname.startsWith(item.path));

  return (
    <div className="min-h-screen bg-lightPrimary text-navy-700">
      <aside
        className={`fixed inset-y-0 left-0 z-50 flex w-[286px] flex-col bg-white shadow-2xl shadow-gray-200/70 transition-transform duration-200 xl:translate-x-0 ${
          open ? "translate-x-0" : "-translate-x-full"
        }`}
      >
        <div className="flex items-center justify-between px-8 pt-8">
          <div>
            <p className="font-poppins text-[24px] font-bold uppercase tracking-normal text-navy-700">
              NATS <span className="font-medium">Runtime</span>
            </p>
            <p className="mt-1 text-sm font-medium text-gray-600">Local console</p>
          </div>
          <button
            className="rounded-lg p-2 text-gray-600 hover:bg-gray-100 xl:hidden"
            onClick={() => setOpen(false)}
            aria-label="Close navigation"
          >
            <FiX className="h-5 w-5" />
          </button>
        </div>
        <div className="mx-8 my-8 h-px bg-gray-200" />
        <nav className="flex flex-1 flex-col gap-2 px-5">
          {navItems.map((item) => {
            const Icon = item.icon;
            return (
              <NavLink
                key={item.path}
                to={item.path}
                onClick={() => setOpen(false)}
                className={({ isActive }) =>
                  `relative flex items-center gap-4 rounded-lg px-4 py-3 text-sm font-bold transition-colors ${
                    isActive
                      ? "bg-brand-50 text-brand-600"
                      : "text-gray-600 hover:bg-gray-100 hover:text-navy-700"
                  }`
                }
              >
                <Icon className="h-5 w-5" />
                {item.label}
              </NavLink>
            );
          })}
        </nav>
        <div className="mx-5 mb-6 rounded-lg border border-gray-200 bg-lightPrimary p-4">
          <p className="text-xs font-bold uppercase text-gray-600">Runtime</p>
          <p className="mt-1 text-sm font-bold text-navy-700">python-runtime</p>
        </div>
      </aside>
      {open ? (
        <button
          className="fixed inset-0 z-40 bg-navy-900/30 xl:hidden"
          onClick={() => setOpen(false)}
          aria-label="Close navigation backdrop"
        />
      ) : null}
      <div className="min-h-screen xl:pl-[286px]">
        <header className="sticky top-0 z-30 flex items-center justify-between bg-lightPrimary/85 px-4 py-4 backdrop-blur-md md:px-8">
          <div className="flex items-center gap-3">
            <button
              className="rounded-lg bg-white p-3 text-navy-700 shadow-sm xl:hidden"
              onClick={() => setOpen(true)}
              aria-label="Open navigation"
            >
              <FiMenu className="h-5 w-5" />
            </button>
            <div>
              <p className="text-sm font-medium text-gray-600">Runtime API</p>
              <h1 className="font-poppins text-2xl font-bold text-navy-700 md:text-[32px]">
                {active?.label || "Overview"}
              </h1>
            </div>
          </div>
        </header>
        <main className="px-4 pb-10 md:px-8">
          <Outlet />
        </main>
      </div>
    </div>
  );
}
