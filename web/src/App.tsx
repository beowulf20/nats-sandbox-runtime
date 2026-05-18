import { Navigate, Route, Routes } from "react-router-dom";
import Shell from "./components/Shell";
import Overview from "./pages/Overview";
import Settings from "./pages/Settings";
import Snapshots from "./pages/Snapshots";
import Workers from "./pages/Workers";
import Workspaces from "./pages/Workspaces";

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<Shell />}>
        <Route index element={<Navigate to="/overview" replace />} />
        <Route path="overview" element={<Overview />} />
        <Route path="workers" element={<Workers />} />
        <Route path="snapshots" element={<Snapshots />} />
        <Route path="workspaces" element={<Workspaces />} />
        <Route path="settings" element={<Settings />} />
        <Route path="*" element={<Navigate to="/overview" replace />} />
      </Route>
    </Routes>
  );
}
