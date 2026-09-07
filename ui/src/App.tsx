import ChatIcon from "@mui/icons-material/Chat";
import BuildIcon from "@mui/icons-material/Build";
import DashboardIcon from "@mui/icons-material/Dashboard";
import ListAltIcon from "@mui/icons-material/ListAlt";
import LogoutIcon from "@mui/icons-material/Logout";
import {
  AppBar,
  Badge,
  BottomNavigation,
  BottomNavigationAction,
  Box,
  CircularProgress,
  Container,
  IconButton,
  Paper,
  Toolbar,
  Typography,
} from "@mui/material";
import React, { lazy, Suspense, useMemo } from "react";
import {
  Navigate,
  Route,
  Routes,
  useLocation,
  useNavigate,
} from "react-router-dom";
import { useAuth } from "./auth";
import { ChatPage } from "./pages/ChatPage";
const DashboardPage = lazy(() => import("./pages/DashboardPage").then((m) => ({ default: m.DashboardPage })));
const JobsPage = lazy(() => import("./pages/JobsPage").then((m) => ({ default: m.JobsPage })));
import { LoginPage } from "./pages/LoginPage";
const ToolsPage = lazy(() => import("./pages/ToolsPage").then((m) => ({ default: m.ToolsPage })));
import { useServerEvents } from "./hooks/useServerEvents";

function RequireAuth({ children }: { children: React.ReactNode }) {
  const { user, loading, config } = useAuth();
  const authRequired = config ? config.auth_enabled : true;
  if (loading) {
    return (
      <Box sx={{ display: "flex", justifyContent: "center", mt: 10 }}>
        <CircularProgress />
      </Box>
    );
  }
  if (authRequired && !user) {
    return <Navigate to="/login" replace />;
  }
  return <>{children}</>;
}

function EventsStatusBadge() {
  const { connected, enabled } = useServerEvents();
  const color = enabled ? (connected ? "success" : "warning") : "default";
  const label = enabled
    ? connected
      ? "Events connected"
      : "Events disconnected"
    : "Events disabled";
  return (
    <Box component="span" title={label} sx={{ display: "inline-flex" }}>
      <Badge color={color} variant="dot" overlap="circular">
        <Box sx={{ width: 12, height: 12 }} />
      </Badge>
    </Box>
  );
}

export function App() {
  const { user, logout } = useAuth();
  const nav = useNavigate();
  const loc = useLocation();

  const navValue = useMemo(() => {
    if (loc.pathname.startsWith("/dashboard")) return "/dashboard";
    if (loc.pathname.startsWith("/jobs")) return "/jobs";
    if (loc.pathname.startsWith("/tools")) return "/tools";
    return "/";
  }, [loc.pathname]);

  return (
    <Box sx={{ pb: "calc(64px + env(safe-area-inset-bottom))" }}>
      <AppBar position="fixed" elevation={0} sx={{ background: "#0b141e", borderBottom: "1px solid #ffffff0c" }}>
        <Toolbar sx={{ display: "flex", justifyContent: "space-between" }}>
          <Typography variant="h6" component="div">
            WLED Show Agent
          </Typography>
          {user ? (
            <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
              <EventsStatusBadge />
              <IconButton
                color="inherit"
                onClick={() => logout().then(() => nav("/login"))}
              >
                <LogoutIcon />
              </IconButton>
            </Box>
          ) : null}
        </Toolbar>
      </AppBar>
      <Toolbar />
      <Container maxWidth="md" sx={{ mt: loc.pathname === "/" ? 0 : 2, px: loc.pathname === "/" ? "0 !important" : undefined }}>
        <Suspense fallback={<Box sx={{ p: 5, textAlign: "center" }}><CircularProgress /></Box>}>
        <Routes>
          <Route path="/login" element={<LoginPage />} />
          <Route
            path="/"
            element={
              <RequireAuth>
                <ChatPage />
              </RequireAuth>
            }
          />
          <Route
            path="/dashboard"
            element={
              <RequireAuth>
                <DashboardPage />
              </RequireAuth>
            }
          />
          <Route
            path="/jobs"
            element={
              <RequireAuth>
                <JobsPage />
              </RequireAuth>
            }
          />
          <Route
            path="/tools/*"
            element={
              <RequireAuth>
                <ToolsPage />
              </RequireAuth>
            }
          />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
        </Suspense>
      </Container>

      <Paper
        sx={{ position: "fixed", bottom: 0, left: 0, right: 0, zIndex: 1100, pb: "env(safe-area-inset-bottom)", background: "#0e1823", borderTop: "1px solid #ffffff0c" }}
        elevation={3}
      >
        <BottomNavigation
          sx={{ height: 64, background: "transparent", maxWidth: 720, mx: "auto" }}
          value={navValue}
          onChange={(_, value) => nav(value)}
          showLabels
        >
          <BottomNavigationAction
            label="Dashboard"
            value="/dashboard"
            icon={<DashboardIcon />}
          />
          <BottomNavigationAction
            label="Chat"
            value="/"
            icon={<ChatIcon />}
          />
          <BottomNavigationAction
            label="Tools"
            value="/tools"
            icon={<BuildIcon />}
          />
          <BottomNavigationAction
            label="Jobs"
            value="/jobs"
            icon={<ListAltIcon />}
          />
        </BottomNavigation>
      </Paper>
    </Box>
  );
}
