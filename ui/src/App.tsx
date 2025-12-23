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
import React, { useMemo } from "react";
import {
  Navigate,
  Route,
  Routes,
  useLocation,
  useNavigate,
} from "react-router-dom";
import { useAuth } from "./auth";
import { ChatPage } from "./pages/ChatPage";
import { DashboardPage } from "./pages/DashboardPage";
import { JobsPage } from "./pages/JobsPage";
import { LoginPage } from "./pages/LoginPage";
import { ToolsPage } from "./pages/ToolsPage";
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
    if (loc.pathname.startsWith("/chat")) return "/chat";
    if (loc.pathname.startsWith("/jobs")) return "/jobs";
    if (loc.pathname.startsWith("/tools")) return "/tools";
    return "/";
  }, [loc.pathname]);

  return (
    <Box sx={{ pb: 8 }}>
      <AppBar position="fixed">
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
      <Container maxWidth="md" sx={{ mt: 2 }}>
        <Routes>
          <Route path="/login" element={<LoginPage />} />
          <Route
            path="/"
            element={
              <RequireAuth>
                <DashboardPage />
              </RequireAuth>
            }
          />
          <Route
            path="/chat"
            element={
              <RequireAuth>
                <ChatPage />
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
      </Container>

      <Paper
        sx={{ position: "fixed", bottom: 0, left: 0, right: 0 }}
        elevation={3}
      >
        <BottomNavigation
          value={navValue}
          onChange={(_, value) => nav(value)}
          showLabels
        >
          <BottomNavigationAction
            label="Dashboard"
            value="/"
            icon={<DashboardIcon />}
          />
          <BottomNavigationAction
            label="Chat"
            value="/chat"
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
