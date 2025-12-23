import React, {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useState,
} from "react";
import { api, setCsrfConfig } from "./api";

export type AuthConfig = {
  ok: boolean;
  version: string;
  ui_enabled: boolean;
  auth_enabled: boolean;
  totp_enabled: boolean;
  csrf_enabled?: boolean;
  csrf_cookie_name?: string;
  csrf_header_name?: string;
  roles?: string[];
  openai_enabled: boolean;
  fpp_enabled: boolean;
  ledfx_enabled?: boolean;
  mqtt_enabled?: boolean;
  peers_configured: number;
};

export type AuthUser = { username: string; role?: string; session_id?: string };

type AuthState = {
  config: AuthConfig | null;
  user: AuthUser | null;
  loading: boolean;
  refresh: () => Promise<void>;
  login: (args: {
    username: string;
    password: string;
    totp?: string;
  }) => Promise<void>;
  logout: () => Promise<void>;
};

const AuthContext = createContext<AuthState | null>(null);

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [config, setConfig] = useState<AuthConfig | null>(null);
  const [user, setUser] = useState<AuthUser | null>(null);
  const [loading, setLoading] = useState(true);

  const refresh = async () => {
    try {
      const me = await api<{ ok: boolean; user: AuthUser; session_id?: string }>(
        "/v1/auth/me",
        {
          method: "GET",
        },
      );
      const u = me.user || null;
      if (u && me.session_id) {
        u.session_id = me.session_id;
      }
      setUser(u);
    } catch {
      setUser(null);
    }
  };

  const login = async (args: {
    username: string;
    password: string;
    totp?: string;
  }) => {
    await api("/v1/auth/login", { method: "POST", json: args });
    await refresh();
  };

  const logout = async () => {
    await api("/v1/auth/logout", { method: "POST", json: {} });
    setUser(null);
  };

  useEffect(() => {
    let mounted = true;
    (async () => {
      try {
        const cfg = await api<AuthConfig>("/v1/auth/config", { method: "GET" });
        if (mounted) setConfig(cfg);
        setCsrfConfig({
          enabled: Boolean(cfg.csrf_enabled),
          cookieName: cfg.csrf_cookie_name || "wsa_csrf",
          headerName: cfg.csrf_header_name || "X-CSRF-Token",
        });
      } catch {
        if (mounted) setConfig(null);
      } finally {
        if (mounted) {
          await refresh();
          setLoading(false);
        }
      }
    })();
    return () => {
      mounted = false;
    };
  }, []);

  const value = useMemo<AuthState>(
    () => ({ config, user, loading, refresh, login, logout }),
    [config, user, loading],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthState {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used within AuthProvider");
  return ctx;
}
