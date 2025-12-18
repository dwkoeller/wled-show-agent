import React, {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useState,
} from "react";
import { api } from "./api";

export type AuthConfig = {
  ok: boolean;
  version: string;
  ui_enabled: boolean;
  auth_enabled: boolean;
  totp_enabled: boolean;
  openai_enabled: boolean;
  fpp_enabled: boolean;
  peers_configured: number;
};

export type AuthUser = { username: string };

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
      const me = await api<{ ok: boolean; user: AuthUser }>("/v1/auth/me", {
        method: "GET",
      });
      setUser(me.user);
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
