import { CssBaseline } from "@mui/material";
import { ThemeProvider, createTheme } from "@mui/material/styles";
import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { App } from "./App";
import { AuthProvider } from "./auth";

const theme = createTheme({
  shape: { borderRadius: 12 },
  typography: { fontFamily: '"Inter", -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif', button: { textTransform: "none", fontWeight: 600 } },
  components: { MuiButton: { styleOverrides: { root: { minHeight: 44 } } }, MuiCssBaseline: { styleOverrides: { body: { overscrollBehavior: "none" } } } },
  palette: {
    mode: "dark",
    primary: { main: "#9aefca", contrastText: "#102b22" },
    background: { default: "#0b141e", paper: "#121e2a" },
    text: { primary: "#eef3f5", secondary: "#8997a8" },
  },
});

if ("serviceWorker" in navigator) {
  window.addEventListener("load", () => {
    navigator.serviceWorker.register("/sw.js").catch(() => {
      // ignore
    });
  });
}

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <BrowserRouter >
        <AuthProvider>
          <App />
        </AuthProvider>
      </BrowserRouter>
    </ThemeProvider>
  </React.StrictMode>,
);
