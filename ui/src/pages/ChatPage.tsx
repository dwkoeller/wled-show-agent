import { useEffect, useRef, useState } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { Alert, Box, Button, Chip, CircularProgress, Dialog, DialogActions, DialogContent, DialogTitle, Drawer, FormControl, IconButton, InputLabel, MenuItem, Select, Stack, TextField, Tooltip, Typography } from "@mui/material";
import AddIcon from "@mui/icons-material/Add";
import ArrowUpwardIcon from "@mui/icons-material/ArrowUpward";
import HistoryIcon from "@mui/icons-material/History";
import TuneIcon from "@mui/icons-material/Tune";
import CloseIcon from "@mui/icons-material/Close";
import DeleteOutlineIcon from "@mui/icons-material/DeleteOutline";
import StopRoundedIcon from "@mui/icons-material/StopRounded";
import AutoAwesomeIcon from "@mui/icons-material/AutoAwesome";
import LinkIcon from "@mui/icons-material/Link";
import AttachFileIcon from "@mui/icons-material/AttachFile";
import { api, csrfHeaders } from "../api";
import { useAuth } from "../auth";
import { VoiceInputButton } from "../components/VoiceInputButton";
import "../chat.css";

type Model = { id: string; model: string; displayName: string; isDefault?: boolean; defaultReasoningEffort?: string; supportedReasoningEfforts?: { reasoningEffort: string; description: string }[] };
type Thread = { id: string; title: string; model?: string };
type ChatItem = { id: string; type: string; text?: string; content?: { type: string; text?: string }[]; tool?: string; arguments?: unknown; status?: string; contentItems?: unknown[]; success?: boolean };
type Account = { connected: boolean; account?: { email?: string; planType?: string; type?: string } };
type Login = { verificationUrl: string; userCode: string };
type ChatEvent = { method: string; params: { item?: ChatItem; itemId?: string; delta?: string; message?: string; error?: { message?: string }; turn?: { status?: string; error?: { message?: string } } } };

function itemText(item: ChatItem) {
  return item.text || (item.content || []).map((c) => c.text || "").join("\n");
}

export function ChatPage() {
  const { user } = useAuth();
  const [account, setAccount] = useState<Account | null>(null);
  const [models, setModels] = useState<Model[]>([]);
  const [model, setModel] = useState("");
  const [effort, setEffort] = useState("");
  const [threads, setThreads] = useState<Thread[]>([]);
  const [thread, setThread] = useState<string | null>(null);
  const [items, setItems] = useState<ChatItem[]>([]);
  const [text, setText] = useState("");
  const [busy, setBusy] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [sheet, setSheet] = useState<"history" | "settings" | null>(null);
  const [login, setLogin] = useState<Login | null>(null);
  const [connecting, setConnecting] = useState(false);
  const [stopShow, setStopShow] = useState(false);
  const [attachments, setAttachments] = useState<string[]>([]);
  const [uploading, setUploading] = useState(false);
  const fileInput = useRef<HTMLInputElement>(null);
  const autoScroll = useRef(true);
  const bottom = useRef<HTMLDivElement>(null);
  const controller = useRef<AbortController | null>(null);
  const lock = useRef(false);
  const currentModel = models.find((m) => m.model === model);

  const fail = (e: unknown) => setError(e instanceof Error ? e.message : String(e));
  const refreshThreads = async () => {
    const res = await api<{ threads: Thread[] }>("/api/chat/threads");
    setThreads(res.threads);
  };
  const refreshAccount = async () => {
    const res = await api<Account>("/api/chat/account");
    setAccount(res);
    if (res.connected) {
      const list = await api<{ data: Model[] }>("/api/chat/models");
      setModels(list.data);
      setModel((old) => old || (list.data.find((m) => m.isDefault) || list.data[0])?.model || "");
    }
    return res;
  };

  useEffect(() => {
    Promise.all([refreshAccount(), refreshThreads()]).catch(fail).finally(() => setLoading(false));
    return () => controller.current?.abort();
  }, []);
  useEffect(() => {
    if (!login) return;
    const timer = window.setInterval(() => {
      refreshAccount().then((res) => { if (res.connected) setLogin(null); }).catch(fail);
    }, 4000);
    return () => window.clearInterval(timer);
  }, [login]);
  useEffect(() => { if (autoScroll.current) bottom.current?.scrollIntoView({ block: "end" }); }, [items, busy]);
  useEffect(() => { setEffort(currentModel?.defaultReasoningEffort || ""); }, [model]);

  const connect = async () => {
    setConnecting(true); setError(null);
    try { setLogin(await api<Login>("/api/chat/login", { method: "POST", json: {} })); }
    catch (e) { fail(e); }
    finally { setConnecting(false); }
  };
  const loadThread = async (id: string) => {
    if (busy) return;
    setLoading(true); setSheet(null); setError(null);
    try {
      const history = await api<{ turns: { items: ChatItem[] }[] }>(`/api/chat/threads/${id}`);
      setThread(id);
      setItems((history.turns || []).flatMap((t) => t.items || []).filter((i) => ["userMessage", "agentMessage", "dynamicToolCall"].includes(i.type)));
    } catch (e) { fail(e); }
    finally { setLoading(false); }
  };
  const fresh = () => { if (!busy) { setThread(null); setItems([]); setText(""); setAttachments([]); setError(null); setSheet(null); } };
  const remove = async (id: string) => {
    try {
      await api(`/api/chat/threads/${id}`, { method: "DELETE" });
      if (thread === id) fresh();
      await refreshThreads();
    } catch (e) { fail(e); }
  };
  const update = (event: ChatEvent) => {
    const { method, params } = event;
    if (method === "item/agentMessage/delta") {
      setItems((old) => {
        const exists = old.some((i) => i.id === params.itemId);
        return exists ? old.map((i) => i.id === params.itemId ? { ...i, text: (i.text || "") + (params.delta || "") } : i)
          : [...old, { id: params.itemId!, type: "agentMessage", text: params.delta || "" }];
      });
    } else if (method === "item/started" || method === "item/completed") {
      const item = params.item;
      if (!item || !["agentMessage", "dynamicToolCall"].includes(item.type)) return;
      setItems((old) => old.some((i) => i.id === item.id) ? old.map((i) => i.id === item.id ? { ...i, ...item } : i) : [...old, item]);
    } else if (method === "error" || method === "chat/error" || (method === "turn/completed" && params.turn?.error)) {
      setError(params.message || params.error?.message || params.turn?.error?.message || "The reply could not be completed.");
    }
  };
  const upload = async (file: File) => {
    if (file.size > 128 * 1024 * 1024) { setError("Choose a file smaller than 128 MB."); return; }
    setUploading(true); setError(null);
    try {
      const ext = file.name.split(".").pop()?.toLowerCase();
      const dir = ext === "xsq" ? "xlights" : ext === "json" ? "sequences" : "audio";
      const form = new FormData(); form.append("file", file); form.append("dir", dir);
      form.append("filename", `${Date.now()}-${file.name}`);
      const response = await fetch("/api/files/upload", { method: "POST", credentials: "include", headers: csrfHeaders("POST"), body: form });
      const result = await response.json();
      if (!response.ok) throw new Error(result.detail || "Upload failed");
      setAttachments((old) => [...old, result.path]);
    } catch (e) { fail(e); }
    finally { setUploading(false); if (fileInput.current) fileInput.current.value = ""; }
  };
  const send = async (value = text) => {
    const prompt = [value.trim(), attachments.length ? `Attached show files: ${attachments.join(", ")}` : ""].filter(Boolean).join("\n\n");
    if (!prompt || lock.current || uploading || !account?.connected) return;
    lock.current = true; autoScroll.current = true; setBusy(true); setError(null); setText(""); setAttachments([]);
    setItems((old) => [...old, { id: `local-${Date.now()}`, type: "userMessage", text: prompt }]);
    let activeThread = thread;
    try {
      if (!activeThread) {
        const created = await api<Thread>("/api/chat/threads", { method: "POST", json: { model: model || null } });
        activeThread = created.id; setThread(created.id);
      }
      controller.current = new AbortController();
      const response = await fetch(`/api/chat/threads/${activeThread}/messages`, {
        method: "POST", credentials: "include", signal: controller.current.signal,
        headers: { "Content-Type": "application/json", ...csrfHeaders("POST") },
        body: JSON.stringify({ text: prompt, model: model || null, effort: effort || null }),
      });
      if (!response.ok) {
        const body = await response.json().catch(() => ({}));
        throw new Error(body.detail || body.error || `Request failed (${response.status})`);
      }
      if (!response.body) throw new Error("Streaming is not supported by this browser.");
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      while (true) {
        const result = await reader.read();
        if (result.done) break;
        buffer += decoder.decode(result.value, { stream: true });
        let end: number;
        while ((end = buffer.indexOf("\n\n")) >= 0) {
          const block = buffer.slice(0, end); buffer = buffer.slice(end + 2);
          for (const line of block.split("\n")) if (line.startsWith("data: ")) update(JSON.parse(line.slice(6)));
        }
      }
    } catch (e) {
      if (!(e instanceof DOMException && e.name === "AbortError")) { fail(e); setText(prompt); }
    } finally {
      controller.current = null; lock.current = false; setBusy(false);
      refreshThreads().catch(fail);
    }
  };
  const stopReply = async () => {
    try { if (thread) await api(`/api/chat/threads/${thread}/stop`, { method: "POST", json: {} }); }
    catch (e) { fail(e); }
    finally { controller.current?.abort(); }
  };
  const stopEverything = async () => {
    setStopShow(true); setError(null);
    const results = await Promise.allSettled([
      api<{ ok: boolean }>("/api/fleet/stop_all", { method: "POST", json: { include_self: true } }),
      api<{ ok: boolean }>("/api/fpp/playlist/stop", { method: "POST", json: {} }),
    ]);
    const failed = results.some((r) => r.status === "rejected" || !r.value.ok);
    if (failed) setError("Some devices did not confirm stopping. Check the dashboard for details.");
    else setItems((old) => [...old, { id: `stop-${Date.now()}`, type: "agentMessage", text: "The show agent and Falcon Player confirmed the stop request." }]);
    setStopShow(false);
  };

  return <Box className="chat-page">
    <Box className="chat-toolbar">
      <Tooltip title="Conversations"><IconButton aria-label="Conversations" onClick={() => setSheet("history")}><HistoryIcon /></IconButton></Tooltip>
      <Button className="model-button" onClick={() => setSheet("settings")} endIcon={<TuneIcon fontSize="small" />}>
        <span>{currentModel?.displayName || "Show assistant"}<small>{account?.connected ? "ChatGPT connected" : "Connect your ChatGPT account"}</small></span>
      </Button>
      <Tooltip title="New conversation"><IconButton aria-label="New conversation" disabled={busy} onClick={fresh}><AddIcon /></IconButton></Tooltip>
      <Button color="error" size="small" variant="outlined" startIcon={<StopRoundedIcon />} onClick={stopEverything} disabled={stopShow}>Stop show</Button>
    </Box>
    {error && <Alert severity="error" onClose={() => setError(null)} sx={{ mx: 2, mt: 1 }}>{error}</Alert>}
    <Box className="chat-transcript" role="log" aria-label="Conversation" aria-live="polite" aria-busy={busy} onScroll={(e) => { const el = e.currentTarget; autoScroll.current = el.scrollHeight - el.scrollTop - el.clientHeight < 100; }}>
      {loading ? <Box className="chat-loading"><CircularProgress size={26} /><span>Getting your assistant ready…</span></Box> : items.length === 0 ? <Box className="chat-welcome">
        <div className="assistant-mark"><AutoAwesomeIcon /></div>
        <div className="eyebrow">YOUR SHOW, IN CONVERSATION</div>
        <h1>What should we<br /><span>bring to light?</span></h1>
        <p>Build a sequence, cue Falcon Player, or set the mood.<br className="desktop-break" /> Your whole show starts here.</p>
        {!account?.connected ? <Box className="connect-card">
          <LinkIcon color="primary" /><Typography variant="subtitle1" fontWeight={700}>Connect ChatGPT</Typography>
          <Typography variant="body2" color="text.secondary">Sign in to use the models available through your subscription. Conversation history is saved on this show server.</Typography>
          <Typography variant="caption" color="text.secondary">Messages and tool results are sent to OpenAI to generate replies.</Typography>
          <Button variant="contained" onClick={connect} disabled={connecting || user?.role !== "admin"}>{connecting ? "Connecting…" : "Sign in with ChatGPT"}</Button>
          {user?.role !== "admin" && <Typography variant="caption">Ask your show administrator to connect the account.</Typography>}
        </Box> : <div className="suggestion-grid">{[
          ["Check the show", "Check the status of my controllers and Falcon Player."],
          ["Find a playlist", "What playlists are available in Falcon Player?"],
          ["Create a look", "Help me create a warm, festive look for the tree."],
          ["Plan tonight", "Help me plan a lighting schedule for tonight."],
        ].map(([label, prompt]) => <button key={label} onClick={() => send(prompt)}><span>{label}</span><small>{prompt}</small><span className="suggestion-arrow">↗</span></button>)}</div>}
      </Box> : <div className="message-list">{items.map((item) => item.type === "dynamicToolCall" ? <details className="tool-card" key={item.id}>
        <summary><span className={item.status === "inProgress" ? "tool-dot running" : "tool-dot"} />{item.tool === "show_catalog" ? "Exploring show controls" : "Using show controls"}<small>{item.status === "inProgress" ? "Working" : item.success === false ? "Needs attention" : "Finished"}</small></summary>
        <pre>{JSON.stringify({ request: item.arguments, result: item.contentItems }, null, 2)}</pre>
      </details> : <article key={item.id} className={`message ${item.type === "userMessage" ? "user-message" : "assistant-message"}`}>
        {item.type === "agentMessage" && <div className="message-label"><AutoAwesomeIcon sx={{ fontSize: 14 }} />SHOW ASSISTANT</div>}
        <div className="message-body"><ReactMarkdown remarkPlugins={[remarkGfm]} components={{ a: ({ children, ...props }) => <a {...props} target="_blank" rel="noopener noreferrer">{children}</a> }}>{itemText(item)}</ReactMarkdown></div>
      </article>)}</div>}
      {busy && <div className="thinking"><span /><span /><span /><small>Working on your show</small></div>}
      <div ref={bottom} />
    </Box>
    <Box className="chat-composer-wrap">
      {!!attachments.length && <Stack direction="row" gap={1} sx={{ overflowX: "auto", pb: 1 }}>{attachments.map((path) => <Chip key={path} size="small" label={path.split("/").pop()} onDelete={() => setAttachments((old) => old.filter((p) => p !== path))} />)}</Stack>}
      <input type="file" ref={fileInput} hidden accept=".wav,.mp3,.ogg,.flac,.m4a,.aac,.xsq,.json" onChange={(e) => { const file = e.target.files?.[0]; if (file) void upload(file); }} />
      <form className="chat-composer" onSubmit={(e) => { e.preventDefault(); void send(); }}>
        <TextField value={text} onChange={(e) => setText(e.target.value)} placeholder="Ask your show assistant…" aria-label="Message" multiline maxRows={5} fullWidth disabled={!account?.connected} variant="standard" slotProps={{ input: { disableUnderline: true }, htmlInput: { "aria-label": "Message", maxLength: 32000 } }}
          onKeyDown={(e) => { if (e.key === "Enter" && (e.ctrlKey || e.metaKey)) { e.preventDefault(); void send(); } }} />
        <div className="composer-actions"><IconButton aria-label="Attach audio or sequence" disabled={uploading || busy || !account?.connected} onClick={() => fileInput.current?.click()}>{uploading ? <CircularProgress size={18} /> : <AttachFileIcon />}</IconButton><VoiceInputButton mode="browser" onText={(value) => setText((old) => old ? `${old} ${value}` : value)} onError={setError} disabled={busy || !account?.connected} />
          {busy ? <IconButton className="send-button" aria-label="Stop reply" onClick={stopReply}><StopRoundedIcon /></IconButton> : <IconButton className="send-button" aria-label="Send message" type="submit" disabled={(!text.trim() && !attachments.length) || uploading || !account?.connected}><ArrowUpwardIcon /></IconButton>}
        </div>
      </form>
      <div className="composer-note">{busy ? "You can stop this reply at any time." : "Describe what you want. Your assistant handles the controls."}</div>
    </Box>
    <Drawer anchor="bottom" open={Boolean(sheet)} onClose={() => setSheet(null)} slotProps={{ paper: { className: "chat-sheet" } }}>
      <div className="sheet-handle" /><Stack direction="row" alignItems="center" justifyContent="space-between"><Typography variant="h6">{sheet === "history" ? "Conversations" : "Assistant settings"}</Typography><IconButton aria-label="Close panel" onClick={() => setSheet(null)}><CloseIcon /></IconButton></Stack>
      {sheet === "history" ? <Stack spacing={1} sx={{ mt: 2 }}><Button variant="contained" startIcon={<AddIcon />} onClick={fresh} disabled={busy}>New conversation</Button>{threads.length === 0 && <Typography color="text.secondary" sx={{ py: 3 }}>Your conversations will appear here.</Typography>}{threads.map((t) => <Stack key={t.id} direction="row" alignItems="center"><Button sx={{ flex: 1, justifyContent: "flex-start", textAlign: "left", minHeight: 48 }} onClick={() => loadThread(t.id)} disabled={busy}>{t.title}</Button><IconButton aria-label={`Delete ${t.title}`} disabled={busy} onClick={() => remove(t.id)}><DeleteOutlineIcon /></IconButton></Stack>)}</Stack> : <Stack spacing={2.5} sx={{ mt: 2 }}>
        <Chip label={account?.connected ? `ChatGPT ${account.account?.planType || "subscription"} connected` : "ChatGPT not connected"} color={account?.connected ? "success" : "default"} variant="outlined" />
        {account?.account?.email && <Typography variant="body2" color="text.secondary">{account.account.email}</Typography>}
        {models.length > 0 && <FormControl fullWidth><InputLabel id="chat-model-label">Model</InputLabel><Select labelId="chat-model-label" value={model} label="Model" onChange={(e) => setModel(e.target.value)} disabled={busy}>{models.map((m) => <MenuItem key={m.id} value={m.model}>{m.displayName}</MenuItem>)}</Select></FormControl>}
        {!!currentModel?.supportedReasoningEfforts?.length && <FormControl fullWidth><InputLabel id="chat-effort-label">Thinking effort</InputLabel><Select labelId="chat-effort-label" value={effort} label="Thinking effort" onChange={(e) => setEffort(e.target.value)} disabled={busy}>{currentModel.supportedReasoningEfforts.map((e) => <MenuItem key={e.reasoningEffort} value={e.reasoningEffort}>{e.reasoningEffort}</MenuItem>)}</Select></FormControl>}
        <Typography variant="body2" color="text.secondary">Models are loaded from your signed-in account. Availability and usage limits follow your subscription.</Typography>
        {user?.role === "admin" && (account?.connected ? <Button color="inherit" onClick={async () => { try { await api("/api/chat/logout", { method: "POST", json: {} }); setModels([]); setModel(""); await refreshAccount(); } catch (e) { fail(e); } }} disabled={busy}>Disconnect ChatGPT</Button> : <Button variant="contained" onClick={connect} disabled={connecting}>Sign in with ChatGPT</Button>)}
      </Stack>}
    </Drawer>
    <Dialog open={Boolean(login)} onClose={() => setLogin(null)} fullWidth maxWidth="xs"><DialogTitle>Connect your ChatGPT account</DialogTitle><DialogContent><Typography>Open the sign-in page and enter this code:</Typography><Typography component="div" sx={{ fontFamily: "monospace", fontSize: 30, letterSpacing: 3, textAlign: "center", py: 3 }}>{login?.userCode}</Typography><Button fullWidth variant="contained" href={login?.verificationUrl || "#"} target="_blank" rel="noopener noreferrer">Open ChatGPT sign-in</Button><Typography variant="body2" color="text.secondary" sx={{ mt: 2 }}>This screen updates automatically after you sign in. If device sign-in is disabled, enable it in your ChatGPT security settings.</Typography></DialogContent><DialogActions><Button onClick={() => setLogin(null)}>Close</Button></DialogActions></Dialog>
  </Box>;
}
