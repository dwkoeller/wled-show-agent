import BuildIcon from "@mui/icons-material/Build";
import ArchiveIcon from "@mui/icons-material/Archive";
import FolderIcon from "@mui/icons-material/Folder";
import MusicNoteIcon from "@mui/icons-material/MusicNote";
import MovieFilterIcon from "@mui/icons-material/MovieFilter";
import TheaterComedyIcon from "@mui/icons-material/TheaterComedy";
import RouterIcon from "@mui/icons-material/Router";
import ScheduleIcon from "@mui/icons-material/Schedule";
import StorageIcon from "@mui/icons-material/Storage";
import SyncAltIcon from "@mui/icons-material/SyncAlt";
import HubIcon from "@mui/icons-material/Hub";
import HistoryIcon from "@mui/icons-material/History";
import SecurityIcon from "@mui/icons-material/Security";
import WifiTetheringIcon from "@mui/icons-material/WifiTethering";
import BackupIcon from "@mui/icons-material/Backup";
import EventNoteIcon from "@mui/icons-material/EventNote";
import EqualizerIcon from "@mui/icons-material/Equalizer";
import InsightsIcon from "@mui/icons-material/Insights";
import {
  Box,
  Card,
  CardContent,
  Stack,
  Tab,
  Tabs,
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
import { AudioTools } from "./tools/AudioTools";
import { FilesTools } from "./tools/FilesTools";
import { FppTools } from "./tools/FppTools";
import { LedfxTools } from "./tools/LedfxTools";
import { FseqTools } from "./tools/FseqTools";
import { FleetTools } from "./tools/FleetTools";
import { AuditTools } from "./tools/AuditTools";
import { AuthTools } from "./tools/AuthTools";
import { BackupTools } from "./tools/BackupTools";
import { EventsTools } from "./tools/EventsTools";
import { MetricsTools } from "./tools/MetricsTools";
import { MetadataTools } from "./tools/MetadataTools";
import { MqttTools } from "./tools/MqttTools";
import { OrchestrationTools } from "./tools/OrchestrationTools";
import { PacksTools } from "./tools/PacksTools";
import { SchedulerTools } from "./tools/SchedulerTools";
import { SequenceTools } from "./tools/SequenceTools";
import { XlightsTools } from "./tools/XlightsTools";

type ToolTab = {
  label: string;
  value: string;
  icon: React.ReactElement;
};

const tabs: ToolTab[] = [
  { label: "Fleet", value: "/tools/fleet", icon: <HubIcon /> },
  { label: "Audit", value: "/tools/audit", icon: <HistoryIcon /> },
  { label: "Events", value: "/tools/events", icon: <EventNoteIcon /> },
  { label: "Metrics", value: "/tools/metrics", icon: <InsightsIcon /> },
  { label: "Auth", value: "/tools/auth", icon: <SecurityIcon /> },
  { label: "Backup", value: "/tools/backup", icon: <BackupIcon /> },
  { label: "xLights", value: "/tools/xlights", icon: <BuildIcon /> },
  { label: "Audio", value: "/tools/audio", icon: <MusicNoteIcon /> },
  { label: "Files", value: "/tools/files", icon: <FolderIcon /> },
  { label: "Packs", value: "/tools/packs", icon: <ArchiveIcon /> },
  { label: "Sequences", value: "/tools/sequences", icon: <SyncAltIcon /> },
  { label: "Metadata", value: "/tools/meta", icon: <StorageIcon /> },
  { label: ".fseq", value: "/tools/fseq", icon: <MovieFilterIcon /> },
  {
    label: "Orchestration",
    value: "/tools/orchestration",
    icon: <TheaterComedyIcon />,
  },
  { label: "FPP", value: "/tools/fpp", icon: <RouterIcon /> },
  { label: "LedFx", value: "/tools/ledfx", icon: <EqualizerIcon /> },
  { label: "MQTT", value: "/tools/mqtt", icon: <WifiTetheringIcon /> },
  { label: "Scheduler", value: "/tools/scheduler", icon: <ScheduleIcon /> },
];

export function ToolsPage() {
  const nav = useNavigate();
  const loc = useLocation();

  const current = useMemo(() => {
    const p = loc.pathname;
    const match = tabs.find((t) => p.startsWith(t.value));
    return match?.value ?? "/tools/xlights";
  }, [loc.pathname]);

  return (
    <Stack spacing={2}>
      <Card>
        <CardContent>
          <Typography variant="h6">Tools</Typography>
          <Typography variant="body2" color="text.secondary">
            Imports, analysis, sequence generation, and export helpers.
          </Typography>
        </CardContent>
        <Box sx={{ borderBottom: 1, borderColor: "divider" }}>
          <Tabs
            value={current}
            onChange={(_, v) => nav(v)}
            variant="scrollable"
            scrollButtons="auto"
            allowScrollButtonsMobile
          >
            {tabs.map((t) => (
              <Tab
                key={t.value}
                icon={t.icon}
                iconPosition="start"
                label={t.label}
                value={t.value}
              />
            ))}
          </Tabs>
        </Box>
      </Card>

      <Routes>
        <Route path="fleet" element={<FleetTools />} />
        <Route path="audit" element={<AuditTools />} />
        <Route path="events" element={<EventsTools />} />
        <Route path="metrics" element={<MetricsTools />} />
        <Route path="auth" element={<AuthTools />} />
        <Route path="backup" element={<BackupTools />} />
        <Route path="xlights" element={<XlightsTools />} />
        <Route path="audio" element={<AudioTools />} />
        <Route path="files" element={<FilesTools />} />
        <Route path="packs" element={<PacksTools />} />
        <Route path="sequences" element={<SequenceTools />} />
        <Route path="meta" element={<MetadataTools />} />
        <Route path="fseq" element={<FseqTools />} />
        <Route path="orchestration" element={<OrchestrationTools />} />
        <Route path="fpp" element={<FppTools />} />
        <Route path="ledfx" element={<LedfxTools />} />
        <Route path="mqtt" element={<MqttTools />} />
        <Route path="scheduler" element={<SchedulerTools />} />
        <Route path="*" element={<Navigate to="/tools/xlights" replace />} />
      </Routes>
    </Stack>
  );
}
