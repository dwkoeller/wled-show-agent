import BuildIcon from "@mui/icons-material/Build";
import ArchiveIcon from "@mui/icons-material/Archive";
import FolderIcon from "@mui/icons-material/Folder";
import MusicNoteIcon from "@mui/icons-material/MusicNote";
import MovieFilterIcon from "@mui/icons-material/MovieFilter";
import RouterIcon from "@mui/icons-material/Router";
import ScheduleIcon from "@mui/icons-material/Schedule";
import SyncAltIcon from "@mui/icons-material/SyncAlt";
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
import { FseqTools } from "./tools/FseqTools";
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
  { label: "xLights", value: "/tools/xlights", icon: <BuildIcon /> },
  { label: "Audio", value: "/tools/audio", icon: <MusicNoteIcon /> },
  { label: "Files", value: "/tools/files", icon: <FolderIcon /> },
  { label: "Packs", value: "/tools/packs", icon: <ArchiveIcon /> },
  { label: "Sequences", value: "/tools/sequences", icon: <SyncAltIcon /> },
  { label: ".fseq", value: "/tools/fseq", icon: <MovieFilterIcon /> },
  { label: "FPP", value: "/tools/fpp", icon: <RouterIcon /> },
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
        <Route path="xlights" element={<XlightsTools />} />
        <Route path="audio" element={<AudioTools />} />
        <Route path="files" element={<FilesTools />} />
        <Route path="packs" element={<PacksTools />} />
        <Route path="sequences" element={<SequenceTools />} />
        <Route path="fseq" element={<FseqTools />} />
        <Route path="fpp" element={<FppTools />} />
        <Route path="scheduler" element={<SchedulerTools />} />
        <Route path="*" element={<Navigate to="/tools/xlights" replace />} />
      </Routes>
    </Stack>
  );
}
