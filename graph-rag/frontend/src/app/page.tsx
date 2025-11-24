/**
 * Main page - chat interface with sidebar.
 */

"use client";

import React from "react";
import { ChatInterface } from "@/components/chat-interface";
import { AppSidebar } from "@/components/app-sidebar";
import { ProjectSettingsDialog } from "@/components/project-settings-dialog";
import {
  SidebarProvider,
  SidebarTrigger,
  SidebarInset,
} from "@/components/ui/sidebar";

export default function Home() {
  const [currentProjectId, setCurrentProjectId] =
    React.useState<string>("default");
  const [settingsProjectId, setSettingsProjectId] = React.useState<
    string | null
  >(null);

  return (
    <SidebarProvider>
      <AppSidebar
        currentProjectId={currentProjectId}
        onProjectChange={setCurrentProjectId}
        onOpenSettings={setSettingsProjectId}
      />
      <SidebarInset>
        <header className="flex h-14 items-center gap-4 border-b px-4">
          <SidebarTrigger />
          <h1 className="text-lg font-semibold">Graph RAG Chat</h1>
        </header>
        <main className="flex-1 overflow-auto p-4">
          <ChatInterface projectId={currentProjectId} />
        </main>
      </SidebarInset>

      <ProjectSettingsDialog
        projectId={settingsProjectId}
        open={settingsProjectId !== null}
        onOpenChange={(open) => !open && setSettingsProjectId(null)}
      />
    </SidebarProvider>
  );
}
