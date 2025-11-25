/**
 * Main page - chat interface with top bar.
 */

"use client";

import React from "react";
import { ChatInterface } from "@/components/chat-interface";
import { TopBar } from "@/components/top-bar";
import { User } from "@/components/user-switcher";
import { useQuery } from "@tanstack/react-query";
import { apiClient } from "@/lib/api";

export default function Home() {
  const [currentProjectId, setCurrentProjectId] = React.useState<
    string | undefined
  >();
  const [currentUser, setCurrentUser] = React.useState<User | null>(null);

  const { data: projects = [] } = useQuery({
    queryKey: ["projects"],
    queryFn: () => apiClient.listProjects(),
  });

  // Auto-select first project if none selected
  React.useEffect(() => {
    if (!currentProjectId && projects.length > 0) {
      setCurrentProjectId(projects[0].id);
    }
  }, [projects, currentProjectId]);

  return (
    <div className="flex flex-col h-screen">
      <TopBar
        currentProjectId={currentProjectId}
        onProjectChange={setCurrentProjectId}
        currentUser={currentUser}
        onUserChange={setCurrentUser}
      />
      <main className="flex-1 overflow-hidden">
        <ChatInterface
          projectId={currentProjectId}
          hasProjects={projects.length > 0}
          currentUser={currentUser}
        />
      </main>
    </div>
  );
}
