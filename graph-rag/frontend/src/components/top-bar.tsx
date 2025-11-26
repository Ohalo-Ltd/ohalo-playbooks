"use client"

import * as React from "react"
import { useRouter } from "next/navigation"
import { ProjectSwitcher } from "@/components/project-switcher"
import { ThemeToggle } from "@/components/theme-toggle"
import { UserSwitcher, User } from "@/components/user-switcher";
import { apiClient } from "@/lib/api";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Button } from "@/components/ui/button"

interface TopBarProps {
  currentProjectId?: string;
  onProjectChange?: (projectId: string) => void;
  currentUser?: User | null;
  onUserChange?: (user: User | null) => void;
}

export function TopBar({
  currentProjectId,
  onProjectChange,
  currentUser,
  onUserChange,
}: TopBarProps) {
  const router = useRouter();
  const [isCreateDialogOpen, setIsCreateDialogOpen] = React.useState(false);
  const [newProjectName, setNewProjectName] = React.useState("");
  const [newProjectDescription, setNewProjectDescription] = React.useState("");
  const queryClient = useQueryClient();

  // Fetch project details to check if entitlements are enabled
  const { data: project } = useQuery({
    queryKey: ["project", currentProjectId],
    queryFn: () => apiClient.getProject(currentProjectId!),
    enabled: !!currentProjectId,
  });

  // Fetch users for switcher when entitlements are enabled
  const { data: users = [] } = useQuery({
    queryKey: ["project-users", currentProjectId],
    queryFn: () => apiClient.listProjectUsers(currentProjectId!),
    enabled: !!currentProjectId && !!project?.entitlements_enabled,
  });

  // Auto-select first user when users are loaded
  React.useEffect(() => {
    if (users.length > 0 && !currentUser && onUserChange) {
      onUserChange({ email: users[0].email, name: users[0].name });
    }
  }, [users, currentUser, onUserChange]);

  const createProjectMutation = useMutation({
    mutationFn: (data: { name: string; description?: string }) =>
      apiClient.createProject(data),
    onSuccess: (newProject) => {
      queryClient.invalidateQueries({ queryKey: ["projects"] });
      setIsCreateDialogOpen(false);
      setNewProjectName("");
      setNewProjectDescription("");
      toast.success("Project created", {
        description: `${newProject.name} has been created successfully.`,
      });
      if (onProjectChange) {
        onProjectChange(newProject.id);
      }
    },
    onError: (error: Error) => {
      toast.error("Error", {
        description: error.message,
      });
    },
  });

  const handleCreateProject = () => {
    if (!newProjectName.trim()) {
      toast.error("Validation error", {
        description: "Project name is required.",
      });
      return;
    }

    createProjectMutation.mutate({
      name: newProjectName,
      description: newProjectDescription || undefined,
    });
  };

  return (
    <>
      <header className="border-b">
        <div className="flex h-14 items-center px-4 gap-4">
          <h1 className="text-lg font-semibold">Data X-Ray</h1>
          <ProjectSwitcher
            currentProjectId={currentProjectId}
            onProjectChange={onProjectChange}
            onOpenSettings={(projectId) =>
              router.push(`/projects/${projectId}`)
            }
            onCreateProject={() => setIsCreateDialogOpen(true)}
          />
          <div className="ml-auto flex items-center gap-3">
            {project?.entitlements_enabled &&
              users.length > 0 &&
              onUserChange && (
                <div className="flex items-center gap-2">
                  <span className="text-xs text-muted-foreground">Ask as:</span>
                  <UserSwitcher
                    users={users.map((u) => ({ email: u.email, name: u.name }))}
                    currentUser={currentUser || null}
                    onUserChange={onUserChange}
                    enabled={true}
                  />
                </div>
              )}
            <ThemeToggle />
          </div>
        </div>
      </header>

      <Dialog open={isCreateDialogOpen} onOpenChange={setIsCreateDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Create New Project</DialogTitle>
            <DialogDescription>
              Create a new project to organize your knowledge base.
            </DialogDescription>
          </DialogHeader>
          <div className="grid gap-4 py-4">
            <div className="grid gap-2">
              <Label htmlFor="name">Name</Label>
              <Input
                id="name"
                value={newProjectName}
                onChange={(e) => setNewProjectName(e.target.value)}
                placeholder="My Project"
              />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="description">Description (optional)</Label>
              <Input
                id="description"
                value={newProjectDescription}
                onChange={(e) => setNewProjectDescription(e.target.value)}
                placeholder="A brief description"
              />
            </div>
          </div>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => setIsCreateDialogOpen(false)}
            >
              Cancel
            </Button>
            <Button
              onClick={handleCreateProject}
              disabled={createProjectMutation.isPending}
            >
              {createProjectMutation.isPending ? "Creating..." : "Create"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}
