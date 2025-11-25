"use client"

import * as React from "react"
import { useRouter } from "next/navigation"
import { ProjectSwitcher } from "@/components/project-switcher"
import { ThemeToggle } from "@/components/theme-toggle"
import { apiClient } from "@/lib/api"
import { useMutation, useQueryClient } from "@tanstack/react-query"
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
  currentProjectId?: string
  onProjectChange?: (projectId: string) => void
}

export function TopBar({
  currentProjectId,
  onProjectChange,
}: TopBarProps) {
  const router = useRouter()
  const [isCreateDialogOpen, setIsCreateDialogOpen] = React.useState(false)
  const [newProjectName, setNewProjectName] = React.useState("")
  const [newProjectDescription, setNewProjectDescription] = React.useState("")
  const queryClient = useQueryClient()

  const createProjectMutation = useMutation({
    mutationFn: (data: { name: string; description?: string }) =>
      apiClient.createProject(data),
    onSuccess: (newProject) => {
      queryClient.invalidateQueries({ queryKey: ["projects"] })
      setIsCreateDialogOpen(false)
      setNewProjectName("")
      setNewProjectDescription("")
      toast.success("Project created", {
        description: `${newProject.name} has been created successfully.`,
      })
      if (onProjectChange) {
        onProjectChange(newProject.id)
      }
    },
    onError: (error: Error) => {
      toast.error("Error", {
        description: error.message,
      })
    },
  })

  const handleCreateProject = () => {
    if (!newProjectName.trim()) {
      toast.error("Validation error", {
        description: "Project name is required.",
      })
      return
    }

    createProjectMutation.mutate({
      name: newProjectName,
      description: newProjectDescription || undefined,
    })
  }

  return (
    <>
      <header className="border-b">
        <div className="flex h-14 items-center px-4 gap-4">
          <h1 className="text-lg font-semibold">Graph RAG</h1>
          <ProjectSwitcher
            currentProjectId={currentProjectId}
            onProjectChange={onProjectChange}
            onOpenSettings={(projectId) => router.push(`/projects/${projectId}`)}
            onCreateProject={() => setIsCreateDialogOpen(true)}
          />
          <div className="ml-auto">
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
  )
}
