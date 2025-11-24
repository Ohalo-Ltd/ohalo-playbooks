/**
 * Application sidebar with project management and settings.
 */

'use client';

import React from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  Settings,
  FolderOpen,
  Plus,
  MessageSquare,
  Database,
  Code,
  Trash2,
} from 'lucide-react';
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarMenuAction,
  SidebarSeparator,
} from '@/components/ui/sidebar';
import { apiClient, Project } from '@/lib/api';
import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { toast } from 'sonner';

interface AppSidebarProps {
  currentProjectId?: string;
  onProjectChange?: (projectId: string) => void;
  onOpenSettings?: (projectId: string) => void;
}

export function AppSidebar({
  currentProjectId,
  onProjectChange,
  onOpenSettings,
}: AppSidebarProps) {
  const [isCreateDialogOpen, setIsCreateDialogOpen] = React.useState(false);
  const [newProjectName, setNewProjectName] = React.useState('');
  const [newProjectDescription, setNewProjectDescription] = React.useState('');
  const queryClient = useQueryClient();

  // Fetch projects
  const { data: projects = [], isLoading } = useQuery({
    queryKey: ['projects'],
    queryFn: () => apiClient.listProjects(),
  });

  // Create project mutation
  const createProjectMutation = useMutation({
    mutationFn: (data: { name: string; description?: string }) =>
      apiClient.createProject(data),
    onSuccess: (newProject) => {
      queryClient.invalidateQueries({ queryKey: ['projects'] });
      setIsCreateDialogOpen(false);
      setNewProjectName('');
      setNewProjectDescription('');
      toast.success('Project created', {
        description: `${newProject.name} has been created successfully.`,
      });
      if (onProjectChange) {
        onProjectChange(newProject.id);
      }
    },
    onError: (error: Error) => {
      toast.error('Error', {
        description: error.message,
      });
    },
  });

  // Delete project mutation
  const deleteProjectMutation = useMutation({
    mutationFn: (projectId: string) => apiClient.deleteProject(projectId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['projects'] });
      toast.success('Project deleted', {
        description: 'The project has been deleted successfully.',
      });
    },
    onError: (error: Error) => {
      toast.error('Error', {
        description: error.message,
      });
    },
  });

  const handleCreateProject = () => {
    if (!newProjectName.trim()) {
      toast.error('Validation error', {
        description: 'Project name is required.',
      });
      return;
    }

    createProjectMutation.mutate({
      name: newProjectName,
      description: newProjectDescription || undefined,
    });
  };

  const handleDeleteProject = (projectId: string, projectName: string) => {
    if (confirm(`Are you sure you want to delete "${projectName}"?`)) {
      deleteProjectMutation.mutate(projectId);
    }
  };

  return (
    <Sidebar>
      <SidebarHeader className="border-b p-4">
        <h2 className="text-lg font-semibold">Graph RAG</h2>
      </SidebarHeader>

      <SidebarContent>
        {/* Navigation */}
        <SidebarGroup>
          <SidebarGroupLabel>Navigation</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              <SidebarMenuItem>
                <SidebarMenuButton asChild isActive>
                  <a href="/">
                    <MessageSquare className="h-4 w-4" />
                    <span>Chat</span>
                  </a>
                </SidebarMenuButton>
              </SidebarMenuItem>
              <SidebarMenuItem>
                <SidebarMenuButton asChild>
                  <a href="/ingestion">
                    <Database className="h-4 w-4" />
                    <span>Ingestion</span>
                  </a>
                </SidebarMenuButton>
              </SidebarMenuItem>
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>

        <SidebarSeparator />

        {/* Projects */}
        <SidebarGroup>
          <SidebarGroupLabel>
            Projects
            <Dialog open={isCreateDialogOpen} onOpenChange={setIsCreateDialogOpen}>
              <DialogTrigger asChild>
                <Button
                  variant="ghost"
                  size="icon"
                  className="ml-auto h-6 w-6"
                >
                  <Plus className="h-4 w-4" />
                </Button>
              </DialogTrigger>
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
                    {createProjectMutation.isPending ? 'Creating...' : 'Create'}
                  </Button>
                </DialogFooter>
              </DialogContent>
            </Dialog>
          </SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              {isLoading ? (
                <SidebarMenuItem>
                  <div className="px-2 py-1.5 text-sm text-muted-foreground">
                    Loading projects...
                  </div>
                </SidebarMenuItem>
              ) : projects.length === 0 ? (
                <SidebarMenuItem>
                  <div className="px-2 py-1.5 text-sm text-muted-foreground">
                    No projects yet
                  </div>
                </SidebarMenuItem>
              ) : (
                projects.map((project) => (
                  <SidebarMenuItem key={project.id}>
                    <SidebarMenuButton
                      asChild
                      isActive={currentProjectId === project.id}
                      onClick={() => onProjectChange?.(project.id)}
                    >
                      <div className="flex items-center">
                        <FolderOpen className="h-4 w-4" />
                        <span className="truncate">{project.name}</span>
                      </div>
                    </SidebarMenuButton>
                    <SidebarMenuAction
                      onClick={() => onOpenSettings?.(project.id)}
                      title="Settings"
                    >
                      <Settings className="h-4 w-4" />
                    </SidebarMenuAction>
                    <SidebarMenuAction
                      onClick={() => handleDeleteProject(project.id, project.name)}
                      title="Delete"
                    >
                      <Trash2 className="h-4 w-4 text-destructive" />
                    </SidebarMenuAction>
                  </SidebarMenuItem>
                ))
              )}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>

      <SidebarFooter className="border-t p-4">
        <div className="text-xs text-muted-foreground">
          Graph RAG v0.1.0
        </div>
      </SidebarFooter>
    </Sidebar>
  );
}
