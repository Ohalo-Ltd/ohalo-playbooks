/**
 * Project settings dialog for editing system prompt and metadata.
 */

'use client';

import React from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import {
  Loader2,
  Settings as SettingsIcon,
  Database,
  FileText,
  RefreshCw,
} from "lucide-react";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Textarea } from '@/components/ui/textarea';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { apiClient, Project } from '@/lib/api';
import { toast } from 'sonner';

interface ProjectSettingsDialogProps {
  projectId: string | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

const EXTRACTOR_PREVIEW = `{
  "entities": [
    {
      "id": "e1",
      "type": "Person",
      "properties": {
        "name": "John Doe",
        "role": "Engineer"
      }
    }
  ],
  "relationships": [
    {
      "from": "e1",
      "to": "e2",
      "type": "WORKS_ON",
      "properties": {
        "since": "2023"
      }
    }
  ]
}`;

export function ProjectSettingsDialog({
  projectId,
  open,
  onOpenChange,
}: ProjectSettingsDialogProps) {
  const [name, setName] = React.useState("");
  const [description, setDescription] = React.useState("");
  const [systemPrompt, setSystemPrompt] = React.useState("");

  // DXR Fields
  const [dxrUrl, setDxrUrl] = React.useState("");
  const [dxrApiToken, setDxrApiToken] = React.useState("");
  const [dxrDatasourceId, setDxrDatasourceId] = React.useState("");

  const queryClient = useQueryClient();

  // Fetch project details
  const { data: project, isLoading } = useQuery({
    queryKey: ["project", projectId],
    queryFn: () => apiClient.getProject(projectId!),
    enabled: !!projectId && open,
  });

  // Fetch documents
  const {
    data: documents = [],
    isLoading: isLoadingDocs,
    refetch: refetchDocs,
  } = useQuery({
    queryKey: ["documents", projectId],
    queryFn: () => apiClient.listDocuments(projectId!),
    enabled: !!projectId && open,
  });

  // Update form when project loads
  React.useEffect(() => {
    if (project) {
      setName(project.name);
      setDescription(project.description || "");
      setSystemPrompt(project.system_prompt || "");
      setDxrUrl(project.dxr_url || "");
      setDxrApiToken(project.dxr_api_token || "");
      setDxrDatasourceId(project.dxr_datasource_id || "");
    }
  }, [project]);

  // Update project mutation
  const updateProjectMutation = useMutation({
    mutationFn: (updates: {
      name?: string;
      description?: string;
      system_prompt?: string;
      dxr_url?: string;
      dxr_api_token?: string;
      dxr_datasource_id?: string;
    }) => apiClient.updateProject(projectId!, updates),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["projects"] });
      queryClient.invalidateQueries({ queryKey: ["project", projectId] });
      toast.success("Settings saved", {
        description: "Project settings have been updated successfully.",
      });
      onOpenChange(false);
    },
    onError: (error: Error) => {
      toast.error("Error", {
        description: error.message,
      });
    },
  });

  // Ingest mutation
  const ingestMutation = useMutation({
    mutationFn: () =>
      apiClient.startIngestion({
        project_id: projectId!,
        datasource_id: dxrDatasourceId,
      }),
    onSuccess: () => {
      toast.success("Ingestion started", {
        description: "Documents are being processed.",
      });
      refetchDocs();
    },
    onError: (error: Error) => {
      toast.error("Ingestion failed", {
        description: error.message,
      });
    },
  });

  const handleSave = () => {
    updateProjectMutation.mutate({
      name,
      description,
      system_prompt: systemPrompt,
      dxr_url: dxrUrl,
      dxr_api_token: dxrApiToken,
      dxr_datasource_id: dxrDatasourceId,
    });
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-4xl h-[80vh] flex flex-col">
        <DialogHeader>
          <DialogTitle>Project Settings</DialogTitle>
          <DialogDescription>
            Manage project configuration, DXR connection, and documents.
          </DialogDescription>
        </DialogHeader>

        <Tabs
          defaultValue="general"
          className="flex-1 flex flex-col overflow-hidden"
        >
          <TabsList className="grid w-full grid-cols-3">
            <TabsTrigger value="general">
              <SettingsIcon className="w-4 h-4 mr-2" />
              General
            </TabsTrigger>
            <TabsTrigger value="dxr">
              <Database className="w-4 h-4 mr-2" />
              DXR Connection
            </TabsTrigger>
            <TabsTrigger value="documents">
              <FileText className="w-4 h-4 mr-2" />
              Documents
            </TabsTrigger>
          </TabsList>

          <div className="flex-1 overflow-y-auto p-4">
            <TabsContent value="general" className="space-y-4 mt-0">
              <div className="grid gap-2">
                <Label htmlFor="name">Project Name</Label>
                <Input
                  id="name"
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  placeholder="My Project"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="description">Description</Label>
                <Input
                  id="description"
                  value={description}
                  onChange={(e) => setDescription(e.target.value)}
                  placeholder="Project description"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="systemPrompt">System Prompt</Label>
                <Textarea
                  id="systemPrompt"
                  value={systemPrompt}
                  onChange={(e) => setSystemPrompt(e.target.value)}
                  placeholder="You are a helpful assistant..."
                  className="min-h-[200px] font-mono text-sm"
                />
                <p className="text-xs text-muted-foreground">
                  Customize how the AI agent behaves and answers questions.
                </p>
              </div>
            </TabsContent>

            <TabsContent value="dxr" className="space-y-6 mt-0">
              <div className="grid gap-4 border p-4 rounded-lg">
                <h3 className="font-semibold">Connection Details</h3>
                <div className="grid gap-2">
                  <Label htmlFor="dxrUrl">DXR URL</Label>
                  <Input
                    id="dxrUrl"
                    value={dxrUrl}
                    onChange={(e) => setDxrUrl(e.target.value)}
                    placeholder="https://api.dataxray.com"
                  />
                </div>
                <div className="grid gap-2">
                  <Label htmlFor="dxrApiToken">API Token</Label>
                  <Input
                    id="dxrApiToken"
                    type="password"
                    value={dxrApiToken}
                    onChange={(e) => setDxrApiToken(e.target.value)}
                    placeholder="sk-..."
                  />
                </div>
                <div className="grid gap-2">
                  <Label htmlFor="dxrDatasourceId">Datasource ID</Label>
                  <Input
                    id="dxrDatasourceId"
                    value={dxrDatasourceId}
                    onChange={(e) => setDxrDatasourceId(e.target.value)}
                    placeholder="ds_..."
                  />
                </div>
              </div>

              <div className="grid gap-4">
                <h3 className="font-semibold">Extractor Configuration</h3>
                <p className="text-sm text-muted-foreground">
                  The extractor must output JSON in the following format:
                </p>
                <div className="bg-muted p-4 rounded-lg overflow-x-auto">
                  <pre className="text-xs font-mono">{EXTRACTOR_PREVIEW}</pre>
                </div>
              </div>

              <div className="flex items-center justify-between border-t pt-4">
                <div className="text-sm text-muted-foreground">
                  Ready to ingest documents from DXR?
                </div>
                <Button
                  onClick={() => ingestMutation.mutate()}
                  disabled={ingestMutation.isPending || !dxrDatasourceId}
                >
                  {ingestMutation.isPending && (
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  )}
                  Start Ingestion
                </Button>
              </div>
            </TabsContent>

            <TabsContent
              value="documents"
              className="mt-0 h-full flex flex-col"
            >
              <div className="flex items-center justify-between mb-4">
                <h3 className="font-semibold">Ingested Documents</h3>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => refetchDocs()}
                >
                  <RefreshCw className="h-4 w-4 mr-2" />
                  Refresh
                </Button>
              </div>

              {isLoadingDocs ? (
                <div className="flex items-center justify-center h-40">
                  <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
                </div>
              ) : documents.length === 0 ? (
                <div className="flex flex-col items-center justify-center h-64 border-2 border-dashed rounded-lg text-center p-8">
                  <FileText className="h-12 w-12 text-muted-foreground mb-4" />
                  <h3 className="text-lg font-semibold">No documents yet</h3>
                  <p className="text-sm text-muted-foreground mb-4">
                    Connect to DXR and start ingestion to see documents here.
                  </p>
                  <Button
                    variant="outline"
                    onClick={() => {
                      // Switch to DXR tab - hacky but works for now
                      const dxrTab = document.querySelector(
                        '[value="dxr"]'
                      ) as HTMLElement;
                      dxrTab?.click();
                    }}
                  >
                    Go to DXR Connection
                  </Button>
                </div>
              ) : (
                <div className="border rounded-md">
                  <table className="w-full text-sm">
                    <thead className="bg-muted/50">
                      <tr className="border-b">
                        <th className="h-10 px-4 text-left font-medium">
                          Name
                        </th>
                        <th className="h-10 px-4 text-left font-medium">
                          Size
                        </th>
                        <th className="h-10 px-4 text-left font-medium">
                          Type
                        </th>
                        <th className="h-10 px-4 text-left font-medium">
                          Path
                        </th>
                      </tr>
                    </thead>
                    <tbody>
                      {documents.map((doc: any) => (
                        <tr
                          key={doc.id}
                          className="border-b last:border-0 hover:bg-muted/50"
                        >
                          <td className="p-4 font-medium">{doc.name}</td>
                          <td className="p-4">
                            {doc.properties.size
                              ? `${(doc.properties.size / 1024).toFixed(1)} KB`
                              : "-"}
                          </td>
                          <td className="p-4">
                            {doc.properties.mime_type || "-"}
                          </td>
                          <td
                            className="p-4 text-muted-foreground truncate max-w-[200px]"
                            title={doc.properties.path}
                          >
                            {doc.properties.path || "-"}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </TabsContent>
          </div>
        </Tabs>

        <DialogFooter className="border-t p-4 mt-auto">
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button
            onClick={handleSave}
            disabled={updateProjectMutation.isPending}
          >
            {updateProjectMutation.isPending && (
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
            )}
            Save Changes
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}


