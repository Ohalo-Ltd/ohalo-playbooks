/**
 * Project settings page with tabbed interface.
 */

"use client";

import * as React from "react";
import { useRouter } from "next/navigation";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  ArrowLeft,
  FileText,
  Settings,
  Link as LinkIcon,
  Loader2,
  Upload,
  Trash2,
  Shield,
  Network,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { apiClient } from "@/lib/api";
import { toast } from "sonner";
import { EntitlementsManagement } from "@/components/entitlements-management";
import { Switch } from "@/components/ui/switch";

interface PageProps {
  params: Promise<{
    id: string;
  }>;
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

export default function ProjectSettingsPage({ params }: PageProps) {
  const router = useRouter();
  const queryClient = useQueryClient();
  const { id: projectId } = React.use(params);

  const [name, setName] = React.useState("");
  const [description, setDescription] = React.useState("");
  const [systemPrompt, setSystemPrompt] = React.useState("");
  const [entitlementsEnabled, setEntitlementsEnabled] = React.useState(false);
  const [dxrUrl, setDxrUrl] = React.useState("");
  const [dxrApiToken, setDxrApiToken] = React.useState("");
  const [dxrDatasourceId, setDxrDatasourceId] = React.useState("");
  const [dxrExtractorId, setDxrExtractorId] = React.useState("");

  // Fetch project details
  const { data: project, isLoading } = useQuery({
    queryKey: ["project", projectId],
    queryFn: () => apiClient.getProject(projectId),
  });

  // Fetch documents
  const {
    data: documents = [],
    isLoading: isLoadingDocs,
    refetch: refetchDocs,
  } = useQuery({
    queryKey: ["documents", projectId],
    queryFn: () => apiClient.listDocuments(projectId),
  });

  // Update form when project loads
  React.useEffect(() => {
    if (project) {
      setName(project.name);
      setDescription(project.description || "");
      setSystemPrompt(project.system_prompt || "");
      setEntitlementsEnabled(project.entitlements_enabled || false);
      setDxrUrl(project.dxr_url || "");
      setDxrApiToken(project.dxr_api_token || "");
      setDxrDatasourceId(project.dxr_datasource_id || "");
      setDxrExtractorId(project.dxr_extractor_id || "");
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
      dxr_extractor_id?: string;
      entitlements_enabled?: boolean;
    }) => apiClient.updateProject(projectId, updates),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["projects"] });
      queryClient.invalidateQueries({ queryKey: ["project", projectId] });
      toast.success("Settings saved", {
        description: "Project settings have been updated successfully.",
      });
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
        project_id: projectId,
        datasource_id: dxrDatasourceId,
        extractor_id: dxrExtractorId || undefined,
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

  const handleSaveConnection = () => {
    updateProjectMutation.mutate({
      dxr_url: dxrUrl,
      dxr_api_token: dxrApiToken,
      dxr_datasource_id: dxrDatasourceId,
    });
  };

  const handleSaveGraph = () => {
    updateProjectMutation.mutate({
      dxr_extractor_id: dxrExtractorId,
    });
  };

  const handleSaveCustomize = () => {
    updateProjectMutation.mutate({
      name,
      description,
      system_prompt: systemPrompt,
      entitlements_enabled: entitlementsEnabled,
    });
  };

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-screen">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (!project) {
    return (
      <div className="flex flex-col items-center justify-center h-screen space-y-4">
        <h1 className="text-2xl font-semibold">Project not found</h1>
        <Button onClick={() => router.push("/")}>Go back</Button>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-background">
      {/* Header */}
      <header className="border-b">
        <div className="container mx-auto px-4 py-4">
          <div className="flex items-center gap-4">
            <Button
              variant="ghost"
              size="icon"
              onClick={() => router.push("/")}
            >
              <ArrowLeft className="h-5 w-5" />
            </Button>
            <div>
              <h1 className="text-2xl font-semibold">{project.name}</h1>
              <p className="text-sm text-muted-foreground">Project Settings</p>
            </div>
          </div>
        </div>
      </header>

      {/* Content */}
      <main className="container mx-auto px-4 py-8">
        <Tabs defaultValue="documents" className="w-full">
          <TabsList className="grid w-full grid-cols-5 max-w-3xl">
            <TabsTrigger value="connection">
              <LinkIcon className="h-4 mr-2" />
              Connect to DXR
            </TabsTrigger>
            <TabsTrigger value="documents">
              <FileText className="h-4 mr-2" />
              Documents
            </TabsTrigger>
            <TabsTrigger value="graph">
              <Network className="h-4 mr-2" />
              Graph
            </TabsTrigger>
            <TabsTrigger value="customize">
              <Settings className="h-4 mr-2" />
              Customize
            </TabsTrigger>
            <TabsTrigger value="entitlements">
              <Shield className="h-4 mr-2" />
              Entitlements
            </TabsTrigger>
          </TabsList>

          {/* Documents Tab */}
          <TabsContent value="documents" className="space-y-6 mt-6">
            <div className="flex items-center justify-between">
              <div>
                <h2 className="text-xl font-semibold">Documents</h2>
                <p className="text-sm text-muted-foreground mt-1">
                  View and manage ingested documents from your data source
                </p>
              </div>
              <Button
                onClick={() => ingestMutation.mutate()}
                disabled={ingestMutation.isPending || !dxrDatasourceId}
                size="lg"
              >
                {ingestMutation.isPending && (
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                )}
                <Upload className="mr-2 h-4 w-4" />
                Start Ingestion
              </Button>
            </div>

            {isLoadingDocs ? (
              <div className="flex items-center justify-center h-64 border rounded-lg">
                <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
              </div>
            ) : documents.length === 0 ? (
              <div className="flex flex-col items-center justify-center h-64 border-2 border-dashed rounded-lg text-center p-8">
                <FileText className="h-12 w-12 text-muted-foreground mb-4" />
                <h3 className="text-lg font-semibold">No documents yet</h3>
                <p className="text-sm text-muted-foreground mt-2 max-w-md">
                  Connect to a data source and start ingestion to see your
                  documents here.
                </p>
                <Button
                  variant="outline"
                  className="mt-4"
                  onClick={() => {
                    const connectionTab = document.querySelector(
                      '[value="connection"]'
                    ) as HTMLElement;
                    connectionTab?.click();
                  }}
                >
                  Go to Connection
                </Button>
              </div>
            ) : (
              <div className="border rounded-lg overflow-hidden">
                <table className="w-full">
                  <thead className="bg-muted/50 border-b">
                    <tr>
                      <th className="text-left p-4 font-medium">Name</th>
                      <th className="text-left p-4 font-medium">Size</th>
                      <th className="text-left p-4 font-medium">Type</th>
                      <th className="text-left p-4 font-medium">Path</th>
                      <th className="text-right p-4 font-medium">Actions</th>
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
                        <td className="p-4 text-muted-foreground">
                          {doc.properties.mime_type || "-"}
                        </td>
                        <td
                          className="p-4 text-muted-foreground truncate max-w-[300px]"
                          title={doc.properties.path}
                        >
                          {doc.properties.path || "-"}
                        </td>
                        <td className="p-4 text-right">
                          <Button variant="ghost" size="icon">
                            <Trash2 className="h-4 w-4 text-destructive" />
                          </Button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </TabsContent>

          {/* Connection Tab */}
          <TabsContent value="connection" className="space-y-6 mt-6">
            <div>
              <h2 className="text-xl font-semibold">Data Source Connection</h2>
              <p className="text-sm text-muted-foreground mt-1">
                Configure your DXR data source and ingest documents
              </p>
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div className="border rounded-lg p-6 space-y-4">
                <h3 className="font-semibold">Connection Details</h3>
                <div className="grid gap-4">
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
                <div className="flex justify-end">
                  <Button
                    onClick={handleSaveConnection}
                    disabled={updateProjectMutation.isPending}
                  >
                    {updateProjectMutation.isPending && (
                      <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                    )}
                    Save Connection
                  </Button>
                </div>
              </div>
            </div>
          </TabsContent>

          {/* Graph Tab */}
          <TabsContent value="graph" className="space-y-6 mt-6">
            <div>
              <h2 className="text-xl font-semibold">Graph Configuration</h2>
              <p className="text-sm text-muted-foreground mt-1">
                Configure how entities and relationships are extracted
              </p>
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div className="border rounded-lg p-6 space-y-4">
                <h3 className="font-semibold">Extractor Settings</h3>
                <div className="grid gap-4">
                  <div className="grid gap-2">
                    <Label htmlFor="dxrExtractorId">Extractor ID</Label>
                    <Input
                      id="dxrExtractorId"
                      value={dxrExtractorId}
                      onChange={(e) => setDxrExtractorId(e.target.value)}
                      placeholder="extracted_metadata#123"
                    />
                    <p className="text-xs text-muted-foreground">
                      The extractor output field to use (e.g.,
                      extracted_metadata#123)
                    </p>
                  </div>
                </div>
                <div className="flex justify-end">
                  <Button
                    onClick={handleSaveGraph}
                    disabled={updateProjectMutation.isPending}
                  >
                    {updateProjectMutation.isPending && (
                      <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                    )}
                    Save Graph Settings
                  </Button>
                </div>
              </div>
              <div className="border rounded-lg p-6 space-y-4">
                <h3 className="font-semibold">Extractor Configuration</h3>
                <p className="text-sm text-muted-foreground">
                  The extractor must output JSON in the following format:
                </p>
                <div className="bg-muted p-4 rounded-lg overflow-x-auto">
                  <pre className="text-xs font-mono">{EXTRACTOR_PREVIEW}</pre>
                </div>
              </div>
            </div>
          </TabsContent>

          {/* Customize Tab */}
          <TabsContent value="customize" className="space-y-6 mt-6">
            <div>
              <h2 className="text-xl font-semibold">Customize Project</h2>
              <p className="text-sm text-muted-foreground mt-1">
                Update project details and AI behavior
              </p>
            </div>

            <div className="border rounded-lg p-6 space-y-4">
              <h3 className="font-semibold">Project Details</h3>
              <div className="grid gap-4">
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
              </div>
            </div>

            <div className="border rounded-lg p-6 space-y-4">
              <div className="flex items-center justify-between">
                <div>
                  <h3 className="font-semibold">Entitlements</h3>
                  <p className="text-sm text-muted-foreground">
                    Enable document-level access control based on user
                    permissions
                  </p>
                </div>
                <Switch
                  checked={entitlementsEnabled}
                  onCheckedChange={setEntitlementsEnabled}
                />
              </div>
              {entitlementsEnabled && (
                <p className="text-xs text-muted-foreground bg-blue-50 dark:bg-blue-950 p-3 rounded">
                  ✓ Entitlements enabled. Users will only see documents they
                  have access to. Go to the Entitlements tab to manage users and
                  groups.
                </p>
              )}
            </div>

            <div className="border rounded-lg p-6 space-y-4">
              <h3 className="font-semibold">AI System Prompt</h3>
              <p className="text-sm text-muted-foreground">
                Customize how the AI agent behaves and answers questions
              </p>
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
                  Leave empty to use the default system prompt. This allows you
                  to tailor the chain-of-thought, tool usage, and response style
                  for your specific use case.
                </p>
              </div>
            </div>

            <div className="border rounded-lg p-6 space-y-4">
              <h3 className="font-semibold">Project Information</h3>
              <div className="text-sm space-y-2 text-muted-foreground">
                <div>
                  <span className="font-medium text-foreground">ID:</span>{" "}
                  {project.id}
                </div>
                <div>
                  <span className="font-medium text-foreground">Created:</span>{" "}
                  {new Date(project.created_at).toLocaleString()}
                </div>
                <div>
                  <span className="font-medium text-foreground">Updated:</span>{" "}
                  {new Date(project.updated_at).toLocaleString()}
                </div>
              </div>
            </div>

            <div className="flex justify-end">
              <Button
                onClick={handleSaveCustomize}
                disabled={updateProjectMutation.isPending}
              >
                {updateProjectMutation.isPending && (
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                )}
                Save Changes
              </Button>
            </div>
          </TabsContent>

          {/* Entitlements Tab */}
          <TabsContent value="entitlements" className="space-y-6 mt-6">
            <div>
              <h2 className="text-xl font-semibold">Entitlements</h2>
              <p className="text-sm text-muted-foreground mt-1">
                Manage users and groups for document-level access control
              </p>
            </div>

            {!entitlementsEnabled ? (
              <div className="border-2 border-dashed rounded-lg p-12 text-center">
                <Shield className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
                <h3 className="text-lg font-semibold mb-2">
                  Entitlements Not Enabled
                </h3>
                <p className="text-sm text-muted-foreground max-w-md mx-auto mb-4">
                  Enable entitlements in the Customize tab to manage
                  document-level access control based on user permissions.
                </p>
                <Button
                  variant="outline"
                  onClick={() => {
                    const customizeTab = document.querySelector(
                      '[value="customize"]'
                    ) as HTMLElement;
                    customizeTab?.click();
                  }}
                >
                  Go to Customize
                </Button>
              </div>
            ) : (
              <EntitlementsManagement projectId={projectId} />
            )}
          </TabsContent>
        </Tabs>
      </main>
    </div>
  );
}
