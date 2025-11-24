/**
 * Project settings dialog for editing system prompt and metadata.
 */

'use client';

import React from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Loader2 } from 'lucide-react';
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

const DEFAULT_SYSTEM_PROMPT = `You are an intelligent assistant that answers questions using a knowledge graph.

You have access to multiple tools to explore the knowledge base:
1. **discover_schema**: Understand the structure of the knowledge graph (entities, relationships, patterns)
2. **vector_search**: Find relevant document chunks using semantic similarity
3. **entity_lookup**: Find specific entities by name
4. **graph_neighbors**: Explore relationships between entities
5. **graph_query**: Execute Cypher queries for complex graph traversal

**Recommended Strategy:**
1. **First interaction**: Call discover_schema to understand what entities and relationships exist
2. **For questions**: Start with vector_search to find relevant context
3. **For entity questions**: Use entity_lookup, then graph_neighbors to expand context
4. **For complex queries**: Use graph_query for multi-hop reasoning

**Hybrid Search Approach:**
- Use vector_search to get initial relevant chunks
- Extract entity names from chunks or question
- Use entity_lookup to find those entities in the graph
- Use graph_neighbors to expand context around entities
- Combine all information for a comprehensive answer

Always:
- Cite your sources by mentioning documents and entities
- Explain relationships you discovered in the graph
- If you find related entities, mention them to provide context
- Be clear about what information comes from direct search vs graph traversal`;

export function ProjectSettingsDialog({
  projectId,
  open,
  onOpenChange,
}: ProjectSettingsDialogProps) {
  const [name, setName] = React.useState('');
  const [description, setDescription] = React.useState('');
  const [systemPrompt, setSystemPrompt] = React.useState('');
  const queryClient = useQueryClient();

  // Fetch project details
  const { data: project, isLoading } = useQuery({
    queryKey: ['project', projectId],
    queryFn: () => apiClient.getProject(projectId!),
    enabled: !!projectId && open,
  });

  // Update form when project loads
  React.useEffect(() => {
    if (project) {
      setName(project.name);
      setDescription(project.description || '');
      setSystemPrompt(project.system_prompt || '');
    }
  }, [project]);

  // Update project mutation
  const updateProjectMutation = useMutation({
    mutationFn: (updates: {
      name?: string;
      description?: string;
      system_prompt?: string;
    }) => apiClient.updateProject(projectId!, updates),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['projects'] });
      queryClient.invalidateQueries({ queryKey: ['project', projectId] });
      toast.success('Settings saved', {
        description: 'Project settings have been updated successfully.',
      });
      onOpenChange(false);
    },
    onError: (error: Error) => {
      toast.error('Error', {
        description: error.message,
      });
    },
  });

  const handleSave = () => {
    const updates: any = {};

    if (name !== project?.name) {
      updates.name = name;
    }
    if (description !== (project?.description || '')) {
      updates.description = description;
    }
    if (systemPrompt !== (project?.system_prompt || '')) {
      updates.system_prompt = systemPrompt || null;
    }

    if (Object.keys(updates).length > 0) {
      updateProjectMutation.mutate(updates);
    } else {
      onOpenChange(false);
    }
  };

  const handleResetPrompt = () => {
    setSystemPrompt('');
    toast.info('Prompt reset', {
      description: 'System prompt will use the default when saved.',
    });
  };

  const handleLoadDefault = () => {
    setSystemPrompt(DEFAULT_SYSTEM_PROMPT);
    toast.info('Default loaded', {
      description: 'Default system prompt has been loaded.',
    });
  };

  if (!projectId) return null;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-3xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>Project Settings</DialogTitle>
          <DialogDescription>
            Manage project details and customize the AI system prompt.
          </DialogDescription>
        </DialogHeader>

        {isLoading ? (
          <div className="flex items-center justify-center py-8">
            <Loader2 className="h-6 w-6 animate-spin" />
          </div>
        ) : (
          <Tabs defaultValue="general" className="w-full">
            <TabsList className="grid w-full grid-cols-2">
              <TabsTrigger value="general">General</TabsTrigger>
              <TabsTrigger value="prompt">System Prompt</TabsTrigger>
            </TabsList>

            <TabsContent value="general" className="space-y-4">
              <div className="grid gap-2">
                <Label htmlFor="project-name">Project Name</Label>
                <Input
                  id="project-name"
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  placeholder="My Project"
                />
              </div>

              <div className="grid gap-2">
                <Label htmlFor="project-description">Description</Label>
                <Textarea
                  id="project-description"
                  value={description}
                  onChange={(e) => setDescription(e.target.value)}
                  placeholder="A brief description of this project"
                  rows={3}
                />
              </div>

              <div className="rounded-lg border p-4 space-y-2">
                <h4 className="text-sm font-medium">Project Info</h4>
                <div className="text-xs text-muted-foreground space-y-1">
                  <div>
                    <span className="font-medium">ID:</span> {project?.id}
                  </div>
                  <div>
                    <span className="font-medium">Created:</span>{' '}
                    {new Date(project?.created_at || '').toLocaleString()}
                  </div>
                  <div>
                    <span className="font-medium">Updated:</span>{' '}
                    {new Date(project?.updated_at || '').toLocaleString()}
                  </div>
                </div>
              </div>
            </TabsContent>

            <TabsContent value="prompt" className="space-y-4">
              <div className="grid gap-2">
                <div className="flex items-center justify-between">
                  <Label htmlFor="system-prompt">Custom System Prompt</Label>
                  <div className="flex gap-2">
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={handleLoadDefault}
                      type="button"
                    >
                      Load Default
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={handleResetPrompt}
                      type="button"
                    >
                      Clear
                    </Button>
                  </div>
                </div>
                <Textarea
                  id="system-prompt"
                  value={systemPrompt}
                  onChange={(e) => setSystemPrompt(e.target.value)}
                  placeholder="Leave empty to use the default system prompt..."
                  rows={15}
                  className="font-mono text-sm"
                />
                <p className="text-xs text-muted-foreground">
                  Customize the AI agent's behavior by defining your own system prompt.
                  This allows you to tailor the chain-of-thought, tool usage, and response
                  style for your specific use case. Leave empty to use the default prompt.
                </p>
              </div>

              <div className="rounded-lg border p-4 space-y-2">
                <h4 className="text-sm font-medium">Available Tools</h4>
                <ul className="text-xs text-muted-foreground space-y-1 list-disc list-inside">
                  <li>
                    <code className="bg-muted px-1 py-0.5 rounded">discover_schema</code>
                    - Introspect graph structure
                  </li>
                  <li>
                    <code className="bg-muted px-1 py-0.5 rounded">vector_search</code>
                    - Semantic similarity search
                  </li>
                  <li>
                    <code className="bg-muted px-1 py-0.5 rounded">entity_lookup</code>
                    - Find entities by name
                  </li>
                  <li>
                    <code className="bg-muted px-1 py-0.5 rounded">graph_neighbors</code>
                    - Explore relationships
                  </li>
                  <li>
                    <code className="bg-muted px-1 py-0.5 rounded">graph_query</code>
                    - Execute Cypher queries
                  </li>
                </ul>
              </div>
            </TabsContent>
          </Tabs>
        )}

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button
            onClick={handleSave}
            disabled={updateProjectMutation.isPending || isLoading}
          >
            {updateProjectMutation.isPending ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Saving...
              </>
            ) : (
              'Save Changes'
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
