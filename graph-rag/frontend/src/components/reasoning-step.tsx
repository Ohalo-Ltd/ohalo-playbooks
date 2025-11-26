import { ChevronDown, Database, FileSearch, Network, Search, Sparkles } from 'lucide-react';
import { useState } from 'react';

import { AgentStep } from '@/hooks/use-agent-stream';
import { cn } from '@/lib/utils';

import { Collapsible, CollapsibleContent, CollapsibleTrigger } from './ui/collapsible';
import { GraphPreview, GraphNode, GraphLink } from './graph-preview';

interface ReasoningStepProps {
  step: AgentStep;
  isLast?: boolean;
}

const TOOL_CONFIG: Record<
  string,
  {
    icon: React.ComponentType<{ className?: string }>;
    label: string;
    color: string;
  }
> = {
  discover_graph: {
    icon: Database,
    label: "Checking graph structure",
    color: "text-blue-600",
  },
  vector_search: {
    icon: Search,
    label: "Searching documents",
    color: "text-purple-600",
  },
  entity_lookup: {
    icon: FileSearch,
    label: "Looking up entities",
    color: "text-green-600",
  },
  graph_neighbors: {
    icon: Network,
    label: "Exploring relationships",
    color: "text-orange-600",
  },
  graph_query: {
    icon: Database,
    label: "Running graph query",
    color: "text-red-600",
  },
};

export function ReasoningStep({ step, isLast }: ReasoningStepProps) {
  const [isExpanded, setIsExpanded] = useState(false);

  // Thinking step
  if (step.type === 'thinking') {
    return (
      <div className="flex items-start gap-3 py-2">
        <Sparkles className="h-4 w-4 mt-1 text-muted-foreground animate-pulse" />
        <p className="text-sm text-muted-foreground italic">{step.content}</p>
      </div>
    );
  }

  // Tool call start
  if (step.type === 'tool_call_start' && step.tool) {
    const config = TOOL_CONFIG[step.tool];
    const Icon = config?.icon || Database;

    return (
      <div className="flex items-start gap-3 py-2">
        <Icon className={cn('h-4 w-4 mt-1', config?.color || 'text-muted-foreground')} />
        <div className="flex-1">
          <p className="text-sm font-medium">{config?.label || step.tool}</p>
          {step.args && Object.keys(step.args).length > 0 && (
            <pre className="text-xs text-muted-foreground mt-1 font-mono">
              {JSON.stringify(step.args, null, 2)}
            </pre>
          )}
        </div>
        <div className="h-4 w-4 border-2 border-muted-foreground/30 border-t-transparent rounded-full animate-spin" />
      </div>
    );
  }

  // Final answer
  if (step.type === 'answer') {
    return (
      <div className="border-t pt-4 mt-4">
        <div className="prose prose-sm max-w-none dark:prose-invert">
          {step.content?.split('\n').map((line, i) => (
            <p key={i}>{line}</p>
          ))}
        </div>
      </div>
    );
  }

  // Error
  if (step.type === 'error') {
    return (
      <div className="bg-destructive/10 border border-destructive/20 rounded-md p-3">
        <p className="text-sm text-destructive font-medium">Error</p>
        <p className="text-sm text-destructive/80 mt-1">{step.message}</p>
      </div>
    );
  }

  return null;
}

function renderToolResult(tool: string, result: any) {
  if (!result) {
    return <p className="text-sm text-muted-foreground">No data</p>;
  }

  // Handle errors
  if (result.error) {
    return (
      <div className="text-sm text-destructive">
        <p className="font-medium">Error:</p>
        <p className="mt-1">{result.error}</p>
      </div>
    );
  }

  switch (tool) {
    case "discover_graph":
      return (
        <div className="space-y-3">
          {/* Node type badges */}
          {result.node_stats && result.node_stats.length > 0 && (
            <div>
              <p className="text-sm font-medium">Available Data</p>
              <div className="flex flex-wrap gap-2 mt-2">
                {result.node_stats.map((stat: any, i: number) => (
                  <span
                    key={i}
                    className="inline-flex items-center gap-1 px-2 py-1 rounded-md bg-muted text-foreground text-xs font-medium"
                  >
                    <span className="font-semibold">{stat.label}</span>
                    <span className="text-muted-foreground">
                      ({stat.count})
                    </span>
                  </span>
                ))}
              </div>
            </div>
          )}

          {/* Entity type badges */}
          {result.entity_types && result.entity_types.length > 0 && (
            <div>
              <p className="text-sm font-medium">
                Entity Types ({result.entity_types.length})
              </p>
              <div className="flex flex-wrap gap-1 mt-1">
                {result.entity_types.map((type: string, i: number) => (
                  <span
                    key={i}
                    className="inline-flex items-center px-2 py-1 rounded-md bg-primary/10 text-primary text-xs font-medium"
                  >
                    {type}
                  </span>
                ))}
              </div>
            </div>
          )}

          {/* No entities message */}
          {(!result.entity_types || result.entity_types.length === 0) && (
            <div className="text-sm text-muted-foreground">
              <p>
                No graph entities found. Document chunks are available for
                semantic search.
              </p>
            </div>
          )}

          {/* Full schema if available */}
          {result.schema && (
            <details className="text-sm">
              <summary className="cursor-pointer font-medium">
                View full schema
              </summary>
              <pre className="mt-2 p-2 bg-background rounded text-xs whitespace-pre-wrap">
                {result.schema}
              </pre>
            </details>
          )}
        </div>
      );
    case "vector_search":
      return (
        <div className="space-y-2">
          {result.results?.slice(0, 3).map((doc: any, i: number) => (
            <div key={i} className="border-l-2 border-primary/30 pl-3">
              <div className="flex items-center gap-2">
                <div className="flex-1">
                  <p className="text-xs font-medium text-primary">
                    Score: {(doc.score * 100).toFixed(1)}%
                  </p>
                </div>
              </div>
              <p className="text-sm mt-1 line-clamp-3">{doc.text}</p>
            </div>
          ))}
          {result.count > 3 && (
            <p className="text-xs text-muted-foreground">
              ... and {result.count - 3} more results
            </p>
          )}
        </div>
      );

    case "entity_lookup":
      return (
        <div className="space-y-2">
          {result.entities?.map((entity: any, i: number) => (
            <div key={i} className="border rounded-md p-2 bg-background">
              <p className="text-sm font-medium">{entity.name}</p>
              <p className="text-xs text-muted-foreground">{entity.type}</p>
            </div>
          ))}
        </div>
      );

    case "graph_neighbors":
      // Build graph data from neighbors
      const neighborNodes: GraphNode[] = [];
      const neighborLinks: GraphLink[] = [];

      // Add center node
      neighborNodes.push({
        id: result.source_entity_id,
        name: "Center",
        type: "Center",
      });

      // Add neighbor nodes and links
      result.neighbors?.forEach((neighbor: any, i: number) => {
        const neighborId = neighbor.entity?.id || `neighbor-${i}`;
        neighborNodes.push({
          id: neighborId,
          name: neighbor.entity?.name || `Entity ${i}`,
          type: neighbor.entity?.type || "Unknown",
        });

        // Create link from center to neighbor
        neighborLinks.push({
          source: result.source_entity_id,
          target: neighborId,
          type: neighbor.relationships?.[0] || "RELATED_TO",
        });
      });

      return (
        <div className="space-y-3">
          <p className="text-sm font-medium">
            Found {result.total_found} neighbors
          </p>

          {/* Ego network visualization */}
          {neighborNodes.length > 1 && (
            <GraphPreview
              nodes={neighborNodes}
              links={neighborLinks}
              height={350}
              centerNodeId={result.source_entity_id}
            />
          )}

          {/* Neighbor list */}
          <div className="space-y-2">
            {result.neighbors?.slice(0, 5).map((neighbor: any, i: number) => (
              <div key={i} className="border-l-2 border-orange-500/30 pl-3">
                <p className="text-sm font-medium">{neighbor.entity?.name}</p>
                <p className="text-xs text-muted-foreground">
                  {neighbor.entity?.type}
                </p>
                {neighbor.relationships?.length > 0 && (
                  <p className="text-xs text-muted-foreground mt-1">
                    via {neighbor.relationships.join(", ")}
                  </p>
                )}
              </div>
            ))}
          </div>
        </div>
      );

    case "graph_query":
      // Try to extract graph data from Cypher results
      const queryNodes: GraphNode[] = [];
      const queryLinks: GraphLink[] = [];
      const seenNodeIds = new Set<string>();

      if (result.results && Array.isArray(result.results)) {
        result.results.forEach((row: any) => {
          // Look for node-like objects in the result
          Object.values(row).forEach((value: any) => {
            if (value && typeof value === "object") {
              // If it has id, name, type - treat as node
              if (value.id && !seenNodeIds.has(value.id)) {
                queryNodes.push({
                  id: value.id,
                  name: value.name || value.id,
                  type: value.type || "Unknown",
                  properties: value,
                });
                seenNodeIds.add(value.id);
              }
            }
          });
        });
      }

      return (
        <div className="space-y-3">
          <p className="text-sm font-medium mb-2">
            Query returned {result.count} results
          </p>

          {/* Graph visualization if we extracted nodes */}
          {queryNodes.length > 0 && (
            <div>
              <p className="text-sm font-medium mb-2">Graph View</p>
              <GraphPreview
                nodes={queryNodes}
                links={queryLinks}
                height={300}
              />
            </div>
          )}

          {/* JSON results */}
          <details>
            <summary className="text-sm font-medium cursor-pointer hover:text-primary">
              View raw results
            </summary>
            <pre className="text-xs overflow-x-auto mt-2">
              {JSON.stringify(result.results, null, 2)}
            </pre>
          </details>
        </div>
      );

    default:
      return (
        <pre className="text-xs overflow-x-auto">
          {JSON.stringify(result, null, 2)}
        </pre>
      );
  }
}
