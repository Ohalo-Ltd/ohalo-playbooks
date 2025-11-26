/**
 * Reasoning accordion component that shows agent thinking steps progressively.
 * Updates in real-time as new steps arrive, showing current status.
 * User can expand to see all steps.
 */

'use client';

import React, { useState } from 'react';
import { Database, FileSearch, Network, Search, Sparkles, Loader2 } from 'lucide-react';
import { Accordion, AccordionContent, AccordionItem, AccordionTrigger } from './ui/accordion';
import { cn } from '@/lib/utils';
import { AgentStep } from '@/hooks/use-agent-stream';

interface ReasoningAccordionProps {
  steps: AgentStep[];
  isStreaming?: boolean;
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
    label: 'Checking graph structure',
    color: 'text-blue-600',
  },
  vector_search: {
    icon: Search,
    label: 'Searching documents',
    color: 'text-purple-600',
  },
  entity_lookup: {
    icon: FileSearch,
    label: 'Looking up entities',
    color: 'text-green-600',
  },
  graph_neighbors: {
    icon: Network,
    label: 'Exploring relationships',
    color: 'text-orange-600',
  },
  graph_query: {
    icon: Database,
    label: 'Running graph query',
    color: 'text-red-600',
  },
};

export function ReasoningAccordion({ steps, isStreaming }: ReasoningAccordionProps) {
  const [isExpanded, setIsExpanded] = useState<string | undefined>(undefined);

  // Get current status (last step that's in progress or just completed)
  const getCurrentStatus = (): { text: string; icon: React.ReactNode } => {
    if (steps.length === 0) {
      return {
        text: 'Connecting to agent...',
        icon: <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />,
      };
    }

    const lastStep = steps[steps.length - 1];

    if (lastStep.type === 'thinking') {
      return {
        text: lastStep.content || 'Thinking...',
        icon: <Sparkles className="h-4 w-4 animate-pulse text-muted-foreground" />,
      };
    }

    if (lastStep.type === 'tool_call_start' && lastStep.tool) {
      const config = TOOL_CONFIG[lastStep.tool];
      const Icon = config?.icon || Database;
      // Extract descriptive message from args if available
      const description = lastStep.args?.description || config?.label || lastStep.tool;
      return {
        text: description,
        icon: <Icon className={cn('h-4 w-4', config?.color || 'text-muted-foreground')} />,
      };
    }

    if (lastStep.type === "tool_call_result" && lastStep.tool) {
      return {
        text: "Thinking...",
        icon: (
          <Sparkles className="h-4 w-4 animate-pulse text-muted-foreground" />
        ),
      };
    }

    if (lastStep.type === 'answer') {
      return {
        text: 'Answer ready',
        icon: <Sparkles className="h-4 w-4 text-green-600" />,
      };
    }

    if (lastStep.type === 'error') {
      return {
        text: lastStep.message || 'An error occurred',
        icon: <span className="h-4 w-4 text-destructive">⚠</span>,
      };
    }

    return {
      text: 'Processing...',
      icon: <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />,
    };
  };

  const currentStatus = getCurrentStatus();
  const stepCount = steps.filter(
    (s) => s.type !== "answer" && s.type !== "answer_chunk"
  ).length;

  return (
    <div
      className={cn(
        "border rounded-lg overflow-hidden transition-all duration-500 relative",
        isStreaming
          ? "border-primary/30 shadow-[0_0_10px_-5px_rgba(var(--primary),0.2)]"
          : "bg-muted/30 border-muted"
      )}
    >
      {isStreaming && (
        <div
          className="absolute inset-0 bg-linear-to-r from-transparent via-primary/5 to-transparent animate-[shimmer_2s_infinite] pointer-events-none"
          style={{ backgroundSize: "200% 100%" }}
        />
      )}
      <Accordion
        type="single"
        collapsible
        value={isExpanded}
        onValueChange={setIsExpanded}
      >
        <AccordionItem value="reasoning" className="border-none">
          <AccordionTrigger className="px-4 py-3 hover:no-underline hover:bg-muted/50 transition-colors">
            <div className="flex items-center gap-3 flex-1">
              {currentStatus.icon}
              <div className="flex-1 text-left">
                <p className="text-sm font-medium">{currentStatus.text}</p>
                {stepCount > 0 && (
                  <p className="text-xs text-muted-foreground">
                    {stepCount} reasoning {stepCount === 1 ? "step" : "steps"}
                  </p>
                )}
              </div>
              {isStreaming && (
                <div className="h-2 w-2 rounded-full bg-blue-500 animate-pulse" />
              )}
            </div>
          </AccordionTrigger>
          <AccordionContent className="px-4 pb-3">
            <div className="space-y-3 pt-2">
              {steps
                .filter((s) => s.type !== "answer" && s.type !== "answer_chunk")
                .map((step, i) => (
                  <StepItem key={i} step={step} />
                ))}
            </div>
          </AccordionContent>
        </AccordionItem>
      </Accordion>
    </div>
  );
}

function StepItem({ step }: { step: AgentStep }) {
  if (step.type === 'thinking') {
    return (
      <div className="flex items-start gap-3 py-1">
        <Sparkles className="h-4 w-4 mt-0.5 text-muted-foreground" />
        <p className="text-sm text-muted-foreground italic">{step.content}</p>
      </div>
    );
  }

  if (step.type === 'tool_call_start' && step.tool) {
    const config = TOOL_CONFIG[step.tool];
    const Icon = config?.icon || Database;
    const description = step.args?.description || config?.label || step.tool;

    return (
      <div className="flex items-start gap-3 py-1">
        <Icon
          className={cn(
            "h-4 w-4 mt-0.5",
            config?.color || "text-muted-foreground"
          )}
        />
        <div className="flex-1">
          <p className="text-sm font-medium">{description}</p>
          {step.args && step.args.description !== description && (
            <p className="text-xs text-muted-foreground mt-0.5">
              {Object.entries(step.args)
                .filter(([k]) => k !== "description")
                .map(([k, v]) => `${k}: ${JSON.stringify(v)}`)
                .join(", ")}
            </p>
          )}
        </div>
      </div>
    );
  }

  if (step.type === 'tool_call_result' && step.tool) {
    const config = TOOL_CONFIG[step.tool];
    const Icon = config?.icon || Database;
    const summary = renderResultSummary(step.tool, step.result);

    return (
      <div className="flex items-start gap-3 py-1 opacity-70">
        <Icon
          className={cn(
            "h-4 w-4 mt-0.5",
            config?.color || "text-muted-foreground"
          )}
        />
        <div className="flex-1">
          <p className="text-sm">
            <span className="font-semibold text-foreground">
              {config?.label || step.tool}
            </span>{" "}
            <span className="text-secondary-foreground font-normal">
              completed
            </span>
          </p>
          {summary && (
            <p className="text-xs text-secondary-foreground mt-0.5">
              {summary}
            </p>
          )}
        </div>
        <span className="text-green-600 text-xs">✓</span>
      </div>
    );
  }

  if (step.type === 'error') {
    return (
      <div className="flex items-start gap-3 py-1 text-destructive">
        <span className="text-lg mt-0.5">⚠</span>
        <div className="flex-1">
          <p className="text-sm font-medium">Error</p>
          <p className="text-xs mt-0.5">{step.message}</p>
        </div>
      </div>
    );
  }

  return null;
}

function renderResultSummary(tool: string, result: unknown): string {
  if (!result) return '';

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const res = result as Record<string, any>;

  switch (tool) {
    case 'discover_graph':
      return `Found ${res.entity_count || 0} entities, ${res.relationship_count || 0} relationships`;
    case 'vector_search':
      return `Found ${res.count || 0} relevant documents`;
    case 'entity_lookup':
      return `Found ${res.count || 0} matching entities`;
    case 'graph_neighbors':
      return `Found ${res.total_found || 0} neighbors`;
    case 'graph_query':
      return `Returned ${res.count || 0} results`;
    default:
      return '';
  }
}
