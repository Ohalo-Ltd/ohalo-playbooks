'use client';

import { useCallback, useRef, useEffect, useState } from 'react';
import dynamic from 'next/dynamic';
import { Maximize2 } from 'lucide-react';
import { Button } from './ui/button';
import { FullScreenGraphDialog } from './full-screen-graph-dialog';

// Dynamically import ForceGraph2D to avoid SSR issues
const ForceGraph2D = dynamic(() => import('react-force-graph-2d'), {
  ssr: false,
  loading: () => <div className="flex items-center justify-center h-full text-muted-foreground">Loading graph...</div>,
});

export interface GraphNode {
  id: string;
  name: string;
  type: string;
  properties?: Record<string, any>;
}

export interface GraphLink {
  source: string;
  target: string;
  type: string;
}

interface GraphPreviewProps {
  nodes: GraphNode[];
  links: GraphLink[];
  height?: number;
  onNodeClick?: (node: GraphNode) => void;
  onExpandClick?: () => void;
  centerNodeId?: string;
}

const NODE_COLORS: Record<string, string> = {
  File: '#3b82f6', // blue
  Person: '#10b981', // green
  Organization: '#f59e0b', // orange
  Location: '#8b5cf6', // purple
  Event: '#ef4444', // red
  Concept: '#06b6d4', // cyan
  default: '#6b7280', // gray
};

export function GraphPreview({
  nodes,
  links,
  height = 400,
  onNodeClick,
  onExpandClick,
  centerNodeId,
}: GraphPreviewProps) {
  const graphRef = useRef<any>(null);
  const [graphData, setGraphData] = useState<any>({ nodes: [], links: [] });
  const [showFullScreen, setShowFullScreen] = useState(false);

  useEffect(() => {
    // Transform data for react-force-graph
    const transformedNodes = nodes.map((node) => ({
      id: node.id,
      name: node.name,
      type: node.type,
      val: centerNodeId === node.id ? 15 : 10, // Larger center node
      color: NODE_COLORS[node.type] || NODE_COLORS.default,
      properties: node.properties,
    }));

    const transformedLinks = links.map((link) => ({
      source: link.source,
      target: link.target,
      type: link.type,
      color: '#94a3b8', // slate-400
    }));

    setGraphData({
      nodes: transformedNodes,
      links: transformedLinks,
    });

    // Center on center node after a short delay
    if (centerNodeId && graphRef.current) {
      setTimeout(() => {
        const centerNode = transformedNodes.find((n) => n.id === centerNodeId);
        if (centerNode && graphRef.current) {
          graphRef.current.centerAt(0, 0, 1000);
        }
      }, 100);
    }
  }, [nodes, links, centerNodeId]);

  const handleNodeClick = useCallback(
    (node: any) => {
      if (onNodeClick) {
        onNodeClick({
          id: node.id,
          name: node.name,
          type: node.type,
          properties: node.properties,
        });
      }
    },
    [onNodeClick]
  );

  if (nodes.length === 0) {
    return (
      <div className="flex items-center justify-center text-sm text-muted-foreground" style={{ height }}>
        No graph data to display
      </div>
    );
  }

  return (
    <div className="relative border rounded-md bg-background" style={{ height }}>
      {/* Expand button - use either custom callback or show full-screen dialog */}
      <Button
        variant="ghost"
        size="icon"
        className="absolute top-2 right-2 z-10 bg-background/80 backdrop-blur-sm"
        onClick={() => {
          if (onExpandClick) {
            onExpandClick();
          } else {
            setShowFullScreen(true);
          }
        }}
      >
        <Maximize2 className="h-4 w-4" />
      </Button>

      <ForceGraph2D
        ref={graphRef}
        graphData={graphData}
        nodeLabel="name"
        nodeColor="color"
        nodeVal="val"
        nodeCanvasObject={(node: any, ctx: CanvasRenderingContext2D, globalScale: number) => {
          // Draw node circle
          const size = node.val || 5;
          ctx.beginPath();
          ctx.arc(node.x, node.y, size, 0, 2 * Math.PI);
          ctx.fillStyle = node.color;
          ctx.fill();

          // Draw node label
          const label = node.name;
          const fontSize = 12 / globalScale;
          ctx.font = `${fontSize}px Sans-Serif`;
          ctx.textAlign = 'center';
          ctx.textBaseline = 'middle';
          ctx.fillStyle = '#000';
          ctx.fillText(label, node.x, node.y + size + fontSize);

          // Draw type label (smaller, above node)
          if (node.type) {
            const typeFontSize = 10 / globalScale;
            ctx.font = `${typeFontSize}px Sans-Serif`;
            ctx.fillStyle = '#666';
            ctx.fillText(node.type, node.x, node.y - size - typeFontSize);
          }
        }}
        linkLabel="type"
        linkColor="color"
        linkDirectionalArrowLength={6}
        linkDirectionalArrowRelPos={1}
        linkCanvasObject={(link: any, ctx: CanvasRenderingContext2D, globalScale: number) => {
          // Draw link line
          const start = link.source;
          const end = link.target;

          ctx.beginPath();
          ctx.moveTo(start.x, start.y);
          ctx.lineTo(end.x, end.y);
          ctx.strokeStyle = link.color;
          ctx.lineWidth = 1 / globalScale;
          ctx.stroke();

          // Draw link label
          if (link.type) {
            const midX = (start.x + end.x) / 2;
            const midY = (start.y + end.y) / 2;
            const fontSize = 10 / globalScale;
            ctx.font = `${fontSize}px Sans-Serif`;
            ctx.textAlign = 'center';
            ctx.textBaseline = 'middle';
            ctx.fillStyle = '#666';
            ctx.fillText(link.type, midX, midY);
          }
        }}
        onNodeClick={handleNodeClick}
        cooldownTicks={100}
        d3VelocityDecay={0.3}
      />

      {/* Full-screen dialog */}
      <FullScreenGraphDialog
        open={showFullScreen}
        onOpenChange={setShowFullScreen}
        nodes={nodes}
        links={links}
        title="Graph Explorer"
      />
    </div>
  );
}
