'use client';

import { useState, useCallback } from 'react';
import dynamic from 'next/dynamic';
import { X, ZoomIn, ZoomOut, Maximize2 } from 'lucide-react';
import { Dialog, DialogContent } from './ui/dialog';
import { Button } from './ui/button';
import { GraphNode, GraphLink } from './graph-preview';
import { PropertyPanel } from './property-panel';

// Dynamically import ForceGraph2D
const ForceGraph2D = dynamic(() => import('react-force-graph-2d'), {
  ssr: false,
  loading: () => (
    <div className="flex items-center justify-center h-full text-muted-foreground">
      Loading graph...
    </div>
  ),
});

interface FullScreenGraphDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  nodes: GraphNode[];
  links: GraphLink[];
  title?: string;
}

const NODE_COLORS: Record<string, string> = {
  File: '#3b82f6',
  Person: '#10b981',
  Organization: '#f59e0b',
  Location: '#8b5cf6',
  Event: '#ef4444',
  Concept: '#06b6d4',
  Center: '#ec4899',
  default: '#6b7280',
};

export function FullScreenGraphDialog({
  open,
  onOpenChange,
  nodes,
  links,
  title = 'Graph Explorer',
}: FullScreenGraphDialogProps) {
  const [selectedNode, setSelectedNode] = useState<GraphNode | null>(null);
  const [graphData, setGraphData] = useState<any>({ nodes: [], links: [] });

  // Transform data for visualization
  const transformedData = {
    nodes: nodes.map((node) => ({
      id: node.id,
      name: node.name,
      type: node.type,
      val: 10,
      color: NODE_COLORS[node.type] || NODE_COLORS.default,
      properties: node.properties,
    })),
    links: links.map((link) => ({
      source: link.source,
      target: link.target,
      type: link.type,
      color: '#94a3b8',
    })),
  };

  const handleNodeClick = useCallback((node: any) => {
    setSelectedNode({
      id: node.id,
      name: node.name,
      type: node.type,
      properties: node.properties,
    });
  }, []);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-[95vw] h-[95vh] p-0 gap-0">
        <div className="flex h-full">
          {/* Main graph area */}
          <div className="flex-1 flex flex-col">
            {/* Header */}
            <div className="p-4 border-b flex items-center justify-between bg-background">
              <div>
                <h2 className="text-lg font-semibold">{title}</h2>
                <p className="text-sm text-muted-foreground mt-1">
                  {nodes.length} nodes, {links.length} relationships
                </p>
              </div>
              <div className="flex items-center gap-2">
                <Button variant="outline" size="sm" onClick={() => onOpenChange(false)}>
                  <X className="h-4 w-4 mr-2" />
                  Close
                </Button>
              </div>
            </div>

            {/* Graph canvas */}
            <div className="flex-1 bg-muted/10">
              {transformedData.nodes.length > 0 ? (
                <ForceGraph2D
                  graphData={transformedData}
                  nodeLabel="name"
                  nodeColor="color"
                  nodeVal="val"
                  nodeCanvasObject={(
                    node: any,
                    ctx: CanvasRenderingContext2D,
                    globalScale: number
                  ) => {
                    const size = node.val || 5;
                    
                    // Draw node circle
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

                    // Draw type label
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
                  linkCanvasObject={(
                    link: any,
                    ctx: CanvasRenderingContext2D,
                    globalScale: number
                  ) => {
                    const start = link.source;
                    const end = link.target;

                    // Draw link line
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
              ) : (
                <div className="flex items-center justify-center h-full text-muted-foreground">
                  No graph data to display
                </div>
              )}
            </div>
          </div>

          {/* Property panel */}
          {selectedNode && (
            <PropertyPanel node={selectedNode} onClose={() => setSelectedNode(null)} />
          )}
        </div>
      </DialogContent>
    </Dialog>
  );
}
