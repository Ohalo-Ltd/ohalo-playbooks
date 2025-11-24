'use client';

import { X } from 'lucide-react';
import { GraphNode } from './graph-preview';
import { Button } from './ui/button';
import { ScrollArea } from './ui/scroll-area';
import { Separator } from './ui/separator';

interface PropertyPanelProps {
  node: GraphNode | null;
  onClose: () => void;
}

export function PropertyPanel({ node, onClose }: PropertyPanelProps) {
  if (!node) return null;

  return (
    <div className="w-80 border-l bg-background h-full flex flex-col">
      {/* Header */}
      <div className="p-4 border-b flex items-center justify-between">
        <div>
          <h3 className="font-semibold text-sm">Node Properties</h3>
          <p className="text-xs text-muted-foreground mt-1">{node.type}</p>
        </div>
        <Button variant="ghost" size="icon" onClick={onClose}>
          <X className="h-4 w-4" />
        </Button>
      </div>

      {/* Content */}
      <ScrollArea className="flex-1 p-4">
        <div className="space-y-4">
          {/* Node ID */}
          <div>
            <p className="text-xs font-medium text-muted-foreground uppercase">ID</p>
            <p className="text-sm mt-1 font-mono break-all">{node.id}</p>
          </div>

          <Separator />

          {/* Node Name */}
          <div>
            <p className="text-xs font-medium text-muted-foreground uppercase">Name</p>
            <p className="text-sm mt-1">{node.name}</p>
          </div>

          <Separator />

          {/* Node Type */}
          <div>
            <p className="text-xs font-medium text-muted-foreground uppercase">Type</p>
            <p className="text-sm mt-1">{node.type}</p>
          </div>

          {/* Additional Properties */}
          {node.properties && Object.keys(node.properties).length > 0 && (
            <>
              <Separator />
              <div>
                <p className="text-xs font-medium text-muted-foreground uppercase mb-2">
                  Additional Properties
                </p>
                <div className="space-y-2">
                  {Object.entries(node.properties).map(([key, value]) => {
                    // Skip already displayed properties
                    if (['id', 'name', 'type'].includes(key)) return null;

                    return (
                      <div key={key} className="bg-muted/30 rounded p-2">
                        <p className="text-xs font-medium text-muted-foreground">{key}</p>
                        <p className="text-sm mt-1 break-all">
                          {typeof value === 'object' ? JSON.stringify(value, null, 2) : String(value)}
                        </p>
                      </div>
                    );
                  })}
                </div>
              </div>
            </>
          )}
        </div>
      </ScrollArea>
    </div>
  );
}
