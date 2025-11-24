/**
 * Chat message component.
 */

import React from 'react';
import { cn } from '@/lib/utils';
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from '@/components/ui/badge';
import { NetworkIcon, ArrowRightIcon } from "lucide-react";
import type { SourceChunk, RelatedEntity, EntityRelationship } from "@/lib/api";

export interface ChatMessageProps {
  role: "user" | "assistant";
  content: string;
  sources?: SourceChunk[];
  entities?: RelatedEntity[];
  relationships?: EntityRelationship[];
  timestamp: Date;
}

export function ChatMessage({
  role,
  content,
  sources,
  entities,
  relationships,
  timestamp,
}: ChatMessageProps) {
  const isUser = role === "user";

  return (
    <div
      className={cn(
        "flex w-full gap-3",
        isUser ? "justify-end" : "justify-start"
      )}
    >
      <div className={cn("max-w-[80%] space-y-2", isUser && "items-end")}>
        <div
          className={cn(
            "rounded-lg px-4 py-2",
            isUser ? "bg-primary text-primary-foreground" : "bg-muted"
          )}
        >
          <p className="text-sm whitespace-pre-wrap">{content}</p>
        </div>

        {!isUser && entities && entities.length > 0 && (
          <Card className="border-muted">
            <CardHeader className="p-3 pb-2">
              <CardTitle className="text-xs font-medium text-muted-foreground flex items-center gap-1">
                <NetworkIcon className="h-3 w-3" />
                Related Entities
              </CardTitle>
            </CardHeader>
            <CardContent className="p-3 pt-0">
              <div className="flex flex-wrap gap-1">
                {entities.map((entity, idx) => (
                  <Badge key={idx} variant="secondary" className="text-xs">
                    {entity.name}
                    {entity.type && (
                      <span className="ml-1 opacity-60">({entity.type})</span>
                    )}
                  </Badge>
                ))}
              </div>
            </CardContent>
          </Card>
        )}

        {!isUser && relationships && relationships.length > 0 && (
          <Card className="border-muted">
            <CardHeader className="p-3 pb-2">
              <CardTitle className="text-xs font-medium text-muted-foreground">
                Relationships
              </CardTitle>
            </CardHeader>
            <CardContent className="p-3 pt-0 space-y-1">
              {relationships.slice(0, 5).map((rel, idx) => (
                <div key={idx} className="flex items-center gap-1 text-xs">
                  <span className="font-medium">{rel.from_entity}</span>
                  <ArrowRightIcon className="h-3 w-3 text-muted-foreground" />
                  <Badge variant="outline" className="text-[10px]">
                    {rel.relationship_type}
                  </Badge>
                  <ArrowRightIcon className="h-3 w-3 text-muted-foreground" />
                  <span className="font-medium">{rel.to_entity}</span>
                </div>
              ))}
              {relationships.length > 5 && (
                <p className="text-xs text-muted-foreground italic">
                  +{relationships.length - 5} more relationships
                </p>
              )}
            </CardContent>
          </Card>
        )}

        {!isUser && sources && sources.length > 0 && (
          <Card className="border-muted">
            <CardHeader className="p-3 pb-2">
              <CardTitle className="text-xs font-medium text-muted-foreground">
                Sources
              </CardTitle>
            </CardHeader>
            <CardContent className="p-3 pt-0 space-y-2">
              {sources.map((source, idx) => (
                <div key={idx} className="text-xs space-y-1">
                  <div className="flex items-center gap-2">
                    <Badge variant="outline" className="text-[10px]">
                      {(source.score * 100).toFixed(1)}% match
                    </Badge>
                    <span className="text-muted-foreground">
                      Chunk {source.chunk_index}
                    </span>
                  </div>
                  <p className="text-muted-foreground line-clamp-2">
                    {source.text}
                  </p>
                </div>
              ))}
            </CardContent>
          </Card>
        )}

        <p className="text-xs text-muted-foreground px-1">
          {timestamp.toLocaleTimeString()}
        </p>
      </div>
    </div>
  );
}
