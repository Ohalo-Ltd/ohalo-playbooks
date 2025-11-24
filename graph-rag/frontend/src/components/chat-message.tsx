/**
 * Chat message component.
 */

import React from 'react';
import { cn } from '@/lib/utils';
import { Card, CardContent } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';

export interface ChatMessageProps {
  role: 'user' | 'assistant';
  content: string;
  sources?: Array<{
    file_name: string;
    similarity: number;
    text: string;
  }>;
  entities?: string[];
  timestamp: Date;
}

export function ChatMessage({ role, content, sources, entities, timestamp }: ChatMessageProps) {
  const isUser = role === 'user';

  return (
    <div className={cn('flex w-full gap-3', isUser ? 'justify-end' : 'justify-start')}>
      <div className={cn('max-w-[80%] space-y-2', isUser && 'items-end')}>
        <div
          className={cn(
            'rounded-lg px-4 py-2',
            isUser
              ? 'bg-primary text-primary-foreground'
              : 'bg-muted'
          )}
        >
          <p className="text-sm whitespace-pre-wrap">{content}</p>
        </div>

        {!isUser && entities && entities.length > 0 && (
          <div className="flex flex-wrap gap-1">
            {entities.map((entity, idx) => (
              <Badge key={idx} variant="secondary" className="text-xs">
                {entity}
              </Badge>
            ))}
          </div>
        )}

        {!isUser && sources && sources.length > 0 && (
          <Card className="border-muted">
            <CardContent className="p-3 space-y-2">
              <p className="text-xs font-medium text-muted-foreground">Sources:</p>
              {sources.map((source, idx) => (
                <div key={idx} className="text-xs space-y-1">
                  <div className="flex items-center gap-2">
                    <span className="font-medium">{source.file_name}</span>
                    <Badge variant="outline" className="text-[10px]">
                      {(source.similarity * 100).toFixed(1)}%
                    </Badge>
                  </div>
                  <p className="text-muted-foreground line-clamp-2">{source.text}</p>
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
