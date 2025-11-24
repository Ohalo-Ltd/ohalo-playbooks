/**
 * Main chat interface component.
 */

'use client';

import React, { useState, useRef, useEffect } from 'react';
import { useMutation } from '@tanstack/react-query';
import {
  apiClient,
  QueryResponse,
  SourceChunk,
  RelatedEntity,
  EntityRelationship,
} from "@/lib/api";
import { ChatMessage } from './chat-message';
import { ChatInput } from "./chat-input";
import { ScrollArea } from '@/components/ui/scroll-area';
import { Loader2Icon } from 'lucide-react';

interface Message {
  id: string;
  role: "user" | "assistant";
  content: string;
  sources?: SourceChunk[];
  entities?: RelatedEntity[];
  relationships?: EntityRelationship[];
  timestamp: Date;
}

interface ChatInterfaceProps {
  projectId?: string;
}

export function ChatInterface({ projectId = "default" }: ChatInterfaceProps) {
  const [messages, setMessages] = useState<Message[]>([]);
  const scrollRef = useRef<HTMLDivElement>(null);

  const queryMutation = useMutation({
    mutationFn: async (question: string) => {
      return apiClient.query({
        question,
        project_id: projectId,
        top_k: 5,
        include_graph_context: true,
      });
    },
    onSuccess: (response: QueryResponse) => {
      const assistantMessage: Message = {
        id: crypto.randomUUID(),
        role: "assistant",
        content: response.answer,
        sources: response.sources,
        entities: response.related_entities,
        relationships: response.relationships,
        timestamp: new Date(),
      };

      setMessages((prev) => [...prev, assistantMessage]);
    },
    onError: (error: Error) => {
      const errorMessage: Message = {
        id: crypto.randomUUID(),
        role: "assistant",
        content: `Sorry, I encountered an error: ${error.message}`,
        timestamp: new Date(),
      };

      setMessages((prev) => [...prev, errorMessage]);
    },
  });

  const handleSend = (message: string) => {
    const userMessage: Message = {
      id: crypto.randomUUID(),
      role: "user",
      content: message,
      timestamp: new Date(),
    };

    setMessages((prev) => [...prev, userMessage]);
    queryMutation.mutate(message);
  };

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollIntoView({ behavior: "smooth" });
    }
  }, [messages]);

  return (
    <div className="flex flex-col h-full max-w-5xl mx-auto">
      <ScrollArea className="flex-1 px-4">
        <div className="space-y-4 py-4">
          {messages.length === 0 && (
            <div className="text-center text-muted-foreground py-12">
              <p className="text-lg font-medium">Welcome to Graph RAG</p>
              <p className="text-sm mt-2">
                Ask questions about your documents and knowledge graph
              </p>
            </div>
          )}

          {messages.map((message) => (
            <ChatMessage
              key={message.id}
              role={message.role}
              content={message.content}
              sources={message.sources}
              entities={message.entities}
              relationships={message.relationships}
              timestamp={message.timestamp}
            />
          ))}

          {queryMutation.isPending && (
            <div className="flex items-center gap-2 text-muted-foreground">
              <Loader2Icon className="h-4 w-4 animate-spin" />
              <span className="text-sm">Thinking...</span>
            </div>
          )}

          <div ref={scrollRef} />
        </div>
      </ScrollArea>

      <div className="border-t p-4">
        <ChatInput
          onSend={handleSend}
          disabled={queryMutation.isPending}
          placeholder="Ask a question about your documents..."
        />
      </div>
    </div>
  );
}
