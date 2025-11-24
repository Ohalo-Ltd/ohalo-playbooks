/**
 * Main chat interface component.
 */

'use client';

import React, { useState, useRef, useEffect } from 'react';
import { useMutation } from '@tanstack/react-query';
import { apiClient, QueryResponse } from '@/lib/api';
import { ChatMessage } from './chat-message';
import { ChatInput } from './chat-input';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Loader2Icon } from 'lucide-react';

interface Message {
  id: string;
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

export function ChatInterface() {
  const [messages, setMessages] = useState<Message[]>([]);
  const [conversationId, setConversationId] = useState<string | undefined>();
  const scrollRef = useRef<HTMLDivElement>(null);

  const queryMutation = useMutation({
    mutationFn: async (query: string) => {
      return apiClient.query({ query, conversation_id: conversationId });
    },
    onSuccess: (response: QueryResponse) => {
      setConversationId(response.conversation_id);
      
      const assistantMessage: Message = {
        id: crypto.randomUUID(),
        role: 'assistant',
        content: response.answer,
        sources: response.sources,
        entities: response.entities_mentioned,
        timestamp: new Date(),
      };
      
      setMessages((prev) => [...prev, assistantMessage]);
    },
    onError: (error: Error) => {
      const errorMessage: Message = {
        id: crypto.randomUUID(),
        role: 'assistant',
        content: `Sorry, I encountered an error: ${error.message}`,
        timestamp: new Date(),
      };
      
      setMessages((prev) => [...prev, errorMessage]);
    },
  });

  const handleSend = (message: string) => {
    const userMessage: Message = {
      id: crypto.randomUUID(),
      role: 'user',
      content: message,
      timestamp: new Date(),
    };
    
    setMessages((prev) => [...prev, userMessage]);
    queryMutation.mutate(message);
  };

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [messages]);

  return (
    <Card className="flex flex-col h-[calc(100vh-8rem)]">
      <CardHeader>
        <CardTitle>Graph RAG Chat</CardTitle>
      </CardHeader>
      <CardContent className="flex-1 flex flex-col gap-4 overflow-hidden">
        <ScrollArea className="flex-1 pr-4">
          <div className="space-y-4">
            {messages.length === 0 && (
              <div className="text-center text-muted-foreground py-12">
                <p className="text-lg font-medium">Welcome to Graph RAG</p>
                <p className="text-sm mt-2">Ask questions about your documents and knowledge graph</p>
              </div>
            )}
            
            {messages.map((message) => (
              <ChatMessage
                key={message.id}
                role={message.role}
                content={message.content}
                sources={message.sources}
                entities={message.entities}
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

        <ChatInput
          onSend={handleSend}
          disabled={queryMutation.isPending}
          placeholder="Ask a question about your documents..."
        />
      </CardContent>
    </Card>
  );
}
