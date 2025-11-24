/**
 * Main chat interface component with streaming agent reasoning.
 */

'use client';

import React, { useState, useRef, useEffect } from "react";
import { ChatInput } from "./chat-input";
import { ScrollArea } from '@/components/ui/scroll-area';
import { Loader2Icon } from 'lucide-react';
import { useAgentStream, AgentStep } from "@/hooks/use-agent-stream";
import { ReasoningStep } from "./reasoning-step";

interface Message {
  id: string;
  role: "user" | "assistant";
  content: string;
  steps?: AgentStep[];
  timestamp: Date;
}

interface ChatInterfaceProps {
  projectId?: string;
}

export function ChatInterface({ projectId = "default" }: ChatInterfaceProps) {
  const [messages, setMessages] = useState<Message[]>([]);
  const [currentAssistantMessage, setCurrentAssistantMessage] =
    useState<Message | null>(null);
  const scrollRef = useRef<HTMLDivElement>(null);

  const { steps, isStreaming, error, startStream } = useAgentStream({
    onStep: (step) => {
      // Update current assistant message with new step
      setCurrentAssistantMessage((prev) => {
        if (!prev) {
          return {
            id: crypto.randomUUID(),
            role: "assistant",
            content: "",
            steps: [step],
            timestamp: new Date(),
          };
        }
        return {
          ...prev,
          steps: [...(prev.steps || []), step],
          content: step.type === "answer" ? step.content || "" : prev.content,
        };
      });
    },
    onComplete: () => {
      // Finalize assistant message
      if (currentAssistantMessage) {
        setMessages((prev) => [...prev, currentAssistantMessage]);
        setCurrentAssistantMessage(null);
      }
    },
    onError: (errorMsg) => {
      const errorMessage: Message = {
        id: crypto.randomUUID(),
        role: "assistant",
        content: `Error: ${errorMsg}`,
        timestamp: new Date(),
      };
      setMessages((prev) => [...prev, errorMessage]);
      setCurrentAssistantMessage(null);
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
    startStream(message, projectId);
  };

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollIntoView({ behavior: "smooth" });
    }
  }, [messages, currentAssistantMessage]);

  return (
    <div className="flex flex-col h-full max-w-5xl mx-auto">
      <ScrollArea className="flex-1 px-4">
        <div className="space-y-6 py-4">
          {messages.length === 0 && !currentAssistantMessage && (
            <div className="text-center text-muted-foreground py-12">
              <p className="text-lg font-medium">Welcome to Graph RAG</p>
              <p className="text-sm mt-2">
                Ask questions about your documents and knowledge graph
              </p>
              <p className="text-xs mt-4 text-muted-foreground/70">
                Watch the agent think and explore the graph in real-time
              </p>
            </div>
          )}

          {messages.map((message) => (
            <div key={message.id} className="space-y-2">
              {message.role === "user" ? (
                <div className="flex justify-end">
                  <div className="bg-primary text-primary-foreground rounded-lg px-4 py-2 max-w-[80%]">
                    <p className="text-sm">{message.content}</p>
                  </div>
                </div>
              ) : (
                <div className="bg-muted/50 rounded-lg p-4 space-y-2">
                  {message.steps?.map((step, i) => (
                    <ReasoningStep
                      key={i}
                      step={step}
                      isLast={i === (message.steps?.length || 0) - 1}
                    />
                  ))}
                  {!message.steps && (
                    <p className="text-sm">{message.content}</p>
                  )}
                </div>
              )}
            </div>
          ))}

          {/* Current streaming message */}
          {currentAssistantMessage && (
            <div className="bg-muted/50 rounded-lg p-4 space-y-2">
              {currentAssistantMessage.steps?.map((step, i) => (
                <ReasoningStep
                  key={i}
                  step={step}
                  isLast={
                    i === (currentAssistantMessage.steps?.length || 0) - 1
                  }
                />
              ))}
            </div>
          )}

          {isStreaming && !currentAssistantMessage?.steps?.length && (
            <div className="flex items-center gap-2 text-muted-foreground">
              <Loader2Icon className="h-4 w-4 animate-spin" />
              <span className="text-sm">Connecting to agent...</span>
            </div>
          )}

          <div ref={scrollRef} />
        </div>
      </ScrollArea>

      <div className="border-t p-4">
        <ChatInput
          onSend={handleSend}
          disabled={isStreaming}
          placeholder="Ask a question about your documents..."
        />
      </div>
    </div>
  );
}
