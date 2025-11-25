/**
 * Main chat interface component with streaming agent reasoning.
 */

'use client';

import React, { useState, useRef, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { ChatInput } from "./chat-input";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Loader2Icon } from "lucide-react";
import { useAgentStream, AgentStep } from "@/hooks/use-agent-stream";
import { ReasoningStep } from "./reasoning-step";
import { UserSwitcher, User } from "./user-switcher";
import { apiClient } from "@/lib/api";

interface Message {
  id: string;
  role: "user" | "assistant";
  content: string;
  steps?: AgentStep[];
  timestamp: Date;
}

interface ChatInterfaceProps {
  projectId?: string;
  hasProjects?: boolean;
  currentUser?: User | null;
}

export function ChatInterface({
  projectId,
  hasProjects = true,
  currentUser,
}: ChatInterfaceProps) {
  const [messages, setMessages] = useState<Message[]>([]);
  const [currentAssistantMessage, setCurrentAssistantMessage] =
    useState<Message | null>(null);
  const scrollRef = useRef<HTMLDivElement>(null);
  const completionHandledRef = useRef(false);
  const currentAssistantRef = useRef<Message | null>(null);
  // Fetch project details
  const { data: project } = useQuery({
    queryKey: ["project", projectId],
    queryFn: () => apiClient.getProject(projectId!),
    enabled: !!projectId,
  });

  const { steps, isStreaming, error, startStream } = useAgentStream({
    onStep: (step) => {
      console.log("[ChatInterface] onStep called:", step.type);
      // Update current assistant message with new step
      setCurrentAssistantMessage((prev) => {
        let next: Message;
        if (!prev) {
          next = {
            id: "", // Will be set on completion
            role: "assistant",
            content: "",
            steps: [step],
            timestamp: new Date(),
          };
        } else {
          next = {
            ...prev,
            steps: [...(prev.steps || []), step],
            content: step.type === "answer" ? step.content || "" : prev.content,
          };
        }
        currentAssistantRef.current = next;
        return next;
      });
    },
    onComplete: () => {
      console.log(
        "[ChatInterface] onComplete called, already handled:",
        completionHandledRef.current
      );

      // Prevent duplicate handling
      if (completionHandledRef.current) {
        console.log("[ChatInterface] onComplete already handled, skipping");
        return;
      }
      completionHandledRef.current = true;

      const assistantMessage = currentAssistantRef.current;
      console.log(
        "[ChatInterface] onComplete - current ref message:",
        assistantMessage ? "exists" : "null"
      );
      if (assistantMessage) {
        const finalMessage: Message = {
          ...assistantMessage,
          id: crypto.randomUUID(),
        };
        setMessages((prev) => [...prev, finalMessage]);
      }
      currentAssistantRef.current = null;
      setCurrentAssistantMessage(null);
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
    // Reset completion flag and pending message for new query
    completionHandledRef.current = false;
    currentAssistantRef.current = null;

    const userMessage: Message = {
      id: crypto.randomUUID(),
      role: "user",
      content: message,
      timestamp: new Date(),
    };

    setMessages((prev) => [...prev, userMessage]);
    startStream(message, projectId, currentUser?.email);
  };

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollIntoView({ behavior: "smooth" });
    }
  }, [messages, currentAssistantMessage]);

  const hasMessages = messages.length > 0 || currentAssistantMessage;

  return (
    <div className="flex flex-col h-full">
      {hasMessages ? (
        <>
          <ScrollArea className="flex-1 px-4">
            <div className="space-y-6 py-4 max-w-4xl mx-auto">
              {messages.map((message) => (
                <div key={message.id} className="space-y-2">
                  {message.role === "user" ? (
                    <div className="flex justify-end">
                      <div className="bg-primary text-primary-foreground rounded-2xl px-4 py-3 max-w-[80%]">
                        <p className="text-sm">{message.content}</p>
                      </div>
                    </div>
                  ) : (
                    <div className="bg-muted/50 rounded-2xl p-4 space-y-2">
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
                <div className="bg-muted/50 rounded-2xl p-4 space-y-2">
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
            <div className="max-w-4xl mx-auto">
              <ChatInput
                onSend={handleSend}
                disabled={isStreaming || !projectId || !hasProjects}
                placeholder={
                  !hasProjects
                    ? "Create a project first to start chatting..."
                    : !projectId
                    ? "Select a project to start chatting..."
                    : "Ask a question about your documents..."
                }
              />
            </div>
          </div>
        </>
      ) : (
        <div className="flex flex-col items-center justify-center h-full px-4">
          {!hasProjects ? (
            <div className="text-center space-y-4 max-w-md">
              <h2 className="text-2xl font-semibold">No Projects Yet</h2>
              <p className="text-muted-foreground">
                Create your first project to start organizing your knowledge
                base and chatting with your documents.
              </p>
              <p className="text-sm text-muted-foreground">
                Click the project switcher in the top bar to create a new
                project.
              </p>
            </div>
          ) : !projectId ? (
            <div className="text-center space-y-4 max-w-md">
              <h2 className="text-2xl font-semibold">Select a Project</h2>
              <p className="text-muted-foreground">
                Choose a project from the switcher above to start chatting.
              </p>
            </div>
          ) : (
            <div className="w-full max-w-2xl space-y-8">
              <div className="text-center space-y-4">
                <h2 className="text-3xl font-semibold">Welcome to Graph RAG</h2>
                <p className="text-muted-foreground text-lg">
                  Ask questions about your documents and knowledge graph
                </p>
                <p className="text-sm text-muted-foreground/70">
                  Watch the agent think and explore the graph in real-time
                </p>
              </div>

              <div className="w-full">
                <ChatInput
                  onSend={handleSend}
                  disabled={isStreaming}
                  placeholder="Ask a question about your documents..."
                  centered
                />
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
