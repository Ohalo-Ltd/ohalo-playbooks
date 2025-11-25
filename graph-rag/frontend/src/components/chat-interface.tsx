/**
 * Main chat interface component with streaming agent reasoning.
 */

'use client';

import React, { useState, useRef, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { ChatInput } from "./chat-input";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Loader2Icon } from "lucide-react";
import { useAgentStream, AgentStep } from "@/hooks/use-agent-stream";
import { ReasoningAccordion } from "./reasoning-accordion";
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
  const viewportRef = useRef<HTMLDivElement>(null);
  const shouldAutoScrollRef = useRef(true);
  const prevMessagesLengthRef = useRef(messages.length);
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
            content: step.type === "answer_chunk" ? step.content || "" : "",
            steps: step.type !== "answer_chunk" ? [step] : [],
            timestamp: new Date(),
          };
        } else {
          const isChunk = step.type === "answer_chunk";
          const isAnswer = step.type === "answer";

          let newContent = prev.content;
          if (isChunk) {
            const chunkContent = step.content || "";
            // Check if the chunk is the full text (starts with previous content)
            // or a delta (append it)
            if (
              chunkContent.length > (prev.content?.length || 0) &&
              chunkContent.startsWith(prev.content || "")
            ) {
              newContent = chunkContent;
            } else {
              newContent = (prev.content || "") + chunkContent;
            }
          } else if (isAnswer) {
            newContent = step.content || "";
          }

          // Don't add chunks to steps array to avoid pollution
          const newSteps = isChunk ? prev.steps : [...(prev.steps || []), step];

          next = {
            ...prev,
            steps: newSteps,
            content: newContent,
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

    // Pass history to the agent
    const history = messages.map((m) => ({ role: m.role, content: m.content }));
    startStream(message, projectId, currentUser?.email, history);
  };

  useEffect(() => {
    const viewport = viewportRef.current;
    if (!viewport) return;

    const handleScroll = () => {
      const { scrollTop, scrollHeight, clientHeight } = viewport;
      // If user is within 50px of bottom, enable auto-scroll
      const isAtBottom = scrollHeight - scrollTop - clientHeight < 50;
      shouldAutoScrollRef.current = isAtBottom;
    };

    viewport.addEventListener("scroll", handleScroll);
    return () => viewport.removeEventListener("scroll", handleScroll);
  }, []);

  useEffect(() => {
    const isNewMessage = messages.length > prevMessagesLengthRef.current;

    if (isNewMessage || shouldAutoScrollRef.current) {
      if (scrollRef.current) {
        scrollRef.current.scrollIntoView({ behavior: "smooth" });
      }
      // If it was a new message, we force auto-scroll back on
      if (isNewMessage) {
        shouldAutoScrollRef.current = true;
      }
    }

    prevMessagesLengthRef.current = messages.length;
  }, [messages, currentAssistantMessage]);

  const hasMessages = messages.length > 0 || currentAssistantMessage;

  return (
    <div className="flex flex-col h-full">
      {hasMessages ? (
        <>
          <div className="flex-1 overflow-hidden">
            <ScrollArea className="h-full" viewportRef={viewportRef}>
              <div className="space-y-6 py-4 px-4 max-w-4xl mx-auto">
                {messages.map((message) => (
                  <div key={message.id} className="space-y-2">
                    {message.role === "user" ? (
                      <div className="flex justify-end">
                        <div className="bg-primary text-primary-foreground rounded-2xl px-4 py-3 max-w-[80%]">
                          <p className="text-sm">{message.content}</p>
                        </div>
                      </div>
                    ) : (
                      <div className="space-y-3">
                        {/* Reasoning accordion before answer */}
                        {message.steps && message.steps.length > 0 && (
                          <ReasoningAccordion
                            steps={message.steps}
                            isStreaming={false}
                          />
                        )}

                        {/* Answer content */}
                        {message.content && (
                          <div className="bg-muted/50 rounded-2xl p-4">
                            <div className="prose prose-sm max-w-none dark:prose-invert">
                              <ReactMarkdown remarkPlugins={[remarkGfm]}>
                                {message.content}
                              </ReactMarkdown>
                            </div>
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                ))}

                {/* Current streaming message */}
                {currentAssistantMessage && (
                  <div className="space-y-3">
                    {currentAssistantMessage.steps &&
                      currentAssistantMessage.steps.length > 0 && (
                        <ReasoningAccordion
                          steps={currentAssistantMessage.steps}
                          isStreaming={isStreaming}
                        />
                      )}

                    {currentAssistantMessage.content && (
                      <div className="bg-muted/50 rounded-2xl p-4">
                        <div className="prose prose-sm max-w-none dark:prose-invert">
                          <ReactMarkdown remarkPlugins={[remarkGfm]}>
                            {currentAssistantMessage.content}
                          </ReactMarkdown>
                        </div>
                      </div>
                    )}
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
          </div>

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
                <h2 className="text-3xl font-semibold">
                  Welcome to Data X-Ray RAG demo
                </h2>
                <p className="text-muted-foreground text-lg">
                  Ask questions about your documents and knowledge graph
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
