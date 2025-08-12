"use client"

import * as React from "react"
import { Send, Copy, CheckCircle, ChevronDown } from "lucide-react";
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuItem,
} from "@/components/ui/dropdown-menu";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import { cn } from "@/lib/utils";
import { Card } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { DEMO_USERS, type User } from "@/components/user-selector";
import ReactMarkdown from "react-markdown";

interface Message {
  id: string;
  role: "user" | "assistant";
  content: string;
  timestamp: Date;
}

export function ChatInterface() {
  const [messages, setMessages] = React.useState<Message[]>([]);
  const [input, setInput] = React.useState("");
  const [isLoading, setIsLoading] = React.useState(false);
  const [selectedUser, setSelectedUser] = React.useState<User>(DEMO_USERS[0]);
  const [copiedMessageId, setCopiedMessageId] = React.useState<string | null>(
    null
  );
  const [pendingAssistantId, setPendingAssistantId] = React.useState<
    string | null
  >(null);

  const handleCopy = async (content: string, messageId: string) => {
    try {
      await navigator.clipboard.writeText(content);
      setCopiedMessageId(messageId);
      setTimeout(() => setCopiedMessageId(null), 2000);
    } catch (err) {
      console.error("Failed to copy text: ", err);
    }
  };

  const sendMessage = async () => {
    if (!input.trim() || isLoading) return;

    const userMessage: Message = {
      id: Date.now().toString(),
      role: "user",
      content: input.trim(),
      timestamp: new Date(),
    };

    setMessages((prev) => [userMessage, ...prev]); // Add to top
    setInput("");
    setIsLoading(true);
    setPendingAssistantId(null);

    try {
      const response = await fetch(
        `http://localhost:8000/chat?email=${encodeURIComponent(
          selectedUser.email
        )}`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            message: userMessage.content,
          }),
        }
      );

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const reader = response.body?.getReader();
      if (!reader) throw new Error("No response body");

      const decoder = new TextDecoder();
      let buffer = "";
      let assistantId: string | null = null;

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop() || "";

        for (const line of lines) {
          if (line.startsWith("data: ")) {
            try {
              const data = JSON.parse(line.slice(6));
              if (data.error) {
                throw new Error(data.error);
              }
              if (data.content) {
                // On first chunk, create the assistant message
                if (!assistantId) {
                  assistantId = (Date.now() + 1).toString();
                  setPendingAssistantId(assistantId);
                  setMessages((prev) => [
                    {
                      id: assistantId,
                      role: "assistant",
                      content: data.content,
                      timestamp: new Date(),
                    },
                    ...prev,
                  ]);
                } else {
                  setMessages((prev) =>
                    prev.map((msg) =>
                      msg.id === assistantId
                        ? { ...msg, content: data.content }
                        : msg
                    )
                  );
                }
              }
              if (data.done) {
                setPendingAssistantId(null);
                break;
              }
            } catch {
              if (line.trim() && line !== "data: ") {
                console.warn("Skipped malformed SSE line:", line);
              }
            }
          }
        }
      }
    } catch (error) {
      setPendingAssistantId(null);
      console.error("Error sending message:", error);
      setMessages((prev) => [
        {
          id: (Date.now() + 1).toString(),
          role: "assistant",
          content: `Sorry, there was an error processing your request: ${
            error instanceof Error ? error.message : "Unknown error"
          }`,
          timestamp: new Date(),
        },
        ...prev,
      ]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  };

  // User dropdown using shadcn/ui
  const UserDropdown = (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button
          variant="outline"
          className="flex items-center gap-2 w-full px-2 py-1.5 border border-muted-foreground/30 rounded-lg bg-background hover:bg-muted transition-colors"
        >
          <Avatar className="h-8 w-8">
            <AvatarFallback>
              {selectedUser.name
                .split(" ")
                .map((n: string) => n[0])
                .join("")}
            </AvatarFallback>
          </Avatar>
          <span className="truncate flex-1 text-left text-base font-medium">
            {selectedUser.name}
          </span>
          <ChevronDown className="w-4 h-4 text-muted-foreground ml-1" />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="w-64">
        {DEMO_USERS.map((user: User) => (
          <DropdownMenuItem
            key={user.email}
            onClick={() => {
              setSelectedUser(user);
              setMessages([]); // Clear chat history
              setPendingAssistantId(null); // Reset pending assistant
            }}
            className={cn(
              "flex items-center gap-2 cursor-pointer px-2 py-1.5",
              selectedUser.email === user.email && "bg-muted"
            )}
          >
            <Avatar className="h-8 w-8">
              <AvatarFallback>
                {user.name
                  .split(" ")
                  .map((n: string) => n[0])
                  .join("")}
              </AvatarFallback>
            </Avatar>
            <div className="flex flex-col">
              <span className="font-medium text-sm">{user.name}</span>
              <span className="text-xs text-muted-foreground">{user.role}</span>
              <span className="text-xs text-foreground">
                ID: {user.employeeId}
              </span>
            </div>
          </DropdownMenuItem>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  );

  return (
    <div className="relative min-h-screen bg-muted flex flex-col items-stretch">
      {/* Header */}
      <div className="w-full sticky top-0 z-10 border-b border-muted-foreground/10 px-4 py-3 flex items-center justify-between bg-background/95 backdrop-blur">
        <div>
          <span className="text-2xl font-bold mb-1 text-foreground">
            PeoplePal HR Bot
          </span>
          <p className="text-base text-muted-foreground">
            Your friendly HR assistant — ask me anything about HR, benefits, or
            company policies!
          </p>
        </div>
        <div className="w-64">{UserDropdown}</div>
      </div>
      {/* Chat area */}
      <main className="w-full flex-1 flex flex-col items-stretch max-w-6xl mx-auto px-4 pb-32 pt-12">
        <div
          className="flex-1 flex flex-col-reverse gap-4 overflow-y-auto pt-12"
          style={{
            minHeight: "60vh",
            maxHeight: "calc(100vh - 180px)",
            paddingBottom: 0,
          }}
        >
          {messages.length === 0 ? (
            <div className="flex flex-col items-start justify-center h-[60vh]">
              {/* Example prompt buttons could go here */}
            </div>
          ) : (
            <>
              {isLoading && pendingAssistantId === null && (
                <div className="flex gap-2 items-end justify-start">
                  <Avatar className="h-8 w-8">
                    <AvatarFallback>AI</AvatarFallback>
                  </Avatar>
                  <Card className="px-5 py-3 bg-background border border-muted-foreground/20 rounded-xl text-foreground max-w-[75%]">
                    <div className="flex items-center gap-2">
                      <Skeleton className="w-4 h-4 rounded-full bg-muted animate-pulse" />
                      <Skeleton className="w-16 h-4 rounded bg-muted animate-pulse" />
                    </div>
                    <div className="text-xs mt-2 opacity-70 text-muted-foreground">
                      The assistant is preparing your answer...
                    </div>
                  </Card>
                </div>
              )}
              {messages.map((message) => (
                <div
                  key={message.id}
                  className={cn(
                    "flex gap-2 items-end",
                    message.role === "user" ? "justify-end" : "justify-start"
                  )}
                >
                  {message.role === "assistant" && (
                    <Avatar className="h-8 w-8">
                      <AvatarFallback>AI</AvatarFallback>
                    </Avatar>
                  )}
                  <Card
                    className={cn(
                      "px-5 py-3 relative group max-w-[75%] border border-muted-foreground/20 rounded-xl",
                      message.role === "user"
                        ? "bg-primary text-primary-foreground ml-auto"
                        : "bg-background text-foreground"
                    )}
                  >
                    <div className="prose prose-base max-w-none font-normal [&_p]:mb-4">
                      <ReactMarkdown>{message.content}</ReactMarkdown>
                    </div>
                    {message.role === "assistant" && (
                      <button
                        onClick={() => handleCopy(message.content, message.id)}
                        className="absolute top-2 right-2 opacity-0 group-hover:opacity-100 transition-opacity p-1 hover:bg-muted rounded"
                        title="Copy response"
                      >
                        {copiedMessageId === message.id ? (
                          <CheckCircle className="w-4 h-4 text-green-600" />
                        ) : (
                          <Copy className="w-4 h-4 text-muted-foreground" />
                        )}
                      </button>
                    )}
                    <div
                      className={cn(
                        "text-xs mt-2 flex justify-end",
                        message.role === "user"
                          ? "text-white font-semibold drop-shadow-sm"
                          : "text-muted-foreground"
                      )}
                    >
                      {message.timestamp.toLocaleTimeString()}
                    </div>
                  </Card>
                  {message.role === "user" && (
                    <Avatar className="h-8 w-8">
                      <AvatarFallback>
                        {selectedUser.name
                          .split(" ")
                          .map((n: string) => n[0])
                          .join("")}
                      </AvatarFallback>
                    </Avatar>
                  )}
                </div>
              ))}
            </>
          )}
        </div>
      </main>
      {/* Fixed input at the bottom */}
      <div className="fixed bottom-0 left-0 w-full bg-background z-20 border-t border-muted-foreground/10">
        <div className="w-full max-w-4xl mx-auto px-4 py-4">
          <div className="border-2 border-foreground rounded-2xl flex items-center px-3 py-2 gap-2 bg-white">
            <Input
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyPress={handleKeyPress}
              placeholder="Ask me anything..."
              disabled={isLoading}
              className="flex-1 bg-white border-none text-lg px-3 py-2 rounded-xl transition-all"
            />
            <Button
              onClick={sendMessage}
              disabled={!input.trim() || isLoading}
              size="icon"
              className="rounded-full bg-primary text-primary-foreground hover:bg-primary/90 flex-shrink-0 w-10 h-10"
            >
              <Send className="w-5 h-5" />
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
}
