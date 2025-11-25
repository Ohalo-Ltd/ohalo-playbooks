/**
 * Chat input component.
 */

import React, { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Textarea } from '@/components/ui/textarea';
import { SendIcon } from 'lucide-react';
import { cn } from "@/lib/utils";

export interface ChatInputProps {
  onSend: (message: string) => void;
  disabled?: boolean;
  placeholder?: string;
  centered?: boolean;
}

export function ChatInput({
  onSend,
  disabled,
  placeholder = "Ask a question...",
  centered = false,
}: ChatInputProps) {
  const [message, setMessage] = useState("");

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (message.trim() && !disabled) {
      onSend(message.trim());
      setMessage("");
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSubmit(e);
    }
  };

  return (
    <form
      onSubmit={handleSubmit}
      className={cn("relative", centered && "w-full")}
    >
      <div
        className={cn(
          "flex items-center gap-2 rounded-3xl border bg-background p-2 shadow-lg",
          centered && "w-full"
        )}
      >
        <Textarea
          value={message}
          onChange={(e) => setMessage(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder={placeholder}
          disabled={disabled}
          className={cn(
            "flex-1 resize-none border-0 bg-transparent px-4 py-3 text-sm focus-visible:ring-0 focus-visible:ring-offset-0",
            centered ? "min-h-[56px]" : "min-h-[60px]"
          )}
          rows={1}
          style={{ maxHeight: "200px" }}
        />
        <Button
          type="submit"
          disabled={disabled || !message.trim()}
          size="icon"
          className={cn(
            "rounded-full shrink-0",
            centered ? "h-10 w-10" : "h-12 w-12"
          )}
        >
          <SendIcon className="h-5 w-5" />
        </Button>
      </div>
    </form>
  );
}
