/**
 * Main page - chat interface.
 */

import { ChatInterface } from "@/components/chat-interface";

export default function Home() {
  return (
    <main className="container mx-auto p-4 md:p-8">
      <ChatInterface />
    </main>
  );
}
