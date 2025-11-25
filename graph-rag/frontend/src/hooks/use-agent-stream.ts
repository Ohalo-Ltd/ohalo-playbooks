import { useCallback, useRef, useState } from 'react';

export interface AgentStep {
  type: 'thinking' | 'tool_call_start' | 'tool_call_result' | 'answer' | 'error';
  content?: string;
  tool?: string;
  args?: Record<string, any>;
  result?: any;
  message?: string;
}

interface UseAgentStreamOptions {
  onStep?: (step: AgentStep) => void;
  onComplete?: () => void;
  onError?: (error: string) => void;
}

export function useAgentStream(options: UseAgentStreamOptions = {}) {
  const [steps, setSteps] = useState<AgentStep[]>([]);
  const [isStreaming, setIsStreaming] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const eventSourceRef = useRef<EventSource | null>(null);

  // Use refs to avoid stale closures
  const optionsRef = useRef(options);
  optionsRef.current = options;

  const startStream = useCallback(
    async (question: string, projectId: string = 'default', currentUserEmail?: string) => {
      // Reset state
      setSteps([]);
      setError(null);
      setIsStreaming(true);

      // Close any existing connection
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
      }

      try {
        // Make POST request to get stream
        const response = await fetch('http://localhost:8000/api/chat/query/stream', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            question,
            project_id: projectId,
            top_k: 5,
            include_graph_context: true,
            current_user_email: currentUserEmail,
          }),
        });

        if (!response.ok) {
          throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        const reader = response.body?.getReader();
        if (!reader) {
          throw new Error('No response body');
        }

        const decoder = new TextDecoder();
        let buffer = '';
        let completed = false;

        // Read stream
        while (true) {
          const { done, value } = await reader.read();

          if (done) {
            console.log('[useAgentStream] Stream done, completed flag:', completed);
            setIsStreaming(false);
            if (!completed) {
              console.log('[useAgentStream] Calling onComplete from done');
              optionsRef.current.onComplete?.();
            }
            break;
          }

          buffer += decoder.decode(value, { stream: true });

          // Process complete SSE messages
          const lines = buffer.split('\n');
          buffer = lines.pop() || ''; // Keep incomplete line in buffer

          for (const line of lines) {
            if (line.startsWith('data: ')) {
              const data = line.slice(6);

              if (data === '[DONE]') {
                console.log('[useAgentStream] Received [DONE]');
                setIsStreaming(false);
                completed = true;
                console.log('[useAgentStream] Calling onComplete from [DONE]');
                optionsRef.current.onComplete?.();
                break;
              }

              try {
                const step: AgentStep = JSON.parse(data);
                console.log('[useAgentStream] Received step:', step.type);

                // Add step to state
                setSteps((prev) => [...prev, step]);
                optionsRef.current.onStep?.(step);

                // Handle errors
                if (step.type === 'error') {
                  setError(step.message || 'Unknown error');
                  optionsRef.current.onError?.(step.message || 'Unknown error');
                }
              } catch (e) {
                console.error('Failed to parse SSE data:', data, e);
              }
            }
          }
        }
      } catch (err) {
        const errorMessage = err instanceof Error ? err.message : 'Unknown error';
        setError(errorMessage);
        setIsStreaming(false);
        optionsRef.current.onError?.(errorMessage);
      }
    },
    [] // Remove options dependency
  );

  const stopStream = useCallback(() => {
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }
    setIsStreaming(false);
  }, []);

  return {
    steps,
    isStreaming,
    error,
    startStream,
    stopStream,
  };
}
