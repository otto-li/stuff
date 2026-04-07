import type { DataUIPart, LanguageModelUsage, UIMessageChunk } from 'ai';
import { useChat } from '@ai-sdk/react';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useSWRConfig } from 'swr';
import { ChatHeader } from '@/components/chat-header';
import { fetchWithErrorHandlers, generateUUID } from '@/lib/utils';
import { MultimodalInput } from './multimodal-input';
import { Messages } from './messages';
import type {
  Attachment,
  ChatMessage,
  CustomUIDataTypes,
  FeedbackMap,
  VisibilityType,
} from '@chat-template/core';
import { unstable_serialize } from 'swr/infinite';
import { getChatHistoryPaginationKey } from './sidebar-history';
import { toast } from './toast';
import { useSearchParams } from 'react-router-dom';
import { useChatVisibility } from '@/hooks/use-chat-visibility';
import { ChatSDKError } from '@chat-template/core/errors';
import { useDataStream } from './data-stream-provider';
import { isCredentialErrorMessage } from '@/lib/oauth-error-utils';
import { ChatTransport } from '../lib/ChatTransport';
import type { ClientSession } from '@chat-template/auth';
import { softNavigateToChatId } from '@/lib/navigation';
import { useAppConfig } from '@/contexts/AppConfigContext';
import { DebugPanel } from './debug-panel';

export function Chat({
  id,
  initialMessages,
  initialChatModel,
  initialVisibilityType,
  isReadonly,
  initialLastContext,
  feedback = {},
}: {
  id: string;
  initialMessages: ChatMessage[];
  initialChatModel: string;
  initialVisibilityType: VisibilityType;
  isReadonly: boolean;
  session: ClientSession;
  initialLastContext?: LanguageModelUsage;
  feedback?: FeedbackMap;
}) {
  const { visibilityType } = useChatVisibility({
    chatId: id,
    initialVisibilityType,
  });

  const { mutate } = useSWRConfig();
  const { setDataStream } = useDataStream();
  const { chatHistoryEnabled } = useAppConfig();

  const [input, setInput] = useState<string>('');
  const [_usage, setUsage] = useState<LanguageModelUsage | undefined>(
    initialLastContext,
  );

  // Debug panel state
  const [useFlatMode, setUseFlatMode] = useState(false);
  const [requestPayload, setRequestPayload] = useState<unknown>(null);
  const [llmPayload, setLlmPayload] = useState<unknown>(null);

  // Flat mode streaming state
  const [isFlatStreaming, setIsFlatStreaming] = useState(false);
  const flatAbortRef = useRef<AbortController | null>(null);

  const [lastPart, setLastPart] = useState<UIMessageChunk | undefined>();
  const lastPartRef = useRef<UIMessageChunk | undefined>(lastPart);
  lastPartRef.current = lastPart;

  // Single counter for resume attempts - reset when stream parts are received
  const resumeAttemptCountRef = useRef(0);
  const maxResumeAttempts = 3;

  const abortController = useRef<AbortController | null>(new AbortController());
  useEffect(() => {
    return () => {
      abortController.current?.abort('ABORT_SIGNAL');
    };
  }, []);

  const fetchWithAbort = useMemo(() => {
    return async (input: RequestInfo | URL, init?: RequestInit) => {
      // useChat does not cancel /stream requests when the component is unmounted
      const signal = abortController.current?.signal;
      return fetchWithErrorHandlers(input, { ...init, signal });
    };
  }, []);

  const stop = useCallback(() => {
    abortController.current?.abort('USER_ABORT_SIGNAL');
  }, []);

  const stopFlat = useCallback(() => {
    flatAbortRef.current?.abort();
    setIsFlatStreaming(false);
  }, []);

  const isNewChat = initialMessages.length === 0;
  const didFetchHistoryOnNewChat = useRef(false);
  const fetchChatHistory = useCallback(() => {
    mutate(unstable_serialize(getChatHistoryPaginationKey));
  }, [mutate]);

  const {
    messages,
    setMessages,
    sendMessage,
    status,
    resumeStream,
    clearError,
    addToolApprovalResponse,
    regenerate,
  } = useChat<ChatMessage>({
    id,
    messages: initialMessages,
    experimental_throttle: 100,
    generateId: generateUUID,
    resume: id !== undefined && initialMessages.length > 0, // Enable automatic stream resumption
    transport: new ChatTransport({
      onStreamPart: (part) => {
        // As soon as we recive a stream part, we fetch the chat history again for new chats
        if (isNewChat && !didFetchHistoryOnNewChat.current) {
          fetchChatHistory();
          didFetchHistoryOnNewChat.current = true;
        }
        // Reset resume attempts when we successfully receive stream parts
        resumeAttemptCountRef.current = 0;
        setLastPart(part);
      },
      api: '/api/chat',
      fetch: fetchWithAbort,
      prepareSendMessagesRequest({ messages, id, body }) {
        const lastMessage = messages.at(-1);
        const isUserMessage = lastMessage?.role === 'user';

        // For continuations (non-user messages like tool results), we must always
        // send previousMessages because the tool result only exists client-side
        // and hasn't been saved to the database yet.
        const needsPreviousMessages = !chatHistoryEnabled || !isUserMessage;

        const requestBodyToSend = {
          id,
          // Only include message field for user messages (new messages)
          // For continuation (assistant messages with tool results), omit message field
          ...(isUserMessage ? { message: lastMessage } : {}),
          selectedChatModel: initialChatModel,
          selectedVisibilityType: visibilityType,
          nextMessageId: generateUUID(),
          // Send previous messages when:
          // 1. Database is disabled (ephemeral mode) - always need client-side messages
          // 2. Continuation request (tool results) - tool result only exists client-side
          ...(needsPreviousMessages
            ? {
                previousMessages: isUserMessage
                  ? messages.slice(0, -1)
                  : messages,
              }
            : {}),
          ...body,
        };

        // Capture for debug panel
        setRequestPayload(requestBodyToSend);

        return { body: requestBodyToSend };
      },
      prepareReconnectToStreamRequest({ id }) {
        return {
          api: `/api/chat/${id}/stream`,
          credentials: 'include',
        };
      },
    }),
    onData: (dataPart) => {
      setDataStream((ds) =>
        ds ? [...ds, dataPart as DataUIPart<CustomUIDataTypes>] : [],
      );
      if (dataPart.type === 'data-usage') {
        setUsage(dataPart.data as LanguageModelUsage);
      }
      if (dataPart.type === 'data-debugPayload') {
        setLlmPayload(dataPart.data);
      }
    },
    onFinish: ({
      isAbort,
      isDisconnect,
      isError,
      messages: finishedMessages,
    }) => {
      // Reset state for next message
      didFetchHistoryOnNewChat.current = false;

      // If user aborted, don't try to resume
      if (isAbort) {
        console.log('[Chat onFinish] Stream was aborted by user, not resuming');
        fetchChatHistory();
        return;
      }

      // Check if the last message contains an OAuth credential error
      // If so, don't try to resume - the user needs to authenticate first
      const lastMessage = finishedMessages?.at(-1);
      const hasOAuthError = lastMessage?.parts?.some(
        (part) =>
          part.type === 'data-error' &&
          typeof part.data === 'string' &&
          isCredentialErrorMessage(part.data),
      );

      if (hasOAuthError) {
        console.log(
          '[Chat onFinish] OAuth credential error detected, not resuming',
        );
        fetchChatHistory();
        clearError();
        return;
      }

      // Determine if we should attempt to resume:
      // 1. Stream didn't end with a 'finish' part (incomplete)
      // 2. It was a disconnect/error that terminated the stream
      // 3. We haven't exceeded max resume attempts
      const streamIncomplete = lastPartRef.current?.type !== 'finish';
      const shouldResume =
        streamIncomplete &&
        (isDisconnect || isError || lastPartRef.current === undefined);

      if (shouldResume && resumeAttemptCountRef.current < maxResumeAttempts) {
        console.log(
          '[Chat onFinish] Resuming stream. Attempt:',
          resumeAttemptCountRef.current + 1,
        );
        resumeAttemptCountRef.current++;
        // Ref: https://github.com/vercel/ai/issues/8477#issuecomment-3603209884
        queueMicrotask(() => {
          resumeStream();
        });
      } else {
        // Stream completed normally or we've exhausted resume attempts
        if (resumeAttemptCountRef.current >= maxResumeAttempts) {
          console.warn('[Chat onFinish] Max resume attempts reached');
        }
        fetchChatHistory();
      }
    },
    onError: (error) => {
      console.log('[Chat onError] Error occurred:', error);

      // Only show toast for explicit ChatSDKError (backend validation errors)
      // Other errors (network, schema validation) are handled silently or in message parts
      if (error instanceof ChatSDKError) {
        toast({
          type: 'error',
          description: error.message,
        });
      } else {
        // Non-ChatSDKError: Could be network error or in-stream error
        // Log but don't toast - errors during streaming may be informational
        console.warn('[Chat onError] Error during streaming:', error.message);
      }
      // Note: We don't call resumeStream here because onError can be called
      // while the stream is still active (e.g., for data-error parts).
      // Resume logic is handled exclusively in onFinish.
    },
  });

  // Custom send for both modes — always uses plain-JSON fetch (not Vercel SDK)
  const flatSendMessage = useCallback(
    async (userMsg: Parameters<typeof sendMessage>[0]) => {
      if (!userMsg?.parts) return;
      const text = userMsg.parts
        .filter(
          (p): p is { type: 'text'; text: string } => p.type === 'text',
        )
        .map((p) => p.text)
        .join('');

      const toText = (m: ChatMessage) =>
        m.parts
          .filter((p): p is { type: 'text'; text: string } => p.type === 'text')
          .map((p) => p.text)
          .join('');

      // Build payload based on current toggle mode
      const payload = useFlatMode
        ? // Flat text: only the current message, no history
          { message: text }
        : // Array messages: full history + new user message
          {
            messages: [
              ...messages.map((m) => ({ role: m.role, content: toText(m) })),
              { role: 'user', content: text },
            ],
          };

      setRequestPayload(payload);

      // Add user message to chat
      const userId = generateUUID();
      const assistantId = generateUUID();

      setMessages([
        ...messages,
        {
          id: userId,
          role: 'user' as const,
          parts: [{ type: 'text' as const, text }],
          metadata: { createdAt: new Date().toISOString() },
        } as ChatMessage,
      ]);

      setIsFlatStreaming(true);
      flatAbortRef.current = new AbortController();

      // Initialize assistant message
      setMessages((prev) => [
        ...prev,
        {
          id: assistantId,
          role: 'assistant' as const,
          parts: [{ type: 'text' as const, text: '' }],
          metadata: { createdAt: new Date().toISOString() },
        } as ChatMessage,
      ]);

      try {
        const res = await fetch('/api/chat', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
          signal: flatAbortRef.current.signal,
        });

        if (!res.body) {
          setIsFlatStreaming(false);
          return;
        }

        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';
        let assistantText = '';

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;

          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split('\n');
          buffer = lines.pop() ?? '';

          for (const line of lines) {
            if (!line.startsWith('data: ')) continue;
            try {
              const event = JSON.parse(line.slice(6));
              if (event.type === 'text') {
                assistantText += event.delta;
                const captured = assistantText;
                setMessages((prev) =>
                  prev.map((m) =>
                    m.id === assistantId
                      ? {
                          ...m,
                          parts: [{ type: 'text' as const, text: captured }],
                        }
                      : m,
                  ),
                );
              } else if (event.type === 'debug') {
                setLlmPayload(event.payload);
              }
            } catch {
              // ignore parse errors on individual SSE events
            }
          }
        }
      } catch (err) {
        if ((err as Error).name !== 'AbortError') {
          console.error('[Chat flat] Fetch error:', err);
        }
      } finally {
        setIsFlatStreaming(false);
      }
    },
    [messages, setMessages, useFlatMode],
  );

  // Both modes always use the plain-JSON fetch path
  const effectiveStatus: typeof status = isFlatStreaming ? 'streaming' : 'ready';
  const effectiveStop = stopFlat;
  const effectiveSendMessage = flatSendMessage as typeof sendMessage;

  const [searchParams] = useSearchParams();
  const query = searchParams.get('query');

  const [hasAppendedQuery, setHasAppendedQuery] = useState(false);

  useEffect(() => {
    if (query && !hasAppendedQuery) {
      sendMessage({
        role: 'user' as const,
        parts: [{ type: 'text', text: query }],
      });

      setHasAppendedQuery(true);
      softNavigateToChatId(id, chatHistoryEnabled);
    }
  }, [query, sendMessage, hasAppendedQuery, id, chatHistoryEnabled]);

  const [attachments, setAttachments] = useState<Array<Attachment>>([]);

  return (
    <>
      <div className="overscroll-behavior-contain flex h-dvh min-w-0 touch-pan-y flex-col bg-background">
        <ChatHeader />

        {/* Format toggle */}
        <div className="flex items-center justify-end gap-2 border-b border-border/40 bg-background px-3 py-1">
          <span className="text-xs text-muted-foreground">
            {useFlatMode ? 'flat text' : 'array messages'}
          </span>
          <button
            type="button"
            onClick={() => setUseFlatMode((v) => !v)}
            className={`relative inline-flex h-5 w-9 shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring ${
              useFlatMode ? 'bg-blue-500' : 'bg-muted'
            }`}
            title="Toggle between array messages format and flat text format"
          >
            <span
              className={`pointer-events-none inline-block h-4 w-4 rounded-full bg-white shadow-lg ring-0 transition-transform ${
                useFlatMode ? 'translate-x-4' : 'translate-x-0'
              }`}
            />
          </button>
          <span className="text-xs font-medium text-muted-foreground">
            Array ↔ Flat Text
          </span>
        </div>

        <Messages
          status={effectiveStatus}
          messages={messages}
          setMessages={setMessages}
          addToolApprovalResponse={addToolApprovalResponse}
          regenerate={regenerate}
          sendMessage={sendMessage}
          isReadonly={isReadonly}
          selectedModelId={initialChatModel}
          feedback={feedback}
        />

        <div className="sticky bottom-0 z-1 mx-auto flex w-full max-w-4xl gap-2 border-t-0 bg-background px-2 pb-3 md:px-4 md:pb-4">
          {!isReadonly && (
            <MultimodalInput
              chatId={id}
              input={input}
              setInput={setInput}
              status={effectiveStatus}
              stop={effectiveStop}
              attachments={attachments}
              setAttachments={setAttachments}
              messages={messages}
              setMessages={setMessages}
              sendMessage={effectiveSendMessage}
              selectedVisibilityType={visibilityType}
            />
          )}
        </div>
      </div>

      <DebugPanel requestPayload={requestPayload} llmPayload={llmPayload} />
    </>
  );
}
