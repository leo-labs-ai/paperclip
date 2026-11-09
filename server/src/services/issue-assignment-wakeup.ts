import { agentWakeupRequests, agents, type Db } from "@paperclipai/db";
import { eq } from "drizzle-orm";
import { logger } from "../middleware/logger.js";
import type { DurableChatWakeupRequest } from "./durable-chat-wakeup.js";

type WakeupTriggerDetail = "manual" | "ping" | "callback" | "system";
type WakeupSource = "timer" | "assignment" | "on_demand" | "automation";

export interface IssueAssignmentWakeupDeps {
  wakeup: (
    agentId: string,
    opts: {
      source?: WakeupSource;
      triggerDetail?: WakeupTriggerDetail;
      reason?: string | null;
      payload?: Record<string, unknown> | null;
      idempotencyKey?: string | null;
      allowRunCoalescing?: boolean;
      requestedByActorType?: "user" | "agent" | "system";
      requestedByActorId?: string | null;
      contextSnapshot?: Record<string, unknown>;
      durableChatRequest?: DurableChatWakeupRequest;
    },
  ) => Promise<unknown>;
}

export function queueIssueAssignmentWakeup(input: {
  db: Db;
  heartbeat: IssueAssignmentWakeupDeps;
  issue: { id: string; assigneeAgentId: string | null; status: string };
  reason: string;
  mutation: string;
  contextSource: string;
  requestedByActorType?: "user" | "agent" | "system";
  requestedByActorId?: string | null;
  taskKey?: string | null;
  /** Latest issue comment that caused this wakeup. Included in both payload
   * and context so the heartbeat can build the exact turn that was requested. */
  wakeCommentId?: string | null;
  /** Closed, server-derived omission counts for provider attachments on the
   * exact wake comment. These are prompt diagnostics, never authorization. */
  attachmentOmissionReasons?: Record<string, number> | null;
  /** External chat conversation the wake answers. The wake queue keeps each
   * conversation in its own batch, because a reviewed chat run attests one. */
  chatConversationId?: string | null;
  rethrowOnError?: boolean;
  durableChatRequest?: DurableChatWakeupRequest;
}) {
  if (!input.issue.assigneeAgentId || input.issue.status === "backlog") return;

  return input.heartbeat
    .wakeup(input.issue.assigneeAgentId, {
      source: "assignment",
      triggerDetail: "system",
      reason: input.reason,
      payload: {
        issueId: input.issue.id,
        mutation: input.mutation,
        ...(input.taskKey ? { taskKey: input.taskKey } : {}),
        ...(input.wakeCommentId ? { wakeCommentId: input.wakeCommentId } : {}),
      },
      requestedByActorType: input.requestedByActorType,
      requestedByActorId: input.requestedByActorId ?? null,
      ...(input.durableChatRequest
        ? { durableChatRequest: input.durableChatRequest }
        : {}),
      contextSnapshot: {
        issueId: input.issue.id,
        source: input.contextSource,
        ...(input.taskKey ? { taskKey: input.taskKey } : {}),
        ...(input.wakeCommentId ? { wakeCommentId: input.wakeCommentId } : {}),
        ...(input.chatConversationId ? { chatConversationId: input.chatConversationId } : {}),
        ...(input.wakeCommentId && input.attachmentOmissionReasons
          ? {
              externalAttachmentOmissions: [
                {
                  commentId: input.wakeCommentId,
                  reasons: input.attachmentOmissionReasons,
                },
              ],
            }
          : {}),
      },
    })
    .catch(async (err) => {
      // Fork (#102): durably record wakeup failures so the board can see
      // them. Durable chat wakes are the exception: upstream's chat outbox
      // owns their receipt/retry accounting, and a synthetic failed row here
      // would double-count against it.
      if (input.durableChatRequest) {
        logger.warn({ err, issueId: input.issue.id }, "failed to wake assignee on issue assignment");
        if (input.rethrowOnError) throw err;
        return null;
      }
      try {
        const agent = await input.db
          .select({ companyId: agents.companyId })
          .from(agents)
          .where(eq(agents.id, input.issue.assigneeAgentId!))
          .then((rows) => rows[0] ?? null);
        if (agent) {
          await input.db.insert(agentWakeupRequests).values({
            companyId: agent.companyId,
            agentId: input.issue.assigneeAgentId!,
            source: "assignment",
            triggerDetail: "system",
            reason: "issue_assignment_wakeup_failed",
            payload: { issueId: input.issue.id, mutation: input.mutation },
            status: "failed",
            requestedByActorType: input.requestedByActorType ?? null,
            requestedByActorId: input.requestedByActorId ?? null,
            finishedAt: new Date(),
            error: err instanceof Error ? err.message : String(err),
          });
        }
      } catch (writeErr) {
        // This is a fire-and-forget best-effort durability write; a failure
        // here (e.g. agent deleted mid-flight, pool exhausted) must not
        // reject this promise and crash callers that use `void`.
        logger.warn({ writeErr, issueId: input.issue.id }, "failed to durably record issue assignment wakeup failure");
      }
      logger.warn({ err, issueId: input.issue.id }, "failed to wake assignee on issue assignment");
      if (input.rethrowOnError) throw err;
      return null;
    });
}
