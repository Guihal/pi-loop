/**
 * Core scheduler engine.
 *
 * 1-second tick loop that checks all tasks, gates on agent idle state,
 * fires prompts via pi.sendUserMessage(), and handles auto-expiry.
 */

import type { ExtensionAPI, ExtensionContext } from "@mariozechner/pi-coding-agent";
import { nextCronRunMs, cronGapMs } from "./cron.js";
import { recurringJitterMs, oneShotJitterMs } from "./jitter.js";
import {
  getAllTasks,
  getTask,
  updateTask,
  removeTask,
  writeDurableTasks,
} from "./store.js";
import type { LoopConfig, LoopTask } from "./types.js";

const DEBUG = !!process.env.PI_LOOP_DEBUG;
function debug(...args: any[]): void {
  if (!DEBUG) return;
  console.debug("[pi-loop:scheduler]", ...args);
}

export class LoopScheduler {
  private interval: ReturnType<typeof setInterval> | null = null;
  private isAgentBusy = false;
  private pendingFires: string[] = [];
  private pi: ExtensionAPI;
  private config: LoopConfig;
  private cwd: string;
  private ctx: ExtensionContext | null = null;
  private currentSessionId: string | null = null;
  private disarmed = false;

  constructor(pi: ExtensionAPI, config: LoopConfig, cwd: string) {
    this.pi = pi;
    this.config = config;
    this.cwd = cwd;
  }

  setContext(ctx: ExtensionContext): void {
    this.ctx = ctx;
    // Three-tier session-id capture. The pi AgentSession exposes
    // ctx.sessionId (getter on agent-session.js:588) and
    // ctx.sessionManager.getSessionId() / .sessionId. Try the most specific
    // first, fall back gracefully. Do NOT clobber a previously-captured good
    // id with a bad (null) one — a partial-init ctx would otherwise lock us
    // into currentSessionId=null and silently filter every stamped task.
    const captured = this.captureSessionId(ctx);
    if (captured != null) {
      this.currentSessionId = captured;
    }
    // Always re-arm on a fresh context — a rebuilt scheduler from
    // session_start is considered live.
    this.disarmed = false;
  }

  /**
   * Read the session id from the context, tolerating partial initialization.
   * Returns null if no id is available.
   */
  private captureSessionId(ctx: unknown): string | null {
    if (!ctx) return null;
    const c = ctx as any;
    // Tier 1: sessionManager.getSessionId() (most specific, used by cron_tools)
    try {
      const id = c?.sessionManager?.getSessionId?.();
      if (typeof id === "string" && id) return id;
    } catch {
      // fall through
    }
    // Tier 2: ctx.sessionId (AgentSession getter, agent-session.js:588)
    try {
      const id = c?.sessionId;
      if (typeof id === "string" && id) return id;
    } catch {
      // fall through
    }
    // Tier 3: sessionManager.sessionId (the field, not the method)
    try {
      const id = c?.sessionManager?.sessionId;
      if (typeof id === "string" && id) return id;
    } catch {
      // fall through
    }
    return null;
  }

  start(): void {
    if (this.interval || this.disarmed) return;
    this.interval = setInterval(() => this.check(), this.config.checkIntervalMs);
  }

  stop(): void {
    if (this.interval) {
      clearInterval(this.interval);
      this.interval = null;
    }
  }

  /**
   * Self-disarm: stop the interval and null ctx. Called on stale-ctx or
   * downstream teardown throws so a mid-session reload (e.g. hier-rules
   * ctx.reload()) does not produce uncaughtException and crash pi.
   */
  disarm(reason: string): void {
    debug("disarm:", reason);
    this.disarmed = true;
    this.ctx = null;
    this.stop();
  }

  setBusy(): void {
    this.isAgentBusy = true;
  }

  setIdle(): void {
    this.isAgentBusy = false;
    this.drainPendingFires();
  }

  private check(): void {
    if (this.disarmed || !this.ctx) return;
    const now = Date.now();

    for (const task of getAllTasks()) {
      if (this.shouldFire(task, now)) {
        if (this.isAgentBusy) {
          // Queue for later, but only if not already queued
          if (!this.pendingFires.includes(task.id)) {
            this.pendingFires.push(task.id);
          }
        } else {
          this.fire(task);
        }
      }
    }
  }

  private shouldFire(task: LoopTask, now: number): boolean {
    // Handle ScheduleWakeup tasks — stored with _wakeup_ prefixed cron
    if (task.cron.startsWith("_wakeup_")) {
      return task.nextFireTime != null && now >= task.nextFireTime;
    }

    // Compute the base next fire time
    const anchor = task.lastFiredAt ?? task.createdAt;
    const baseNext = nextCronRunMs(task.cron, anchor);
    if (baseNext === null) return false;

    let fireTime: number;

    if (task.recurring) {
      // Forward jitter for recurring tasks
      const gap = cronGapMs(task.cron, anchor);
      const jitter = gap ? recurringJitterMs(task, gap, this.config) : 0;
      fireTime = baseNext + jitter;
    } else {
      // Backward jitter for one-shots
      const jitter = oneShotJitterMs(task, baseNext, this.config);
      fireTime = baseNext - jitter;
    }

    return now >= fireTime;
  }

  /**
   * Returns true if the task should fire under the current session.
   *
   * Rules:
   * - Durable tasks are intentionally cross-session: always fire.
   * - Tasks without a sessionId stamp are legacy/cross-session-allowed: fire.
   * - If currentSessionId is null (session not yet bound) AND the task has
   *   no own stamp → fire (legacy pass-through).
   * - If currentSessionId is null but the task has its own sessionId stamp →
   *   fire anyway (legacy pass-through, fail-open). This avoids the
   *   "task disappears from cron_list" symptom when setContext runs before
   *   the SessionManager is fully wired.
   * - Otherwise, fire only if the task's sessionId matches the scheduler's
   *   currentSessionId.
   */
  belongsToCurrentSession(task: LoopTask): boolean {
    if (task.durable) return true;
    if (!task.sessionId) return true;
    if (!this.currentSessionId) return true; // fail-open: see method doc
    return task.sessionId === this.currentSessionId;
  }

  private fire(task: LoopTask): void {
    if (this.disarmed || !this.ctx) return;
    debug("fire:", task.id, task.prompt.slice(0, 40));

    // Foreign session-only task: skip silently, do not update state, do not notify.
    if (!this.belongsToCurrentSession(task)) {
      debug(
        "fire: skip foreign session-only task",
        task.id,
        "sessionId=",
        task.sessionId,
        "current=",
        this.currentSessionId,
      );
      return;
    }

    // Notify UI — guard against stale ctx / torn-down runner.
    try {
      if (this.ctx.hasUI) {
        const label = task.label || task.prompt.slice(0, 40);
        this.ctx.ui.notify(`Loop firing: ${label}`, "info");
      }
    } catch (e) {
      this.disarm(`notify threw: ${(e as Error)?.message ?? e}`);
      return;
    }

    // Inject prompt as user message — deliverAs: "followUp" so concurrent
    // fires queue during streaming instead of throwing
    // "Agent is already processing".
    try {
      this.pi.sendUserMessage(task.prompt, { deliverAs: "followUp" });
    } catch (e) {
      this.disarm(`sendUserMessage threw: ${(e as Error)?.message ?? e}`);
      return;
    }

    // Update fire time and bookkeeping.
    try {
      task.lastFiredAt = Date.now();

      if (task.recurring) {
        if (this.isAgedOut(task)) {
          // Final fire — remove the task
          debug("fire:", task.id, "aged out, removing after final fire");
          removeTask(task.id);
          if (this.ctx.hasUI) {
            this.ctx.ui.notify(
              `Loop ${task.id} expired after 7 days`,
              "warning",
            );
          }
        } else {
          updateTask(task);
        }
      } else {
        // One-shot: remove after firing
        removeTask(task.id);
      }

      // Persist durable tasks
      if (task.durable) {
        writeDurableTasks(this.cwd, this.config).catch(() => {});
      }

      this.updateStatus();
    } catch (e) {
      this.disarm(`post-fire bookkeeping threw: ${(e as Error)?.message ?? e}`);
    }
  }

  private drainPendingFires(): void {
    if (this.disarmed || !this.ctx) return;
    const ids = this.pendingFires.splice(0);
    for (const id of ids) {
      const task = getTask(id);
      if (task) {
        this.fire(task);
      }
    }
  }

  private isAgedOut(task: LoopTask): boolean {
    if (this.config.recurringMaxAgeMs <= 0) return false;
    return Date.now() - task.createdAt >= this.config.recurringMaxAgeMs;
  }

  private updateStatus(): void {
    if (this.disarmed || !this.ctx) return;
    try {
      if (!this.ctx.hasUI) return;
      const count = getAllTasks().length;
      if (count > 0) {
        this.ctx.ui.setStatus("pi-loop", `${count} loop${count === 1 ? "" : "s"} active`);
      } else {
        this.ctx.ui.setStatus("pi-loop", undefined);
      }
    } catch (e) {
      this.disarm(`updateStatus threw: ${(e as Error)?.message ?? e}`);
    }
  }

  /** Refresh status bar (called externally after task changes) */
  refreshStatus(): void {
    this.updateStatus();
  }
}
