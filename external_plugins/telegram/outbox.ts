/**
 * Durable outbound-send outbox for the Telegram channel.
 *
 * The gotcha this exists to fix: a send can leave the local process and
 * still be lost — the Bot API call can throw after Telegram already queued
 * the message, the process can be SIGKILL'd mid-retry, or a restart can
 * happen between "we decided to send" and "we confirmed it went out." From
 * the outside this looks like "got a message id, message never arrived" or
 * "no id, no error, nothing arrives" (send-id ≠ delivered). Without a
 * durable record, a mid-flight send is just gone.
 *
 * This module persists send *intent* before the Bot API call (status:
 * pending) and the outcome after it (acked / failed-permanent), to
 * STATE_DIR/outbox.json — same atomic tmp-then-rename write pattern
 * server.ts already uses for access.json. On poller startup, any entry
 * still `pending` (the process died between persisting intent and
 * persisting the outcome) gets resumed, subject to max-age and
 * max-attempts guards.
 *
 * At-least-once, not exactly-once: the Bot API has no idempotency key for
 * sendMessage/sendPhoto/etc, so a crash in the narrow window after
 * Telegram accepted the send but before the local ack is persisted can
 * produce a duplicate on resume. That's an accepted tradeoff for this
 * channel — a rare duplicate is far better than a silently dropped
 * message. Every resend is recorded in `resumes` so a duplicate is
 * traceable back to a specific restart, not a mystery.
 *
 * Framework-agnostic on purpose: this file only depends on `grammy`'s
 * error type for classification, never on a live `Bot` instance, so it's
 * unit-testable without a network connection or a real bot token.
 */

import { readFileSync, writeFileSync, renameSync, mkdirSync } from 'fs'
import { dirname } from 'path'
import { randomBytes } from 'crypto'
import { GrammyError } from 'grammy'

export type OutboxPayload =
  | { kind: 'text'; text: string; parseMode?: 'MarkdownV2' | 'HTML'; replyToMessageId?: number }
  | { kind: 'photo' | 'voice' | 'document'; filePath: string; replyToMessageId?: number }

export type OutboxStatus = 'pending' | 'acked' | 'failed-permanent' | 'expired'

export type OutboxEntry = {
  id: string
  chatId: string
  payload: OutboxPayload
  createdAt: number
  updatedAt: number
  attempts: number
  status: OutboxStatus
  lastError?: string
  sentMessageId?: number
  /** Timestamps of every startup-resend attempt on this entry — traceability, not dedup. */
  resumes: number[]
}

export type SendOutcome = { ok: true; messageId: number } | { ok: false; error: unknown }

// --- Tunables -------------------------------------------------------------

/** After this many attempts a still-failing retryable entry is given up on. */
export const OUTBOX_MAX_ATTEMPTS = 5
/** Entries older than this at poller startup are marked expired, not resent. */
export const OUTBOX_MAX_AGE_MS = 6 * 60 * 60 * 1000
/** Acked entries are pruned once they've sat around this long — they did their job. */
const PRUNE_ACKED_AFTER_MS = 24 * 60 * 60 * 1000
/**
 * failed-permanent/expired entries are kept longer than acked ones — they're
 * the ones a human actually wants to review — but still bounded so the file
 * doesn't grow forever.
 */
const PRUNE_TERMINAL_AFTER_MS = 7 * 24 * 60 * 60 * 1000

const BACKOFF_BASE_MS = 500
const BACKOFF_FACTOR = 2
const BACKOFF_CAP_MS = 30_000

/**
 * Exponential backoff with full jitter: delay is uniform on
 * [0, min(cap, base * factor^(attempt-1))]. `rng` is injectable for
 * deterministic tests.
 */
export function computeBackoff(attempt: number, rng: () => number = Math.random): number {
  const ceiling = Math.min(BACKOFF_CAP_MS, BACKOFF_BASE_MS * BACKOFF_FACTOR ** Math.max(0, attempt - 1))
  return Math.round(rng() * ceiling)
}

/**
 * Classifies a send failure. Mirrors the reasoning already in server.ts's
 * `retryingSend`: 429 and 5xx (and anything without an error_code, i.e. a
 * network/timeout failure — grammy's HttpError or a raw fetch failure) are
 * retryable; any other 4xx is a permanent client error (bad chat_id,
 * blocked bot, bad request shape) that retrying will never fix.
 */
export function classifySendError(err: unknown): { retryable: boolean; retryAfterMs?: number; message: string } {
  if (err instanceof GrammyError) {
    const code = err.error_code
    if (code === 429) {
      const ra = err.parameters?.retry_after
      return {
        retryable: true,
        retryAfterMs: typeof ra === 'number' ? ra * 1000 : undefined,
        message: err.message,
      }
    }
    if (code >= 500) return { retryable: true, message: err.message }
    return { retryable: false, message: err.message }
  }
  // grammy's HttpError (network failure) and any other thrown value carry no
  // error_code — treat as transient, same as retryingSend's `code === undefined`.
  return { retryable: true, message: err instanceof Error ? err.message : String(err) }
}

export class Outbox {
  constructor(private readonly filePath: string) {}

  private load(): OutboxEntry[] {
    try {
      const raw = readFileSync(this.filePath, 'utf8')
      const parsed = JSON.parse(raw) as { entries?: OutboxEntry[] }
      return Array.isArray(parsed.entries) ? parsed.entries : []
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code === 'ENOENT') return []
      // Corrupt file — move aside (same convention as access.json) and start
      // fresh rather than crashing the channel over a torn write.
      try {
        renameSync(this.filePath, `${this.filePath}.corrupt-${Date.now()}`)
      } catch {}
      return []
    }
  }

  private save(entries: OutboxEntry[]): void {
    mkdirSync(dirname(this.filePath), { recursive: true, mode: 0o700 })
    const tmp = `${this.filePath}.tmp`
    writeFileSync(tmp, JSON.stringify({ entries }, null, 2) + '\n', { mode: 0o600 })
    renameSync(tmp, this.filePath)
  }

  /** Read-modify-write a single entry by id. No-op (returns undefined) if it's gone. */
  private mutate(id: string, fn: (e: OutboxEntry) => void): OutboxEntry | undefined {
    const entries = this.load()
    const entry = entries.find(e => e.id === id)
    if (!entry) return undefined
    fn(entry)
    this.save(entries)
    return entry
  }

  /** Persist send intent as `pending` before the Bot API call is made. */
  enqueue(chatId: string, payload: OutboxPayload, now = Date.now()): OutboxEntry {
    const entries = this.load()
    const entry: OutboxEntry = {
      id: randomBytes(8).toString('hex'),
      chatId,
      payload,
      createdAt: now,
      updatedAt: now,
      attempts: 0,
      status: 'pending',
      resumes: [],
    }
    entries.push(entry)
    this.save(entries)
    return entry
  }

  /**
   * Records the outcome of one Bot API attempt. On success: acked. On a
   * retryable failure under the attempt cap: stays pending (caller retries).
   * On a permanent failure, or a retryable one that's exhausted its
   * attempts: failed-permanent.
   */
  recordAttempt(id: string, outcome: SendOutcome, now = Date.now()): OutboxEntry | undefined {
    return this.mutate(id, entry => {
      entry.attempts += 1
      entry.updatedAt = now
      if (outcome.ok) {
        entry.status = 'acked'
        entry.sentMessageId = outcome.messageId
        entry.lastError = undefined
        return
      }
      const { retryable, message } = classifySendError(outcome.error)
      entry.lastError = message
      if (!retryable) {
        entry.status = 'failed-permanent'
      } else if (entry.attempts >= OUTBOX_MAX_ATTEMPTS) {
        entry.status = 'failed-permanent'
        entry.lastError = `${message} (max attempts reached)`
      } else {
        entry.status = 'pending'
      }
    })
  }

  getPending(): OutboxEntry[] {
    return this.load().filter(e => e.status === 'pending')
  }

  getAll(): OutboxEntry[] {
    return this.load()
  }

  /** Drops acked entries past their retention window, and terminal entries past theirs. Returns count dropped. */
  pruneOld(now = Date.now()): number {
    const entries = this.load()
    const kept = entries.filter(e => {
      if (e.status === 'acked') return now - e.updatedAt < PRUNE_ACKED_AFTER_MS
      if (e.status === 'failed-permanent' || e.status === 'expired') {
        return now - e.updatedAt < PRUNE_TERMINAL_AFTER_MS
      }
      return true // pending is never pruned by age — resumeOnStartup's expiry is the only way out
    })
    if (kept.length !== entries.length) this.save(kept)
    return entries.length - kept.length
  }

  /**
   * Called on poller startup (including re-promotion after a peer dies —
   * see server.ts's yield/promote watchdog). Walks every entry still
   * `pending` and, subject to guards, resends it via `send`.
   *
   * Guards, applied before any network call:
   *   - max age (OUTBOX_MAX_AGE_MS): too stale to still be relevant → expired.
   *   - max attempts (OUTBOX_MAX_ATTEMPTS): already tried enough → failed-permanent.
   *
   * Each entry is read-modify-written individually (not one batch save at
   * the end) so a crash partway through the resume loop leaves a
   * consistent trail instead of losing the whole batch's progress, and so
   * a concurrent enqueue from another process (e.g. a tool-only peer
   * handling a live `reply` call while this process resumes) isn't
   * clobbered by an overwrite of the full file.
   */
  async resumeOnStartup(
    send: (entry: OutboxEntry) => Promise<number>,
    now = Date.now(),
  ): Promise<{ resumed: number; acked: number; expired: number; permanentFailed: number; stillPending: number }> {
    const pendingIds = this.getPending().map(e => e.id)
    let resumed = 0
    let acked = 0
    let expired = 0
    let permanentFailed = 0

    for (const id of pendingIds) {
      const snapshot = this.load().find(e => e.id === id)
      if (!snapshot || snapshot.status !== 'pending') continue // already resolved by someone else

      if (now - snapshot.createdAt > OUTBOX_MAX_AGE_MS) {
        this.mutate(id, e => {
          e.status = 'expired'
          e.updatedAt = now
        })
        expired++
        continue
      }
      if (snapshot.attempts >= OUTBOX_MAX_ATTEMPTS) {
        this.mutate(id, e => {
          e.status = 'failed-permanent'
          e.updatedAt = now
          e.lastError = `${e.lastError ?? ''} (max attempts reached before resend)`.trim()
        })
        permanentFailed++
        continue
      }

      this.mutate(id, e => e.resumes.push(now))
      resumed++
      try {
        const messageId = await send(snapshot)
        this.mutate(id, e => {
          e.attempts += 1
          e.updatedAt = Date.now()
          e.status = 'acked'
          e.sentMessageId = messageId
          e.lastError = undefined
        })
        acked++
      } catch (err) {
        const { retryable, message } = classifySendError(err)
        const updated = this.mutate(id, e => {
          e.attempts += 1
          e.updatedAt = Date.now()
          e.lastError = message
          e.status = !retryable || e.attempts >= OUTBOX_MAX_ATTEMPTS ? 'failed-permanent' : 'pending'
        })
        if (updated?.status === 'failed-permanent') permanentFailed++
      }
    }

    const stillPending = this.getPending().length
    return { resumed, acked, expired, permanentFailed, stillPending }
  }
}
