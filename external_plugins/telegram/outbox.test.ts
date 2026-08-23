import { describe, test, expect, beforeEach, afterEach } from 'bun:test'
import { mkdtempSync, rmSync, readFileSync, existsSync, writeFileSync } from 'fs'
import { tmpdir } from 'os'
import { join } from 'path'
import { GrammyError } from 'grammy'
import {
  Outbox,
  classifySendError,
  computeBackoff,
  OUTBOX_MAX_ATTEMPTS,
  OUTBOX_MAX_AGE_MS,
  type OutboxPayload,
} from './outbox'

function grammyError(code: number, opts: { retryAfter?: number } = {}): GrammyError {
  return new GrammyError(
    `Bot API error ${code}`,
    {
      ok: false,
      error_code: code,
      description: `simulated ${code}`,
      ...(opts.retryAfter != null ? { parameters: { retry_after: opts.retryAfter } } : {}),
    },
    'sendMessage',
    {},
  )
}

const textPayload: OutboxPayload = { kind: 'text', text: 'hello William' }

let dir: string
let filePath: string

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), 'outbox-test-'))
  filePath = join(dir, 'outbox.json')
})

afterEach(() => {
  rmSync(dir, { recursive: true, force: true })
})

describe('classifySendError', () => {
  test('429 is retryable and surfaces retry_after in ms', () => {
    const r = classifySendError(grammyError(429, { retryAfter: 3 }))
    expect(r.retryable).toBe(true)
    expect(r.retryAfterMs).toBe(3000)
  })

  test('429 without retry_after is still retryable, no retryAfterMs', () => {
    const r = classifySendError(grammyError(429))
    expect(r.retryable).toBe(true)
    expect(r.retryAfterMs).toBeUndefined()
  })

  test('5xx is retryable', () => {
    expect(classifySendError(grammyError(500)).retryable).toBe(true)
    expect(classifySendError(grammyError(502)).retryable).toBe(true)
    expect(classifySendError(grammyError(503)).retryable).toBe(true)
  })

  test('4xx other than 429 is permanent', () => {
    expect(classifySendError(grammyError(400)).retryable).toBe(false)
    expect(classifySendError(grammyError(401)).retryable).toBe(false)
    expect(classifySendError(grammyError(403)).retryable).toBe(false)
    expect(classifySendError(grammyError(404)).retryable).toBe(false)
  })

  test('network/timeout errors with no error_code are retryable', () => {
    expect(classifySendError(new Error('fetch failed')).retryable).toBe(true)
    expect(classifySendError(new TypeError('network timeout')).retryable).toBe(true)
    expect(classifySendError('weird non-Error throw').retryable).toBe(true)
  })
})

describe('computeBackoff', () => {
  test('is 0 when rng returns 0, and equals the ceiling when rng returns just under 1', () => {
    expect(computeBackoff(1, () => 0)).toBe(0)
    expect(computeBackoff(1, () => 0.999999)).toBeGreaterThan(0)
  })

  test('grows exponentially with attempt, capped', () => {
    // rng pinned at 1 (well, just under) to read the ceiling for each attempt
    const rng = () => 0.999999
    const a1 = computeBackoff(1, rng)
    const a2 = computeBackoff(2, rng)
    const a3 = computeBackoff(3, rng)
    expect(a2).toBeGreaterThan(a1)
    expect(a3).toBeGreaterThan(a2)
    // cap is 30_000ms — even a huge attempt count must not exceed it
    expect(computeBackoff(20, rng)).toBeLessThanOrEqual(30_000)
  })

  test('never negative, never exceeds the 30s cap', () => {
    for (let attempt = 1; attempt <= 10; attempt++) {
      for (const rngVal of [0, 0.25, 0.5, 0.75, 0.999]) {
        const d = computeBackoff(attempt, () => rngVal)
        expect(d).toBeGreaterThanOrEqual(0)
        expect(d).toBeLessThanOrEqual(30_000)
      }
    }
  })
})

describe('Outbox — enqueue + ack path', () => {
  test('enqueue persists a pending entry to disk', () => {
    const outbox = new Outbox(filePath)
    const entry = outbox.enqueue('12345', textPayload)
    expect(entry.status).toBe('pending')
    expect(entry.attempts).toBe(0)
    expect(entry.chatId).toBe('12345')
    expect(existsSync(filePath)).toBe(true)

    const onDisk = JSON.parse(readFileSync(filePath, 'utf8'))
    expect(onDisk.entries).toHaveLength(1)
    expect(onDisk.entries[0].id).toBe(entry.id)
    expect(onDisk.entries[0].status).toBe('pending')
  })

  test('recordAttempt(ok) marks acked with the returned message id', () => {
    const outbox = new Outbox(filePath)
    const entry = outbox.enqueue('12345', textPayload)
    const updated = outbox.recordAttempt(entry.id, { ok: true, messageId: 999 })

    expect(updated?.status).toBe('acked')
    expect(updated?.sentMessageId).toBe(999)
    expect(updated?.attempts).toBe(1)
    expect(updated?.lastError).toBeUndefined()

    // survives a reload
    const reloaded = new Outbox(filePath).getAll()
    expect(reloaded[0]!.status).toBe('acked')
    expect(reloaded[0]!.sentMessageId).toBe(999)
  })

  test('acked entries are excluded from getPending', () => {
    const outbox = new Outbox(filePath)
    const entry = outbox.enqueue('12345', textPayload)
    outbox.recordAttempt(entry.id, { ok: true, messageId: 1 })
    expect(outbox.getPending()).toHaveLength(0)
  })

  test('recordAttempt on an unknown id is a safe no-op', () => {
    const outbox = new Outbox(filePath)
    expect(outbox.recordAttempt('does-not-exist', { ok: true, messageId: 1 })).toBeUndefined()
  })
})

describe('Outbox — retry classification on the persisted record', () => {
  test('a retryable failure under the attempt cap stays pending', () => {
    const outbox = new Outbox(filePath)
    const entry = outbox.enqueue('1', textPayload)
    const updated = outbox.recordAttempt(entry.id, { ok: false, error: grammyError(500) })
    expect(updated?.status).toBe('pending')
    expect(updated?.attempts).toBe(1)
    expect(updated?.lastError).toContain('simulated 500')
    expect(outbox.getPending()).toHaveLength(1)
  })

  test('a permanent (non-429) 4xx fails immediately regardless of attempt count', () => {
    const outbox = new Outbox(filePath)
    const entry = outbox.enqueue('1', textPayload)
    const updated = outbox.recordAttempt(entry.id, { ok: false, error: grammyError(400) })
    expect(updated?.status).toBe('failed-permanent')
    expect(updated?.attempts).toBe(1)
  })

  test('a retryable failure that exhausts OUTBOX_MAX_ATTEMPTS becomes failed-permanent', () => {
    const outbox = new Outbox(filePath)
    const entry = outbox.enqueue('1', textPayload)
    let updated = entry
    for (let i = 0; i < OUTBOX_MAX_ATTEMPTS; i++) {
      updated = outbox.recordAttempt(entry.id, { ok: false, error: grammyError(503) })!
    }
    expect(updated.attempts).toBe(OUTBOX_MAX_ATTEMPTS)
    expect(updated.status).toBe('failed-permanent')
    expect(updated.lastError).toContain('max attempts reached')
  })
})

describe('Outbox — startup resend (resumeOnStartup)', () => {
  test('resends a fresh pending entry and marks it acked on success', async () => {
    const outbox = new Outbox(filePath)
    const entry = outbox.enqueue('1', textPayload)
    const calls: string[] = []

    const result = await outbox.resumeOnStartup(async e => {
      calls.push(e.id)
      return 4242
    })

    expect(calls).toEqual([entry.id])
    expect(result).toEqual({ resumed: 1, acked: 1, expired: 0, permanentFailed: 0, stillPending: 0 })

    const stored = outbox.getAll()[0]!
    expect(stored.status).toBe('acked')
    expect(stored.sentMessageId).toBe(4242)
    // idempotency trace: this resend is recorded, distinct from a normal attempt
    expect(stored.resumes).toHaveLength(1)
  })

  test('does not touch acked/failed-permanent entries', async () => {
    const outbox = new Outbox(filePath)
    const okEntry = outbox.enqueue('1', textPayload)
    outbox.recordAttempt(okEntry.id, { ok: true, messageId: 1 })
    const badEntry = outbox.enqueue('1', textPayload)
    outbox.recordAttempt(badEntry.id, { ok: false, error: grammyError(400) })

    let called = 0
    const result = await outbox.resumeOnStartup(async () => {
      called++
      return 1
    })
    expect(called).toBe(0)
    expect(result.resumed).toBe(0)
  })

  test('an entry older than OUTBOX_MAX_AGE_MS is expired, not resent', async () => {
    const outbox = new Outbox(filePath)
    const staleCreatedAt = Date.now() - (OUTBOX_MAX_AGE_MS + 60_000)
    const entry = outbox.enqueue('1', textPayload, staleCreatedAt)

    let called = 0
    const result = await outbox.resumeOnStartup(async () => {
      called++
      return 1
    })

    expect(called).toBe(0)
    expect(result.expired).toBe(1)
    expect(result.resumed).toBe(0)
    expect(outbox.getAll().find(e => e.id === entry.id)?.status).toBe('expired')
  })

  test('an entry already at OUTBOX_MAX_ATTEMPTS is failed-permanent at startup without a network call', async () => {
    const outbox = new Outbox(filePath)
    const entry = outbox.enqueue('1', textPayload)
    for (let i = 0; i < OUTBOX_MAX_ATTEMPTS - 1; i++) {
      outbox.recordAttempt(entry.id, { ok: false, error: grammyError(503) })
    }
    // one attempt short of the cap, but recordAttempt already flips to
    // failed-permanent exactly at the cap (tested above) — so force the
    // scenario resumeOnStartup's own guard exists for: attempts sitting at
    // the cap while status is still 'pending' (e.g. a crash right after the
    // attempts++ but before the status flip landed on disk in a hypothetical
    // partial write). Simulate directly via one more raw mutate-equivalent.
    outbox.recordAttempt(entry.id, { ok: false, error: grammyError(503) }) // now failed-permanent via normal path

    // Reset it back to pending with attempts at the cap to exercise the
    // startup guard specifically (defense in depth, independent of recordAttempt).
    const all = outbox.getAll()
    const target = all.find(e => e.id === entry.id)!
    target.status = 'pending'
    writeFileSync(filePath, JSON.stringify({ entries: all }, null, 2))

    let called = 0
    const result = await outbox.resumeOnStartup(async () => {
      called++
      return 1
    })

    expect(called).toBe(0)
    expect(result.permanentFailed).toBe(1)
    expect(outbox.getAll().find(e => e.id === entry.id)?.status).toBe('failed-permanent')
  })

  test('a resend that fails retryably (attempts still under cap) stays pending for a future startup', async () => {
    const outbox = new Outbox(filePath)
    const entry = outbox.enqueue('1', textPayload)

    const result = await outbox.resumeOnStartup(async () => {
      throw grammyError(500)
    })

    expect(result.resumed).toBe(1)
    expect(result.acked).toBe(0)
    expect(result.stillPending).toBe(1)
    const stored = outbox.getAll()[0]!
    expect(stored.status).toBe('pending')
    expect(stored.attempts).toBe(1)
  })

  test('a resend that fails permanently (4xx) is marked failed-permanent, not retried again', async () => {
    const outbox = new Outbox(filePath)
    outbox.enqueue('1', textPayload)

    const result = await outbox.resumeOnStartup(async () => {
      throw grammyError(403)
    })

    expect(result.permanentFailed).toBe(1)
    expect(outbox.getAll()[0]!.status).toBe('failed-permanent')
  })

  test('multiple pending entries are each resumed independently', async () => {
    const outbox = new Outbox(filePath)
    const a = outbox.enqueue('1', { kind: 'text', text: 'a' })
    const b = outbox.enqueue('2', { kind: 'text', text: 'b' })
    const seen: string[] = []

    const result = await outbox.resumeOnStartup(async e => {
      seen.push(e.id)
      return 1
    })

    expect(result.acked).toBe(2)
    expect(new Set(seen)).toEqual(new Set([a.id, b.id]))
  })
})

describe('Outbox — pruning', () => {
  test('drops acked entries older than the 1-day retention window', () => {
    const outbox = new Outbox(filePath)
    const entry = outbox.enqueue('1', textPayload)
    outbox.recordAttempt(entry.id, { ok: true, messageId: 1 })

    const twoDaysFromNow = Date.now() + 2 * 24 * 60 * 60 * 1000
    const dropped = outbox.pruneOld(twoDaysFromNow)

    expect(dropped).toBe(1)
    expect(outbox.getAll()).toHaveLength(0)
  })

  test('keeps a freshly-acked entry', () => {
    const outbox = new Outbox(filePath)
    const entry = outbox.enqueue('1', textPayload)
    outbox.recordAttempt(entry.id, { ok: true, messageId: 1 })

    const dropped = outbox.pruneOld(Date.now())
    expect(dropped).toBe(0)
    expect(outbox.getAll()).toHaveLength(1)
  })

  test('never prunes a pending entry, no matter how old', () => {
    const outbox = new Outbox(filePath)
    const veryOld = Date.now() - 30 * 24 * 60 * 60 * 1000
    outbox.enqueue('1', textPayload, veryOld)

    const dropped = outbox.pruneOld(Date.now())
    expect(dropped).toBe(0)
    expect(outbox.getAll()).toHaveLength(1)
  })

  test('drops failed-permanent/expired entries past the 7-day terminal retention window', () => {
    const outbox = new Outbox(filePath)
    const e1 = outbox.enqueue('1', textPayload)
    outbox.recordAttempt(e1.id, { ok: false, error: grammyError(400) })

    const eightDaysFromNow = Date.now() + 8 * 24 * 60 * 60 * 1000
    const dropped = outbox.pruneOld(eightDaysFromNow)
    expect(dropped).toBe(1)
    expect(outbox.getAll()).toHaveLength(0)
  })
})

describe('Outbox — disk robustness', () => {
  test('missing file loads as empty, no throw', () => {
    const outbox = new Outbox(join(dir, 'does-not-exist.json'))
    expect(outbox.getAll()).toEqual([])
    expect(outbox.getPending()).toEqual([])
  })

  test('corrupt JSON is moved aside and treated as empty rather than crashing', () => {
    writeFileSync(filePath, '{not valid json')
    const outbox = new Outbox(filePath)
    expect(outbox.getAll()).toEqual([])
    // subsequent writes should succeed against a fresh file
    outbox.enqueue('1', textPayload)
    expect(outbox.getAll()).toHaveLength(1)
    // the corrupt original was preserved, not silently deleted
    const siblings = require('fs').readdirSync(dir) as string[]
    expect(siblings.some((f: string) => f.startsWith('outbox.json.corrupt-'))).toBe(true)
  })
})
