import { describe, expect, it } from 'vitest'
import {
  MAX_HISTORY,
  STALE_AFTER_SECONDS,
  applyAlertPush,
  applyReadings,
  formatValue,
  isStale,
  seedLatest,
  sortAlerts,
  type AlertPush,
  type ReadingPush,
} from './telemetryState'
import type { AlertDto } from './api'

const T0 = Date.parse('2026-06-01T12:00:00Z')
const iso = (seconds: number) => new Date(T0 + seconds * 1000).toISOString()
const reading = (sensorId: string, seconds: number, value: number): ReadingPush => ({ sensorId, timestamp: iso(seconds), value })

describe('applyReadings', () => {
  it('keeps the newest reading of each sensor and builds history in chronological order', () => {
    const state = applyReadings({}, [reading('a', 1, 10), reading('a', 2, 20), reading('b', 1, 5)])

    expect(state.a.latest?.v).toBe(20)
    expect(state.a.history.map((p) => p.v)).toEqual([10, 20])
    expect(state.b.latest?.v).toBe(5)
  })

  it('ignores late or duplicated readings (the dashboard never goes back in time)', () => {
    let state = applyReadings({}, [reading('a', 10, 100)])
    state = applyReadings(state, [reading('a', 5, 50), reading('a', 10, 999)])

    expect(state.a.latest?.v).toBe(100)
    expect(state.a.history).toHaveLength(1)
  })

  it('bounds the history so memory does not grow with time', () => {
    const many = Array.from({ length: MAX_HISTORY * 3 }, (_, i) => reading('a', i, i))

    const state = applyReadings({}, many)

    expect(state.a.history).toHaveLength(MAX_HISTORY)
    expect(state.a.history.at(-1)?.v).toBe(MAX_HISTORY * 3 - 1)
  })

  it('does not mutate the previous state (React relies on new references)', () => {
    const before = applyReadings({}, [reading('a', 1, 1)])
    const after = applyReadings(before, [reading('a', 2, 2)])

    expect(after).not.toBe(before)
    expect(before.a.history).toHaveLength(1)
  })
})

describe('seedLatest', () => {
  const seed = (sensorId: string, seconds: number, value: number) => ({
    sensorId,
    timestamp: iso(seconds),
    value,
    ageSeconds: 0,
  })

  it('fills sensors that have no live data yet', () => {
    const state = seedLatest({}, [seed('a', 1, 7)])

    expect(state.a.latest?.v).toBe(7)
  })

  it('never overwrites a newer live value with an older snapshot', () => {
    const live = applyReadings({}, [reading('a', 10, 100)])

    const state = seedLatest(live, [seed('a', 5, 50)])

    expect(state.a.latest?.v).toBe(100)
  })
})

describe('isStale', () => {
  it('is stale without data or after the threshold, fresh before it', () => {
    const latest = { t: T0, v: 1 }

    expect(isStale(undefined, T0)).toBe(true)
    expect(isStale(latest, T0 + (STALE_AFTER_SECONDS - 1) * 1000)).toBe(false)
    expect(isStale(latest, T0 + (STALE_AFTER_SECONDS + 1) * 1000)).toBe(true)
  })
})

describe('alerts', () => {
  const fired: AlertPush = {
    alertId: 'x1',
    ruleId: 'r1',
    sensorId: 's1',
    kind: 'Fired',
    severity: 'Critical',
    at: iso(0),
    value: 95,
    message: 'Temp alta',
  }

  it('adds on Fired and removes on Resolved', () => {
    let open = applyAlertPush({}, fired)
    expect(Object.keys(open)).toEqual(['x1'])
    expect(open.x1.status).toBe('Firing')

    open = applyAlertPush(open, { ...fired, kind: 'Resolved' })
    expect(open).toEqual({})
  })

  it('is idempotent for republished events (the engine is at-least-once)', () => {
    const once = applyAlertPush({}, fired)
    const twice = applyAlertPush(once, fired)

    expect(twice).toBe(once) // mesma referência: nenhuma re-renderização
    expect(applyAlertPush({}, { ...fired, kind: 'Resolved' })).toEqual({}) // resolver o que não está aberto é no-op
  })

  it('sorts by severity first and then by most recent', () => {
    const dto = (id: string, severity: AlertDto['severity'], seconds: number): AlertDto => ({
      id,
      ruleId: 'r',
      sensorId: 's',
      severity,
      status: 'Firing',
      message: id,
      triggerValue: 1,
      firedAt: iso(seconds),
      acknowledgedBy: null,
      resolvedAt: null,
    })
    const open = Object.fromEntries(
      [dto('warn-old', 'Warning', 1), dto('crit-old', 'Critical', 2), dto('crit-new', 'Critical', 9), dto('info', 'Info', 100)].map((a) => [a.id, a]),
    )

    expect(sortAlerts(open).map((a) => a.id)).toEqual(['crit-new', 'crit-old', 'warn-old', 'info'])
  })
})

describe('formatValue', () => {
  it('formats with the unit and shows a dash when there is no value', () => {
    expect(formatValue(21.456, '°C')).toBe('21.46 °C')
    expect(formatValue(1013.25, 'hPa')).toBe('1013.3 hPa')
    expect(formatValue(undefined, '°C')).toBe('—')
  })
})
