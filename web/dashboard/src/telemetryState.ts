// Lógica de estado do dashboard, sem React: pura e testável.

import type { AlertDto, LatestValue, Severity } from './api'

export interface ReadingPush {
  sensorId: string
  timestamp: string
  value: number
}

export interface AlertPush {
  alertId: string
  ruleId: string
  sensorId: string
  kind: 'Fired' | 'Resolved'
  severity: Severity
  at: string
  value: number | null
  message: string
}

export interface Point {
  t: number // epoch ms
  v: number
}

export interface SensorLive {
  latest?: Point
  history: Point[] // últimos MAX_HISTORY pontos, em ordem cronológica
}

export type LiveState = Record<string, SensorLive>

export const MAX_HISTORY = 60
/** Sem leitura há mais que isto = o cartão fica "apagado" (o sensor pode estar offline). */
export const STALE_AFTER_SECONDS = 30

/** Aplica um lote de leituras empurradas. Leitura mais antiga que a última conhecida é ignorada (ordem/duplicata). */
export function applyReadings(state: LiveState, readings: ReadingPush[]): LiveState {
  const next: LiveState = { ...state }
  for (const r of readings) {
    const t = Date.parse(r.timestamp)
    const current = next[r.sensorId]
    if (current?.latest && t <= current.latest.t) continue

    const history = [...(current?.history ?? []), { t, v: r.value }]
    next[r.sensorId] = { latest: { t, v: r.value }, history: history.slice(-MAX_HISTORY) }
  }
  return next
}

/** Semeia o estado com o "último valor" vindo do REST, sem sobrescrever o que já chegou ao vivo. */
export function seedLatest(state: LiveState, values: LatestValue[]): LiveState {
  const next: LiveState = { ...state }
  for (const v of values) {
    const t = Date.parse(v.timestamp)
    const current = next[v.sensorId]
    if (current?.latest && current.latest.t >= t) continue
    next[v.sensorId] = { latest: { t, v: v.value }, history: current?.history?.length ? current.history : [{ t, v: v.value }] }
  }
  return next
}

export function isStale(latest: Point | undefined, nowMs: number): boolean {
  return !latest || (nowMs - latest.t) / 1000 > STALE_AFTER_SECONDS
}

/** Alertas abertos por id. Um evento de disparo adiciona; o de resolução remove. Idempotente (o motor é at-least-once). */
export type OpenAlerts = Record<string, AlertDto>

export function applyAlertPush(open: OpenAlerts, push: AlertPush): OpenAlerts {
  if (push.kind === 'Resolved') {
    if (!(push.alertId in open)) return open
    const { [push.alertId]: _resolved, ...rest } = open
    return rest
  }
  if (push.alertId in open) return open // duplicata

  return {
    ...open,
    [push.alertId]: {
      id: push.alertId,
      ruleId: push.ruleId,
      sensorId: push.sensorId,
      severity: push.severity,
      status: 'Firing',
      message: push.message,
      triggerValue: push.value,
      firedAt: push.at,
      acknowledgedBy: null,
      resolvedAt: null,
    },
  }
}

export function toOpenAlerts(list: AlertDto[]): OpenAlerts {
  return Object.fromEntries(list.map((a) => [a.id, a]))
}

const severityRank: Record<Severity, number> = { Critical: 3, Warning: 2, Info: 1 }

/** Mais graves primeiro; empate: mais recente primeiro. */
export function sortAlerts(open: OpenAlerts): AlertDto[] {
  return Object.values(open).sort(
    (a, b) => severityRank[b.severity] - severityRank[a.severity] || Date.parse(b.firedAt) - Date.parse(a.firedAt),
  )
}

export function formatValue(value: number | undefined, unit: string): string {
  return value === undefined ? '—' : `${value.toFixed(value >= 100 ? 1 : 2)} ${unit}`
}
