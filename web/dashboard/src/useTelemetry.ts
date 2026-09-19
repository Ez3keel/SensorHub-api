import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { latestValues, openAlerts, type AlertDto, type Sensor } from './api'
import { TelemetryClient, type ConnectionStatus } from './realtime'
import {
  applyAlertPush,
  applyReadings,
  seedLatest,
  toOpenAlerts,
  type AlertPush,
  type LiveState,
  type OpenAlerts,
  type ReadingPush,
} from './telemetryState'

export interface Toast {
  id: string
  alert: AlertPush
}

/**
 * Estado ao vivo do dashboard. O servidor já coalesce (no máximo 4 atualizações por sensor por segundo); mesmo assim as
 * mensagens são acumuladas num buffer e aplicadas ao estado do React no máximo uma vez por quadro de animação, para que
 * um lote grande não provoque dezenas de re-renderizações.
 */
export function useTelemetry(selectedGroup: string | null, sensors: Sensor[]) {
  const [status, setStatus] = useState<ConnectionStatus>('connecting')
  const [live, setLive] = useState<LiveState>({})
  const [alerts, setAlerts] = useState<OpenAlerts>({})
  const [toasts, setToasts] = useState<Toast[]>([])
  const [detailReadings, setDetailReadings] = useState<ReadingPush[]>([])

  const client = useRef<TelemetryClient | null>(null)
  const buffer = useRef<ReadingPush[]>([])
  const detailBuffer = useRef<ReadingPush[]>([])
  const frame = useRef<number | null>(null)
  const previousGroup = useRef<string | null>(null)

  const schedule = useCallback(() => {
    if (frame.current !== null) return
    frame.current = requestAnimationFrame(() => {
      frame.current = null
      if (buffer.current.length > 0) {
        const batch = buffer.current
        buffer.current = []
        setLive((state) => applyReadings(state, batch))
      }
      if (detailBuffer.current.length > 0) {
        const batch = detailBuffer.current
        detailBuffer.current = []
        setDetailReadings((current) => [...current, ...batch].slice(-500))
      }
    })
  }, [])

  // Conexão única durante a vida do app.
  useEffect(() => {
    const c = new TelemetryClient({
      onStatus: setStatus,
      onReadings: (list) => {
        buffer.current.push(...list)
        schedule()
      },
      onReading: (r) => {
        detailBuffer.current.push(r)
        schedule()
      },
      onAlert: (alert) => {
        setAlerts((open) => applyAlertPush(open, alert))
        if (alert.kind === 'Fired' && alert.severity !== 'Info')
          setToasts((t) => (t.some((x) => x.id === alert.alertId) ? t : [...t.slice(-3), { id: alert.alertId, alert }]))
      },
    })
    client.current = c

    void c.start().then(() => c.subscribeAlerts())
    void openAlerts().then((list) => setAlerts((current) => ({ ...toOpenAlerts(list), ...current })))

    return () => {
      void c.stop()
      if (frame.current !== null) cancelAnimationFrame(frame.current)
    }
  }, [schedule])

  // Troca de grupo: assina o novo, solta o anterior e semeia com o "último valor" do REST.
  useEffect(() => {
    const c = client.current
    if (!c || !selectedGroup) return

    const previous = previousGroup.current
    if (previous && previous !== selectedGroup) void c.unsubscribeGroup(previous)
    previousGroup.current = selectedGroup
    void c.subscribeGroup(selectedGroup)

    const ids = sensors.filter((s) => s.group === selectedGroup).map((s) => s.id)
    void latestValues(ids).then((values) => setLive((state) => seedLatest(state, values)))
  }, [selectedGroup, sensors])

  const dismissToast = useCallback((id: string) => setToasts((t) => t.filter((x) => x.id !== id)), [])
  const acknowledged = useCallback((updated: AlertDto) => setAlerts((open) => ({ ...open, [updated.id]: updated })), [])

  const watchSensor = useCallback(async (sensorId: string | null, previous: string | null) => {
    const c = client.current
    if (!c) return
    setDetailReadings([])
    detailBuffer.current = []
    if (previous) await c.unsubscribeSensor(previous)
    if (sensorId) await c.subscribeSensor(sensorId)
  }, [])

  return useMemo(
    () => ({ status, live, alerts, toasts, detailReadings, dismissToast, acknowledged, watchSensor }),
    [status, live, alerts, toasts, detailReadings, dismissToast, acknowledged, watchSensor],
  )
}
