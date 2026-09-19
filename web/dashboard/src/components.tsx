import { useEffect, useMemo, useState, type FormEvent } from 'react'
import { Area, AreaChart, CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { hasRole, login, type Session } from './auth'
import { acknowledge, recentReadings, series, type AlertDto, type Sensor, type SeriesPoint } from './api'
import type { ConnectionStatus } from './realtime'
import { formatValue, isStale, sortAlerts, type OpenAlerts, type LiveState, type Point, type ReadingPush } from './telemetryState'
import type { Toast } from './useTelemetry'

const statusText: Record<ConnectionStatus, string> = {
  connecting: 'conectando…',
  connected: 'ao vivo',
  reconnecting: 'reconectando…',
  disconnected: 'desconectado',
}

export function ConnectionBadge({ status }: { status: ConnectionStatus }) {
  return (
    <span className={`badge badge-${status}`} title="Estado da conexão em tempo real (SignalR)">
      <span className="dot" /> {statusText[status]}
    </span>
  )
}

export function GroupPicker({ groups, selected, onSelect }: { groups: string[]; selected: string | null; onSelect: (g: string) => void }) {
  return (
    <div className="groups" role="tablist" aria-label="Grupos de sensores">
      {groups.map((g) => (
        <button key={g} role="tab" aria-selected={g === selected} className={g === selected ? 'tab active' : 'tab'} onClick={() => onSelect(g)}>
          {g}
        </button>
      ))}
    </div>
  )
}

function Sparkline({ points, stale }: { points: Point[]; stale: boolean }) {
  if (points.length < 2) return <div className="spark-empty" />
  return (
    <ResponsiveContainer width="100%" height={36}>
      <LineChart data={points}>
        <Line type="monotone" dataKey="v" dot={false} strokeWidth={1.5} stroke={stale ? '#5b6472' : '#4ade80'} isAnimationActive={false} />
      </LineChart>
    </ResponsiveContainer>
  )
}

export function SensorGrid({
  sensors,
  live,
  alerts,
  selectedSensor,
  onSelect,
  nowMs,
}: {
  sensors: Sensor[]
  live: LiveState
  alerts: OpenAlerts
  selectedSensor: string | null
  onSelect: (sensorId: string) => void
  nowMs: number
}) {
  const alerting = useMemo(() => new Set(Object.values(alerts).map((a) => a.sensorId)), [alerts])
  return (
    <div className="grid">
      {sensors.map((s) => {
        const state = live[s.id]
        const stale = isStale(state?.latest, nowMs)
        const cls = ['card', stale ? 'stale' : '', alerting.has(s.id) ? 'alerting' : '', selectedSensor === s.id ? 'selected' : ''].join(' ')
        return (
          <button key={s.id} className={cls} onClick={() => onSelect(s.id)} title={stale ? 'sem leitura recente' : undefined}>
            <span className="card-name">{s.name}</span>
            <span className="card-metric">{s.metric}</span>
            <span className="card-value">{formatValue(state?.latest?.v, s.unit)}</span>
            <Sparkline points={state?.history ?? []} stale={stale} />
          </button>
        )
      })}
    </div>
  )
}

const ranges = [
  { label: 'ao vivo (5 min)', minutes: 5 },
  { label: '1 h', minutes: 60 },
  { label: '6 h', minutes: 360 },
  { label: '24 h', minutes: 1440 },
  { label: '7 d', minutes: 10080 },
] as const

/** Detalhe de um sensor: os últimos 5 min + empurrões ao vivo, ou séries agregadas (avg/min/max) de períodos maiores. */
export function SensorDetail({ sensor, detailReadings, onClose }: { sensor: Sensor; detailReadings: ReadingPush[]; onClose: () => void }) {
  const [minutes, setMinutes] = useState<number>(5)
  const [history, setHistory] = useState<SeriesPoint[]>([])
  const [seed, setSeed] = useState<Point[]>([])
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    setError(null)
    if (minutes === 5) {
      recentReadings(sensor.id, 5)
        .then((pts) => !cancelled && setSeed(pts.map((p) => ({ t: Date.parse(p.ts), v: p.value }))))
        .catch((e: Error) => !cancelled && setError(e.message))
    } else {
      const to = new Date()
      series(sensor.id, new Date(to.getTime() - minutes * 60_000), to)
        .then((pts) => !cancelled && setHistory(pts))
        .catch((e: Error) => !cancelled && setError(e.message))
    }
    return () => {
      cancelled = true
    }
  }, [sensor.id, minutes])

  const liveData = useMemo(() => {
    const merged = new Map<number, number>()
    for (const p of seed) merged.set(p.t, p.v)
    for (const r of detailReadings) if (r.sensorId === sensor.id) merged.set(Date.parse(r.timestamp), r.value)
    const cutoff = Date.now() - 5 * 60_000
    return [...merged.entries()].filter(([t]) => t >= cutoff).sort((a, b) => a[0] - b[0]).map(([t, v]) => ({ t, v }))
  }, [seed, detailReadings, sensor.id])

  return (
    <section className="detail" aria-label={`Detalhe de ${sensor.name}`}>
      <header>
        <div>
          <h2>{sensor.name}</h2>
          <p className="muted">
            {sensor.metric} · {sensor.unit} · grupo {sensor.group}
          </p>
        </div>
        <button className="ghost" onClick={onClose} aria-label="Fechar detalhe">
          ✕
        </button>
      </header>
      <div className="ranges">
        {ranges.map((r) => (
          <button key={r.minutes} className={minutes === r.minutes ? 'tab active' : 'tab'} onClick={() => setMinutes(r.minutes)}>
            {r.label}
          </button>
        ))}
      </div>
      {error && <p className="error">Não foi possível carregar: {error}</p>}
      <div className="chart">
        <ResponsiveContainer width="100%" height={280}>
          {minutes === 5 ? (
            <LineChart data={liveData}>
              <CartesianGrid stroke="#263042" strokeDasharray="3 3" />
              <XAxis dataKey="t" type="number" domain={['dataMin', 'dataMax']} tickFormatter={(t: number) => new Date(t).toLocaleTimeString()} stroke="#7b8494" />
              <YAxis domain={['auto', 'auto']} stroke="#7b8494" width={50} />
              <Tooltip labelFormatter={(t: number) => new Date(t).toLocaleTimeString()} contentStyle={{ background: '#111827', border: '1px solid #263042' }} />
              <Line type="monotone" dataKey="v" name={sensor.unit} dot={false} stroke="#4ade80" strokeWidth={2} isAnimationActive={false} />
            </LineChart>
          ) : (
            <AreaChart data={history}>
              <CartesianGrid stroke="#263042" strokeDasharray="3 3" />
              <XAxis dataKey="bucket" tickFormatter={(b: string) => new Date(b).toLocaleString([], { hour: '2-digit', minute: '2-digit', day: '2-digit', month: '2-digit' })} stroke="#7b8494" />
              <YAxis domain={['auto', 'auto']} stroke="#7b8494" width={50} />
              <Tooltip contentStyle={{ background: '#111827', border: '1px solid #263042' }} />
              <Area type="monotone" dataKey="max" name="máx" stroke="#f87171" fill="#f8717133" isAnimationActive={false} />
              <Area type="monotone" dataKey="avg" name="média" stroke="#4ade80" fill="#4ade8033" isAnimationActive={false} />
              <Area type="monotone" dataKey="min" name="mín" stroke="#60a5fa" fill="#60a5fa22" isAnimationActive={false} />
            </AreaChart>
          )}
        </ResponsiveContainer>
      </div>
    </section>
  )
}

export function AlertsPanel({
  alerts,
  sensors,
  session,
  onAcknowledged,
  onSelectSensor,
}: {
  alerts: OpenAlerts
  sensors: Sensor[]
  /** null = API em modo aberto (segurança desligada): o nome do operador é digitado. */
  session: Session | null
  onAcknowledged: (a: AlertDto) => void
  onSelectSensor: (sensorId: string) => void
}) {
  const [user, setUser] = useState(() => localStorage.getItem('sensorhub.user') ?? '')
  const [error, setError] = useState<string | null>(null)
  const names = useMemo(() => new Map(sensors.map((s) => [s.id, s.name])), [sensors])
  const list = sortAlerts(alerts)

  async function ack(alert: AlertDto) {
    if (!session && !user.trim()) return setError('Informe seu nome para reconhecer alertas.')
    if (!session) localStorage.setItem('sensorhub.user', user.trim())
    try {
      setError(null)
      onAcknowledged(await acknowledge(alert.id, session ? undefined : user.trim()))
    } catch (e) {
      setError((e as Error).message)
    }
  }

  return (
    <aside className="alerts" aria-label="Alertas abertos">
      <h2>
        Alertas abertos <span className="count">{list.length}</span>
      </h2>
      {!session && <input className="user" placeholder="seu nome (para reconhecer)" value={user} onChange={(e) => setUser(e.target.value)} />}
      {error && <p className="error">{error}</p>}
      {list.length === 0 && <p className="muted">Nenhum alerta aberto.</p>}
      <ul>
        {list.map((a) => (
          <li key={a.id} className={`alert sev-${a.severity.toLowerCase()}`}>
            <div className="alert-head">
              <strong>{a.severity}</strong>
              <time>{new Date(a.firedAt).toLocaleTimeString()}</time>
            </div>
            <button className="link" onClick={() => onSelectSensor(a.sensorId)}>
              {names.get(a.sensorId) ?? a.sensorId.slice(0, 8)}
            </button>
            <p>{a.message}</p>
            {a.status === 'Acknowledged' ? (
              <p className="muted">reconhecido por {a.acknowledgedBy}</p>
            ) : (
              // esconder é só cortesia: quem barra o Viewer de verdade é a API (403)
              (!session || hasRole(session, 'Operator')) && (
                <button className="ghost" onClick={() => void ack(a)}>
                  Reconhecer
                </button>
              )
            )}
          </li>
        ))}
      </ul>
    </aside>
  )
}

export function Toasts({ toasts, onDismiss }: { toasts: Toast[]; onDismiss: (id: string) => void }) {
  useEffect(() => {
    if (toasts.length === 0) return
    const timer = setTimeout(() => onDismiss(toasts[0].id), 6000)
    return () => clearTimeout(timer)
  }, [toasts, onDismiss])

  return (
    <div className="toasts" role="status" aria-live="polite">
      {toasts.map((t) => (
        <div key={t.id} className={`toast sev-${t.alert.severity.toLowerCase()}`} onClick={() => onDismiss(t.id)}>
          <strong>{t.alert.severity}</strong> {t.alert.message}
        </div>
      ))}
    </div>
  )
}

export function LoginForm({ onSuccess }: { onSuccess: () => void }) {
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

  async function submit(e: FormEvent) {
    e.preventDefault()
    setBusy(true)
    setError(null)
    try {
      await login(email.trim(), password)
      setPassword('')
      onSuccess()
    } catch (err) {
      setError((err as Error).message)
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="login-page">
      <form className="login" onSubmit={(e) => void submit(e)} aria-label="Entrar">
        <h1>SensorHub</h1>
        <p className="muted">Entre para acompanhar a telemetria.</p>
        <label>
          E-mail
          <input type="email" autoComplete="username" required value={email} onChange={(e) => setEmail(e.target.value)} />
        </label>
        <label>
          Senha
          <input type="password" autoComplete="current-password" required value={password} onChange={(e) => setPassword(e.target.value)} />
        </label>
        {error && <p className="error" role="alert">{error}</p>}
        <button type="submit" disabled={busy}>{busy ? 'Entrando…' : 'Entrar'}</button>
      </form>
    </div>
  )
}
