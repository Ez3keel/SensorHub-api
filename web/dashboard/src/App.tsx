import { useEffect, useMemo, useRef, useState } from 'react'
import { listSensors, type Sensor } from './api'
import { getSession, logout, onSessionChange, restoreSession, type Session } from './auth'
import { AlertsPanel, ConnectionBadge, GroupPicker, LoginForm, SensorDetail, SensorGrid, Toasts } from './components'
import { useTelemetry } from './useTelemetry'

type Gate = 'checking' | 'login' | 'ready'

/**
 * Porta de entrada. Ordem: (1) tenta restaurar a sessão pelo refresh token; (2) se não há, sonda a API sem token: 200 = modo
 * aberto (segurança desligada, como no desenvolvimento simples), 401 = exige login. O dashboard só monta depois disso, então
 * nenhuma conexão SignalR nem chamada REST sai sem credencial.
 */
export default function App() {
  const [gate, setGate] = useState<Gate>('checking')
  const [session, setSession] = useState<Session | null>(getSession())

  useEffect(() => {
    let alive = true
    void (async () => {
      const restored = await restoreSession()
      if (!alive) return
      if (restored) return setGate('ready')
      const probe = await fetch('/api/sensors?take=1').catch(() => null)
      if (alive) setGate(probe?.status === 401 ? 'login' : 'ready')
    })()

    // sessão encerrada (refresh recusado, logout): volta para o login
    const off = onSessionChange((s) => {
      setSession(s)
      if (s === null) setGate('login')
    })
    return () => {
      alive = false
      off()
    }
  }, [])

  if (gate === 'checking') return <p className="muted banner">Carregando…</p>
  if (gate === 'login') return <LoginForm onSuccess={() => { setSession(getSession()); setGate('ready') }} />
  return <Dashboard session={session} />
}

function Dashboard({ session }: { session: Session | null }) {
  const [sensors, setSensors] = useState<Sensor[]>([])
  const [loadError, setLoadError] = useState<string | null>(null)
  const [group, setGroup] = useState<string | null>(null)
  const [selected, setSelected] = useState<string | null>(null)
  const [nowMs, setNowMs] = useState(() => Date.now())
  const watched = useRef<string | null>(null)

  const telemetry = useTelemetry(group, sensors)

  useEffect(() => {
    listSensors()
      .then((list) => {
        setSensors(list)
        setGroup((g) => g ?? [...new Set(list.map((s) => s.group))].sort()[0] ?? null)
      })
      .catch((e: Error) => setLoadError(e.message))
  }, [])

  // relógio para o indicador "sem leitura recente"
  useEffect(() => {
    const id = setInterval(() => setNowMs(Date.now()), 1000)
    return () => clearInterval(id)
  }, [])

  const groups = useMemo(() => [...new Set(sensors.map((s) => s.group))].sort(), [sensors])
  const visible = useMemo(() => sensors.filter((s) => s.group === group), [sensors, group])
  const selectedSensor = useMemo(() => sensors.find((s) => s.id === selected) ?? null, [sensors, selected])

  // assina o sensor em detalhe (e solta o anterior)
  useEffect(() => {
    const previous = watched.current
    watched.current = selected
    void telemetry.watchSensor(selected, previous)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selected])

  return (
    <div className="app">
      <header className="topbar">
        <h1>SensorHub</h1>
        <span className="muted">{sensors.length} sensores</span>
        <ConnectionBadge status={telemetry.status} />
        {session && (
          <span className="who">
            {session.email} <span className="role">{session.role}</span>
            <button className="ghost" onClick={() => void logout()}>Sair</button>
          </span>
        )}
      </header>

      {loadError && <p className="error banner">Não foi possível carregar os sensores: {loadError}</p>}
      {!loadError && sensors.length === 0 && <p className="muted banner">Nenhum sensor cadastrado. Use o simulador: <code>--mode register</code>.</p>}

      <main className="layout">
        <div className="main">
          <GroupPicker groups={groups} selected={group} onSelect={(g) => { setGroup(g); setSelected(null) }} />
          {selectedSensor && (
            <SensorDetail sensor={selectedSensor} detailReadings={telemetry.detailReadings} onClose={() => setSelected(null)} />
          )}
          <SensorGrid
            sensors={visible}
            live={telemetry.live}
            alerts={telemetry.alerts}
            selectedSensor={selected}
            onSelect={setSelected}
            nowMs={nowMs}
          />
        </div>
        <AlertsPanel alerts={telemetry.alerts} sensors={sensors} session={session} onAcknowledged={telemetry.acknowledged} onSelectSensor={(id) => {
          const target = sensors.find((s) => s.id === id)
          if (target) setGroup(target.group)
          setSelected(id)
        }} />
      </main>

      <Toasts toasts={telemetry.toasts} onDismiss={telemetry.dismissToast} />
    </div>
  )
}
