import { useEffect, useMemo, useRef, useState } from 'react'
import { listSensors, type Sensor } from './api'
import { AlertsPanel, ConnectionBadge, GroupPicker, SensorDetail, SensorGrid, Toasts } from './components'
import { useTelemetry } from './useTelemetry'

export default function App() {
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
        <AlertsPanel alerts={telemetry.alerts} sensors={sensors} onAcknowledged={telemetry.acknowledged} onSelectSensor={(id) => {
          const target = sensors.find((s) => s.id === id)
          if (target) setGroup(target.group)
          setSelected(id)
        }} />
      </main>

      <Toasts toasts={telemetry.toasts} onDismiss={telemetry.dismissToast} />
    </div>
  )
}
