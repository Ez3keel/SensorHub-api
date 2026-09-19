// Cliente da API REST. Em dev o Vite faz proxy de /api para a API; em produção o nginx faz o mesmo.

export interface Sensor {
  id: string
  deviceId: string
  name: string
  metric: 'Temperature' | 'Humidity' | 'Vibration' | 'Pressure'
  unit: string
  group: string
  active: boolean
}

export interface LatestValue {
  sensorId: string
  timestamp: string
  value: number
  ageSeconds: number
}

export interface SeriesPoint {
  bucket: string
  avg: number
  min: number
  max: number
  count: number
}

export interface RawPoint {
  ts: string
  value: number
}

export type AlertStatus = 'Firing' | 'Acknowledged' | 'Resolved'
export type Severity = 'Info' | 'Warning' | 'Critical'

export interface AlertDto {
  id: string
  ruleId: string
  sensorId: string
  severity: Severity
  status: AlertStatus
  message: string
  triggerValue: number | null
  firedAt: string
  acknowledgedBy: string | null
  resolvedAt: string | null
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, {
    ...init,
    headers: { 'Content-Type': 'application/json', ...(init?.headers ?? {}) },
  })
  if (!response.ok) {
    let detail = `${response.status} ${response.statusText}`
    try {
      const problem = await response.json()
      if (problem?.detail) detail = problem.detail
    } catch {
      /* corpo não é JSON: mantém o status */
    }
    throw new Error(detail)
  }
  return response.status === 204 ? (undefined as T) : ((await response.json()) as T)
}

/** Carrega todos os sensores paginando (a API limita a página a 500). */
export async function listSensors(): Promise<Sensor[]> {
  const all: Sensor[] = []
  for (let skip = 0; ; skip += 500) {
    const page = await request<Sensor[]>(`/api/sensors?active=true&take=500&skip=${skip}`)
    all.push(...page)
    if (page.length < 500) return all
  }
}

/** Último valor de vários sensores; o servidor aceita até 200 ids por chamada. */
export async function latestValues(ids: string[]): Promise<LatestValue[]> {
  const chunks: string[][] = []
  for (let i = 0; i < ids.length; i += 200) chunks.push(ids.slice(i, i + 200))
  const pages = await Promise.all(chunks.map((c) => request<LatestValue[]>(`/api/sensors/latest?ids=${c.join(',')}`)))
  return pages.flat()
}

export async function series(sensorId: string, from: Date, to: Date): Promise<SeriesPoint[]> {
  const q = `from=${encodeURIComponent(from.toISOString())}&to=${encodeURIComponent(to.toISOString())}`
  return (await request<{ points: SeriesPoint[] }>(`/api/sensors/${sensorId}/series?${q}`)).points
}

export async function recentReadings(sensorId: string, minutes: number): Promise<RawPoint[]> {
  const to = new Date()
  const from = new Date(to.getTime() - minutes * 60_000)
  const q = `from=${encodeURIComponent(from.toISOString())}&to=${encodeURIComponent(to.toISOString())}&limit=1000`
  return (await request<{ points: RawPoint[] }>(`/api/sensors/${sensorId}/readings?${q}`)).points
}

export function openAlerts(): Promise<AlertDto[]> {
  return Promise.all([
    request<AlertDto[]>('/api/alerts?status=Firing&take=200'),
    request<AlertDto[]>('/api/alerts?status=Acknowledged&take=200'),
  ]).then(([firing, acknowledged]) => [...firing, ...acknowledged])
}

export function acknowledge(alertId: string, user: string): Promise<AlertDto> {
  return request<AlertDto>(`/api/alerts/${alertId}/acknowledge`, { method: 'POST', body: JSON.stringify({ user }) })
}
