import { HubConnection, HubConnectionBuilder, HubConnectionState, LogLevel } from '@microsoft/signalr'
import type { AlertPush, ReadingPush } from './telemetryState'

export type ConnectionStatus = 'connecting' | 'connected' | 'reconnecting' | 'disconnected'

export interface TelemetryEvents {
  onReadings: (readings: ReadingPush[]) => void
  onReading: (reading: ReadingPush) => void
  onAlert: (alert: AlertPush) => void
  onStatus: (status: ConnectionStatus) => void
}

/**
 * Conexão com o hub de tempo real. Guarda as assinaturas ativas e as REFAZ depois de uma reconexão: no SignalR os grupos
 * pertencem à conexão, então uma queda de rede perderia silenciosamente todas as assinaturas.
 */
export class TelemetryClient {
  private readonly connection: HubConnection
  private readonly groups = new Set<string>()
  private readonly sensors = new Set<string>()
  private alerts = false
  /** Um cliente parado NUNCA volta a conectar (senão, um efeito desmontado ressuscitaria como "zumbi" e duplicaria eventos). */
  private stopped = false

  constructor(private readonly events: TelemetryEvents, url = '/hubs/telemetry') {
    this.connection = new HubConnectionBuilder()
      .withUrl(url)
      .withAutomaticReconnect([0, 1000, 2000, 5000, 10000, 30000])
      .configureLogging(LogLevel.Warning)
      .build()

    // Um cliente parado fica mudo: eventos tardios dele não podem sobrescrever o estado de quem o substituiu.
    this.connection.on('readings', (list: ReadingPush[]) => !this.stopped && events.onReadings(list))
    this.connection.on('reading', (r: ReadingPush) => !this.stopped && events.onReading(r))
    this.connection.on('alert', (a: AlertPush) => !this.stopped && events.onAlert(a))

    this.connection.onreconnecting(() => this.emitStatus('reconnecting'))
    this.connection.onreconnected(async () => {
      await this.resubscribe()
      this.emitStatus('connected')
    })
    this.connection.onclose(() => this.emitStatus('disconnected'))
  }

  private emitStatus(status: ConnectionStatus): void {
    if (!this.stopped) this.events.onStatus(status)
  }

  async start(): Promise<void> {
    if (this.stopped) return
    this.emitStatus('connecting')
    try {
      await this.connection.start()
      if (this.stopped) return await this.connection.stop()
      await this.resubscribe()
      this.emitStatus('connected')
    } catch {
      if (this.stopped) return
      this.emitStatus('disconnected')
      // o SignalR não tenta de novo se o primeiro start falha: agenda uma nova tentativa
      setTimeout(() => void this.start(), 5000)
    }
  }

  async stop(): Promise<void> {
    this.stopped = true
    await this.connection.stop()
  }

  async subscribeGroup(group: string): Promise<void> {
    this.groups.add(group)
    if (this.isConnected) await this.connection.invoke('SubscribeGroup', group)
  }

  async unsubscribeGroup(group: string): Promise<void> {
    this.groups.delete(group)
    if (this.isConnected) await this.connection.invoke('UnsubscribeGroup', group)
  }

  async subscribeSensor(sensorId: string): Promise<void> {
    this.sensors.add(sensorId)
    if (this.isConnected) await this.connection.invoke('SubscribeSensor', sensorId)
  }

  async unsubscribeSensor(sensorId: string): Promise<void> {
    this.sensors.delete(sensorId)
    if (this.isConnected) await this.connection.invoke('UnsubscribeSensor', sensorId)
  }

  async subscribeAlerts(): Promise<void> {
    this.alerts = true
    if (this.isConnected) await this.connection.invoke('SubscribeAlerts')
  }

  private get isConnected(): boolean {
    return this.connection.state === HubConnectionState.Connected
  }

  private async resubscribe(): Promise<void> {
    for (const g of this.groups) await this.connection.invoke('SubscribeGroup', g)
    for (const s of this.sensors) await this.connection.invoke('SubscribeSensor', s)
    if (this.alerts) await this.connection.invoke('SubscribeAlerts')
  }
}
