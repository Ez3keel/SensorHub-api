// Sessão do dashboard: login, renovação rotativa do token e papel do usuário.
//
// Onde ficam os tokens (decisão consciente):
//  - access token (curto, ~15 min): SÓ em memória. Não sobrevive a um F5, mas também não fica exposto em storage.
//  - refresh token (longo, rotativo): em sessionStorage. Sobrevive ao F5 dentro da aba e some ao fechá-la. Um XSS
//    conseguiria lê-lo, mas ele é de uso único (a API detecta o reuso e revoga a sessão inteira) e não há localStorage.
//    A alternativa mais forte é um cookie HttpOnly emitido pela API (exige CSRF + same-site); fica como evolução.

export type Role = 'Viewer' | 'Operator' | 'Admin'

export interface Session {
  accessToken: string
  accessTokenExpiresAt: number // epoch ms
  email: string
  role: Role
}

interface SessionResponse {
  accessToken: string
  accessTokenExpiresAt: string
  refreshToken: string
  user: { email: string; role: Role }
}

const REFRESH_KEY = 'sensorhub.refresh'
const ROLE_RANK: Record<Role, number> = { Viewer: 0, Operator: 1, Admin: 2 }

let session: Session | null = null
let refreshing: Promise<Session | null> | null = null
const listeners = new Set<(s: Session | null) => void>()

function storage(): Storage | null {
  try {
    return globalThis.sessionStorage ?? null
  } catch {
    return null // modo privado / storage bloqueado: o app funciona, só não sobrevive ao F5
  }
}

function publish(next: Session | null): void {
  session = next
  for (const l of listeners) l(next)
}

/** Só para a INTERFACE (esconder o que o usuário não pode usar). Quem autoriza de verdade é sempre o servidor. */
export function hasRole(current: Session | null, minimum: Role): boolean {
  return current !== null && ROLE_RANK[current.role] >= ROLE_RANK[minimum]
}

export function getSession(): Session | null {
  return session
}

export function onSessionChange(listener: (s: Session | null) => void): () => void {
  listeners.add(listener)
  return () => listeners.delete(listener)
}

function fromResponse(body: SessionResponse): Session {
  storage()?.setItem(REFRESH_KEY, body.refreshToken)
  return {
    accessToken: body.accessToken,
    accessTokenExpiresAt: Date.parse(body.accessTokenExpiresAt),
    email: body.user.email,
    role: body.user.role,
  }
}

export async function login(email: string, password: string): Promise<Session> {
  const response = await fetch('/api/auth/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ email, password }),
  })
  if (response.status === 429) throw new Error('Muitas tentativas. Aguarde um instante e tente de novo.')
  if (!response.ok) throw new Error('E-mail ou senha inválidos.')
  const next = fromResponse((await response.json()) as SessionResponse)
  publish(next)
  return next
}

/**
 * Renova a sessão com o refresh token. É "single-flight": várias requisições que tomam 401 ao mesmo tempo compartilham UMA
 * renovação. Sem isso, a segunda usaria o mesmo refresh token já consumido, a API leria isso como roubo e derrubaria a sessão.
 */
export function refresh(): Promise<Session | null> {
  if (refreshing) return refreshing

  const run = async (): Promise<Session | null> => {
    const token = storage()?.getItem(REFRESH_KEY)
    if (!token) return endSession()
    try {
      const response = await fetch('/api/auth/refresh', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ refreshToken: token }),
      })
      if (!response.ok) return endSession() // token inválido, expirado ou reutilizado
      const next = fromResponse((await response.json()) as SessionResponse)
      publish(next)
      return next
    } catch {
      return session // falha de rede: mantém o que já temos, não desloga por causa de um soluço
    }
  }

  // o .finally cobre TODOS os caminhos (inclusive o de retorno antecipado sem refresh token)
  refreshing = run().finally(() => {
    refreshing = null
  })
  return refreshing
}

function endSession(): null {
  storage()?.removeItem(REFRESH_KEY)
  publish(null)
  return null
}

export async function logout(): Promise<void> {
  const token = storage()?.getItem(REFRESH_KEY)
  endSession()
  if (!token) return
  try {
    await fetch('/api/auth/logout', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ refreshToken: token }),
    })
  } catch {
    /* já saímos localmente; o refresh token expira sozinho no servidor */
  }
}

/** Ao abrir o app: se há refresh token guardado, tenta restaurar a sessão sem pedir a senha. */
export async function restoreSession(): Promise<Session | null> {
  return storage()?.getItem(REFRESH_KEY) ? refresh() : null
}

/** Token válido para uso AGORA (renova antes de expirar). Usado pelo SignalR a cada (re)conexão. */
export async function getAccessToken(): Promise<string> {
  if (session && session.accessTokenExpiresAt - Date.now() > 30_000) return session.accessToken
  return (await refresh())?.accessToken ?? ''
}

/** Só para testes. */
export function __resetForTests(): void {
  session = null
  refreshing = null
  listeners.clear()
  storage()?.removeItem(REFRESH_KEY)
}
