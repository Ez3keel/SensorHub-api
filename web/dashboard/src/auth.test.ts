import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { acknowledge, UnauthorizedError, listSensors } from './api'
import { __resetForTests, getAccessToken, getSession, hasRole, login, logout, refresh, restoreSession, type Role } from './auth'

function sessionBody(overrides: Partial<{ access: string; refresh: string; role: Role; ttlMs: number }> = {}) {
  return {
    accessToken: overrides.access ?? 'access-1',
    accessTokenExpiresAt: new Date(Date.now() + (overrides.ttlMs ?? 15 * 60_000)).toISOString(),
    refreshToken: overrides.refresh ?? 'refresh-1',
    refreshTokenExpiresAt: new Date(Date.now() + 7 * 86_400_000).toISOString(),
    user: { id: 'u1', email: 'op@teste.local', role: overrides.role ?? 'Operator' },
  }
}

const json = (body: unknown, status = 200) =>
  Promise.resolve(new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } }))

let fetchMock: ReturnType<typeof vi.fn>

beforeEach(() => {
  __resetForTests()
  fetchMock = vi.fn()
  vi.stubGlobal('fetch', fetchMock)
})

afterEach(() => vi.unstubAllGlobals())

describe('login', () => {
  it('keeps the access token in memory and the refresh token in sessionStorage only', async () => {
    fetchMock.mockReturnValueOnce(json(sessionBody()))

    const session = await login('op@teste.local', 'senha')

    expect(session.role).toBe('Operator')
    expect(getSession()?.accessToken).toBe('access-1')
    expect(sessionStorage.getItem('sensorhub.refresh')).toBe('refresh-1')
    expect(JSON.stringify({ ...localStorage })).not.toContain('access-1') // nada de token em localStorage
    expect(JSON.stringify({ ...sessionStorage })).not.toContain('access-1')
  })

  it('reports a generic message for bad credentials and a distinct one for rate limiting', async () => {
    fetchMock.mockReturnValueOnce(json({ detail: 'x' }, 401))
    await expect(login('a@b.c', 'errada')).rejects.toThrow('E-mail ou senha inválidos.')

    fetchMock.mockReturnValueOnce(json({}, 429))
    await expect(login('a@b.c', 'errada')).rejects.toThrow('Muitas tentativas')
  })
})

describe('refresh', () => {
  it('is single-flight: concurrent callers share ONE refresh call (the token is single-use)', async () => {
    sessionStorage.setItem('sensorhub.refresh', 'refresh-1')
    fetchMock.mockReturnValue(json(sessionBody({ access: 'access-2', refresh: 'refresh-2' })))

    const [a, b, c] = await Promise.all([refresh(), refresh(), refresh()])

    expect(fetchMock).toHaveBeenCalledTimes(1)
    expect(a?.accessToken).toBe('access-2')
    expect(b).toBe(a)
    expect(c).toBe(a)
    expect(sessionStorage.getItem('sensorhub.refresh')).toBe('refresh-2') // rotacionou
  })

  it('a rejected refresh ends the session and clears the stored token', async () => {
    sessionStorage.setItem('sensorhub.refresh', 'refresh-1')
    const seen: (unknown)[] = []
    const { onSessionChange } = await import('./auth')
    onSessionChange((s) => seen.push(s))
    fetchMock.mockReturnValueOnce(json({}, 401))

    expect(await refresh()).toBeNull()

    expect(sessionStorage.getItem('sensorhub.refresh')).toBeNull()
    expect(seen).toEqual([null]) // o app é avisado e volta para o login
  })

  it('a network failure keeps the current session instead of logging out', async () => {
    sessionStorage.setItem('sensorhub.refresh', 'refresh-1')
    fetchMock.mockReturnValueOnce(json(sessionBody()))
    await login('op@teste.local', 'x')
    fetchMock.mockRejectedValueOnce(new TypeError('network'))

    const result = await refresh()

    expect(result?.accessToken).toBe('access-1')
    expect(sessionStorage.getItem('sensorhub.refresh')).not.toBeNull()
  })

  it('can refresh again after a previous refresh without a stored token (no stuck promise)', async () => {
    expect(await refresh()).toBeNull() // sem refresh token: retorno antecipado

    sessionStorage.setItem('sensorhub.refresh', 'refresh-9')
    fetchMock.mockReturnValueOnce(json(sessionBody({ access: 'novo' })))
    expect((await refresh())?.accessToken).toBe('novo')
  })
})

describe('restoreSession and access token', () => {
  it('restores from the refresh token without asking for the password', async () => {
    sessionStorage.setItem('sensorhub.refresh', 'refresh-1')
    fetchMock.mockReturnValueOnce(json(sessionBody({ role: 'Admin' })))

    expect((await restoreSession())?.role).toBe('Admin')
  })

  it('does not call the API when there is nothing to restore', async () => {
    expect(await restoreSession()).toBeNull()
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('renews an access token that is about to expire before handing it to SignalR', async () => {
    fetchMock.mockReturnValueOnce(json(sessionBody({ ttlMs: 10_000 }))) // expira em 10 s (< margem de 30 s)
    await login('op@teste.local', 'x')
    fetchMock.mockReturnValueOnce(json(sessionBody({ access: 'renovado', refresh: 'refresh-2' })))

    expect(await getAccessToken()).toBe('renovado')
  })

  it('logout clears everything locally even when the server call fails', async () => {
    fetchMock.mockReturnValueOnce(json(sessionBody()))
    await login('op@teste.local', 'x')
    fetchMock.mockRejectedValueOnce(new TypeError('offline'))

    await logout()

    expect(getSession()).toBeNull()
    expect(sessionStorage.getItem('sensorhub.refresh')).toBeNull()
  })
})

describe('hasRole (UI only)', () => {
  const as = (role: Role) => ({ accessToken: 't', accessTokenExpiresAt: 0, email: 'e', role })

  it('orders Viewer < Operator < Admin and denies without a session', () => {
    expect(hasRole(null, 'Viewer')).toBe(false)
    expect(hasRole(as('Viewer'), 'Operator')).toBe(false)
    expect(hasRole(as('Operator'), 'Operator')).toBe(true)
    expect(hasRole(as('Admin'), 'Operator')).toBe(true)
    expect(hasRole(as('Operator'), 'Admin')).toBe(false)
  })
})

describe('api client', () => {
  it('sends the bearer token', async () => {
    fetchMock.mockReturnValueOnce(json(sessionBody()))
    await login('op@teste.local', 'x')
    fetchMock.mockReturnValueOnce(json([]))

    await listSensors()

    const headers = fetchMock.mock.calls[1][1].headers as Record<string, string>
    expect(headers.Authorization).toBe('Bearer access-1')
  })

  it('on 401 refreshes once and repeats the call with the new token', async () => {
    fetchMock.mockReturnValueOnce(json(sessionBody()))
    await login('op@teste.local', 'x')
    fetchMock
      .mockReturnValueOnce(json({}, 401)) // token vencido
      .mockReturnValueOnce(json(sessionBody({ access: 'access-2', refresh: 'refresh-2' }))) // refresh
      .mockReturnValueOnce(json([])) // repetição

    await listSensors()

    const retry = fetchMock.mock.calls[3][1].headers as Record<string, string>
    expect(retry.Authorization).toBe('Bearer access-2')
  })

  it('still 401 after refreshing means the session is over', async () => {
    fetchMock.mockReturnValueOnce(json(sessionBody()))
    await login('op@teste.local', 'x')
    fetchMock
      .mockReturnValueOnce(json({}, 401))
      .mockReturnValueOnce(json(sessionBody({ access: 'access-2', refresh: 'refresh-2' })))
      .mockReturnValueOnce(json({}, 401))

    await expect(listSensors()).rejects.toBeInstanceOf(UnauthorizedError)
  })

  it('acknowledge does not send a user name when authenticated identity is used', async () => {
    fetchMock.mockReturnValueOnce(json({ id: 'a1' }))

    await acknowledge('a1')

    expect(JSON.parse(fetchMock.mock.calls[0][1].body as string)).toEqual({})
  })
})
