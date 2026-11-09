// Unit test for execDetachedScriptStep's retry/resume behavior. Fully mocks
// @kubernetes/client-node so this needs no real cluster/kubeconfig and never
// touches the network: only the retry/offset-tracking logic is exercised.
//
// Unlike execPodStep, a connection drop here is retried *regardless* of
// whether output was already seen, because re-invoking the launcher script
// (see writeDetachedLauncherScript) never replays the user's command — it
// only resumes tailing a process that is already running detached from the
// exec session that started it.
//
// A dead connection is detected via WebSocketHeartbeat (ping/pong over the
// exec websocket itself), not by inferring it from a lack of stdout/stderr
// bytes — a silent-but-healthy step must never be mistaken for a dropped
// connection.

import { EventEmitter } from 'events'

const mockExecFn = jest.fn()

jest.mock('@actions/core', () => ({
  debug: jest.fn(),
  warning: jest.fn(),
  error: jest.fn(),
  info: jest.fn()
}))

jest.mock('@kubernetes/client-node', () => ({
  KubeConfig: jest.fn().mockImplementation(() => ({
    loadFromDefault: jest.fn(),
    makeApiClient: jest.fn().mockReturnValue({}),
    getContexts: jest.fn().mockReturnValue([])
  })),
  CoreV1Api: jest.fn(),
  BatchV1Api: jest.fn(),
  AuthorizationV1Api: jest.fn(),
  Exec: jest.fn().mockImplementation(() => ({ exec: mockExecFn }))
}))

import * as core from '@actions/core'
import { execDetachedScriptStep } from '../src/k8s'

// Minimal WebSocket test double — exec.exec() resolves with this, and
// WebSocketHeartbeat attaches ping/pong/close listeners to it.
class MockWebSocket extends EventEmitter {
  readyState = 1 // OPEN
  ping = jest.fn()
  close = jest.fn()
}

// args passed to exec.exec(namespace, pod, container, command, stdout,
// stderr, stdin, tty, callback)
function commandArg(args: unknown[]): string[] {
  return args[3] as string[]
}
function callbackArg(
  args: unknown[]
): (resp: { status: string; code?: number; message?: string }) => void {
  return args[8] as (resp: {
    status: string
    code?: number
    message?: string
  }) => void
}
function stdoutArg(args: unknown[]): NodeJS.WritableStream {
  return args[4] as NodeJS.WritableStream
}

describe('execDetachedScriptStep', () => {
  beforeEach(() => {
    process.env.ACTIONS_RUNNER_KUBERNETES_NAMESPACE = 'test-ns'
    // Small but not instant, so the near-zero-delay resp callbacks used by
    // most tests settle before any ping ever fires.
    process.env.ACTIONS_RUNNER_HEARTBEAT_PERIOD_MS = '1000'
    process.env.ACTIONS_RUNNER_HEARTBEAT_DEADLINE_MS = '2000'
    jest.useFakeTimers()
    mockExecFn.mockReset()
  })

  afterEach(() => {
    jest.useRealTimers()
  })

  it('runs the launcher once and resolves on a clean success', async () => {
    mockExecFn.mockImplementationOnce((...args) => {
      const out = stdoutArg(args)
      out.write('hello from the job\n')
      const callback = callbackArg(args)
      setTimeout(() => callback({ status: 'Success', code: 0 }), 0)
      return Promise.resolve(new MockWebSocket())
    })

    const promise = execDetachedScriptStep(
      '/__w/_temp/launcher.sh',
      'pod-1',
      'job'
    )
    await jest.advanceTimersByTimeAsync(1000)

    await expect(promise).resolves.toBeUndefined()
    expect(mockExecFn).toHaveBeenCalledTimes(1)
    expect(commandArg(mockExecFn.mock.calls[0])).toEqual([
      'sh',
      '/__w/_temp/launcher.sh',
      '0'
    ])
  })

  it('resumes from the correct byte offset after a mid-stream disconnect', async () => {
    mockExecFn
      .mockImplementationOnce((...args) => {
        const out = stdoutArg(args)
        out.write('0123456789') // 10 bytes seen before the drop
        const callback = callbackArg(args)
        setTimeout(
          () =>
            callback({
              status: 'Failure',
              message: 'No agent available: dial tcp timeout'
            }),
          0
        )
        return Promise.resolve(new MockWebSocket())
      })
      .mockImplementationOnce((...args) => {
        const callback = callbackArg(args)
        setTimeout(() => callback({ status: 'Success', code: 0 }), 0)
        return Promise.resolve(new MockWebSocket())
      })

    const promise = execDetachedScriptStep(
      '/__w/_temp/launcher.sh',
      'pod-1',
      'job'
    )
    await jest.advanceTimersByTimeAsync(30000)

    await expect(promise).resolves.toBeUndefined()
    expect(mockExecFn).toHaveBeenCalledTimes(2)
    expect(commandArg(mockExecFn.mock.calls[0])[2]).toBe('0')
    expect(commandArg(mockExecFn.mock.calls[1])[2]).toBe('10')
    expect(core.warning).toHaveBeenCalledWith(
      expect.stringContaining('resuming from byte offset 10')
    )
  })

  it('treats a connection with no pong response as a drop via the heartbeat timeout', async () => {
    mockExecFn
      .mockImplementationOnce(() => Promise.resolve(new MockWebSocket())) // OPEN, never emits 'pong', resp callback never fires
      .mockImplementationOnce((...args) => {
        const callback = callbackArg(args)
        setTimeout(() => callback({ status: 'Success', code: 0 }), 0)
        return Promise.resolve(new MockWebSocket())
      })

    const promise = execDetachedScriptStep(
      '/__w/_temp/launcher.sh',
      'pod-1',
      'job'
    )
    // period=1000 (first ping) + deadline=2000 (from that ping) = 3000ms
    await jest.advanceTimersByTimeAsync(5000)

    await expect(promise).resolves.toBeUndefined()
    expect(mockExecFn).toHaveBeenCalledTimes(2)
    expect(core.warning).toHaveBeenCalledWith(
      expect.stringContaining('heartbeat timeout')
    )
  })

  it('does not retry a genuine nonzero-exit failure from the script', async () => {
    mockExecFn.mockImplementation((...args) => {
      const callback = callbackArg(args)
      setTimeout(
        () =>
          callback({
            status: 'Failure',
            message: 'command terminated with exit code 1'
          }),
        0
      )
      return Promise.resolve(new MockWebSocket())
    })

    const promise = execDetachedScriptStep(
      '/__w/_temp/launcher.sh',
      'pod-1',
      'job'
    )
    const assertion = expect(promise).rejects.toBe(
      'command terminated with exit code 1'
    )
    await jest.advanceTimersByTimeAsync(1000)
    await assertion
    expect(mockExecFn).toHaveBeenCalledTimes(1)
  })

  it('gives up after the max attempts and throws', async () => {
    mockExecFn.mockImplementation(() =>
      Promise.reject(new Error('No agent available: dial tcp timeout'))
    )

    const promise = execDetachedScriptStep(
      '/__w/_temp/launcher.sh',
      'pod-1',
      'job'
    )
    const assertion = expect(promise).rejects.toThrow(/No agent available/)
    await jest.advanceTimersByTimeAsync(150000)
    await assertion
    expect(mockExecFn).toHaveBeenCalledTimes(10)
  })
})
