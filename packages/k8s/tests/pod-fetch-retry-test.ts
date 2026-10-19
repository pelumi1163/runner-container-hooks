// Unit tests for the retry/backoff behavior added to createPod,
// getPodStatus, and waitForPodPhases. Fully mocks @kubernetes/client-node
// so this needs no real cluster/kubeconfig and never touches the network.

const mockCreateNamespacedPod = jest.fn()
const mockReadNamespacedPod = jest.fn()

jest.mock('@actions/core', () => ({
  debug: jest.fn(),
  warning: jest.fn(),
  error: jest.fn(),
  info: jest.fn()
}))

jest.mock('@kubernetes/client-node', () => {
  class ApiException extends Error {
    code: number
    body: unknown
    constructor(code: number, message: string, body: unknown) {
      super(message)
      this.code = code
      this.body = body
      Object.setPrototypeOf(this, ApiException.prototype)
    }
  }
  // Minimal constructible stand-ins — createPod only ever sets properties
  // on instances of these, never reads from them.
  class Stub {
    [key: string]: unknown
  }
  return {
    KubeConfig: jest.fn().mockImplementation(() => ({
      loadFromDefault: jest.fn(),
      makeApiClient: jest.fn().mockReturnValue({
        createNamespacedPod: mockCreateNamespacedPod,
        readNamespacedPod: mockReadNamespacedPod
      }),
      getContexts: jest.fn().mockReturnValue([])
    })),
    CoreV1Api: jest.fn(),
    BatchV1Api: jest.fn(),
    AuthorizationV1Api: jest.fn(),
    ApiException,
    V1Pod: Stub,
    V1ObjectMeta: Stub,
    V1PodSpec: Stub,
    V1LocalObjectReference: Stub
  }
})

import * as core from '@actions/core'
import { createPod, getPodStatus, waitForPodPhases } from '../src/k8s'
import { PodPhase } from '../src/k8s/utils'

describe('createPod retry/adoption on create failure', () => {
  beforeEach(() => {
    process.env.ACTIONS_RUNNER_KUBERNETES_NAMESPACE = 'test-ns'
    process.env.ACTIONS_RUNNER_POD_NAME = 'runner-pod-1'
    jest.useFakeTimers()
    mockCreateNamespacedPod.mockReset()
    mockReadNamespacedPod.mockReset()
    // getCurrentNodeName() reads the runner pod first, on every call.
    mockReadNamespacedPod.mockResolvedValue({ spec: { nodeName: 'node-1' } })
  })

  afterEach(() => {
    jest.useRealTimers()
  })

  it('retries a transient create failure and then succeeds', async () => {
    mockCreateNamespacedPod
      .mockRejectedValueOnce(new Error('connect ETIMEDOUT'))
      .mockResolvedValueOnce({ metadata: { name: 'job-pod-1' } })

    const promise = createPod(undefined, [], undefined, undefined)
    await jest.advanceTimersByTimeAsync(5000)

    const pod = await promise
    expect(pod.metadata?.name).toBe('job-pod-1')
    expect(mockCreateNamespacedPod).toHaveBeenCalledTimes(2)
  })

  it('adopts the existing pod on a 409 instead of failing or looping', async () => {
    const { ApiException } = jest.requireMock('@kubernetes/client-node') as {
      ApiException: new (code: number, message: string, body: unknown) => Error
    }
    mockCreateNamespacedPod.mockRejectedValueOnce(
      new ApiException(409, 'pods "job-pod-1" already exists', {
        reason: 'AlreadyExists'
      })
    )
    // Second readNamespacedPod call (after the first, for getCurrentNodeName)
    // is the adoption fetch for the job pod itself.
    mockReadNamespacedPod
      .mockResolvedValueOnce({ spec: { nodeName: 'node-1' } })
      .mockResolvedValueOnce({ metadata: { name: 'job-pod-1' } })

    const pod = await createPod(undefined, [], undefined, undefined)

    expect(pod.metadata?.name).toBe('job-pod-1')
    expect(mockCreateNamespacedPod).toHaveBeenCalledTimes(1)
    expect(core.warning).toHaveBeenCalledWith(
      expect.stringContaining('already exists')
    )
  })
})

describe('getPodStatus retry', () => {
  beforeEach(() => {
    process.env.ACTIONS_RUNNER_KUBERNETES_NAMESPACE = 'test-ns'
    jest.useFakeTimers()
    mockReadNamespacedPod.mockReset()
  })

  afterEach(() => {
    jest.useRealTimers()
  })

  it('retries a transient read failure and then succeeds', async () => {
    mockReadNamespacedPod
      .mockRejectedValueOnce(new Error('socket hang up'))
      .mockResolvedValueOnce({ status: { phase: 'Succeeded' } })

    const promise = getPodStatus('job-pod-1')
    await jest.advanceTimersByTimeAsync(5000)

    await expect(promise).resolves.toEqual({ phase: 'Succeeded' })
    expect(mockReadNamespacedPod).toHaveBeenCalledTimes(2)
  })
})

describe('waitForPodPhases retry', () => {
  beforeEach(() => {
    process.env.ACTIONS_RUNNER_KUBERNETES_NAMESPACE = 'test-ns'
    jest.useFakeTimers()
    mockReadNamespacedPod.mockReset()
  })

  afterEach(() => {
    jest.useRealTimers()
  })

  it('retries a transient fetch failure and then reaches the awaited phase', async () => {
    mockReadNamespacedPod
      .mockRejectedValueOnce(new Error('connect ETIMEDOUT'))
      .mockResolvedValueOnce({ status: { phase: 'Running' } })

    const promise = waitForPodPhases(
      'job-pod-1',
      new Set([PodPhase.RUNNING]),
      new Set([PodPhase.PENDING]),
      30
    )
    await jest.advanceTimersByTimeAsync(5000)

    await expect(promise).resolves.toBeUndefined()
    expect(mockReadNamespacedPod).toHaveBeenCalledTimes(2)
    expect(core.warning).toHaveBeenCalledWith(
      expect.stringContaining('failed to fetch pod status')
    )
  })

  it('still fails immediately on a genuinely unhealthy phase, without retrying', async () => {
    mockReadNamespacedPod.mockResolvedValue({ status: { phase: 'Failed' } })

    const promise = waitForPodPhases(
      'job-pod-1',
      new Set([PodPhase.RUNNING]),
      new Set([PodPhase.PENDING]),
      30
    )
    await expect(promise).rejects.toThrow(/unhealthy with phase status Failed/)
    expect(mockReadNamespacedPod).toHaveBeenCalledTimes(1)
  })
})
