import executeScheduleWorker from '../execute-schedule.worker'
import type { ExecuteSchedulePayload } from '../execute-schedule.worker'
import { ScheduledJob } from '../../data/entities'
import { createQueue } from '@open-mercato/queue'

const mockQueue = {
  enqueue: jest.fn(),
  close: jest.fn(),
}

jest.mock('@open-mercato/queue', () => ({
  createQueue: jest.fn(() => mockQueue),
}), { virtual: true })

jest.mock('@open-mercato/shared/lib/redis/connection', () => ({
  getRedisUrlOrThrow: jest.fn(() => 'redis://queue'),
}))

const mockEmitSchedulerEvent = jest.fn()
jest.mock('../../events.js', () => ({
  emitSchedulerEvent: (...args: unknown[]) => mockEmitSchedulerEvent(...args),
}))

const mockCommandBusInstance = {
  execute: jest.fn(),
}

jest.mock('@open-mercato/shared/lib/commands', () => ({
  CommandBus: jest.fn().mockImplementation(() => mockCommandBusInstance),
}))

type MockEntityManager = {
  findOne: jest.Mock
  flush: jest.Mock
}

function createSchedule(overrides: Partial<ScheduledJob> = {}): ScheduledJob {
  return {
    id: 'schedule-1',
    name: 'Data sync: test products import',
    scopeType: 'organization',
    tenantId: 'tenant-1',
    organizationId: 'org-1',
    isEnabled: true,
    requireFeature: 'data_sync.run',
    targetType: 'queue',
    targetQueue: 'data-sync-scheduled',
    targetPayload: {
      scheduleId: 'sync-schedule-1',
      scope: {
        tenantId: 'tenant-1',
        organizationId: 'org-1',
      },
    },
    scheduleType: 'cron',
    scheduleValue: '*/2 * * * *',
    timezone: 'UTC',
    nextRunAt: new Date(),
    createdAt: new Date(),
    updatedAt: new Date(),
    sourceType: 'module',
    sourceModule: 'data_sync',
    ...overrides,
  } as ScheduledJob
}

function createJob(overrides: Partial<ExecuteSchedulePayload> = {}) {
  return {
    payload: {
      scheduleId: 'schedule-1',
      tenantId: 'tenant-1',
      organizationId: 'org-1',
      scopeType: 'organization',
      ...overrides,
    },
  }
}

function createContext(em: MockEntityManager, rbacService: { tenantHasFeature: jest.Mock }) {
  return {
    jobId: 'job-1',
    attemptNumber: 1,
    resolve: jest.fn((name: string) => {
      if (name === 'em') return em
      if (name === 'rbacService') return rbacService
      throw new Error(`Unexpected dependency: ${name}`)
    }),
  }
}

describe('executeScheduleWorker', () => {
  const originalQueueStrategy = process.env.QUEUE_STRATEGY

  beforeEach(() => {
    jest.clearAllMocks()
    process.env.QUEUE_STRATEGY = 'async'
    mockQueue.enqueue.mockResolvedValue('target-job-1')
    mockQueue.close.mockResolvedValue(undefined)
  })

  afterEach(() => {
    process.env.QUEUE_STRATEGY = originalQueueStrategy
  })

  it('checks required features with organization scope before enqueueing the target job', async () => {
    const schedule = createSchedule()
    const em: MockEntityManager = {
      findOne: jest.fn().mockResolvedValue(schedule),
      flush: jest.fn().mockResolvedValue(undefined),
    }
    const rbacService = {
      tenantHasFeature: jest.fn().mockResolvedValue(true),
    }
    const ctx = createContext(em, rbacService)

    await executeScheduleWorker(createJob() as never, ctx as never)

    expect(rbacService.tenantHasFeature).toHaveBeenCalledWith(
      'tenant-1',
      'data_sync.run',
      { organizationId: 'org-1' },
    )
    expect(createQueue).toHaveBeenCalledWith('data-sync-scheduled', 'async', {
      connection: { url: 'redis://queue' },
    })
    expect(mockQueue.enqueue).toHaveBeenCalledWith(expect.objectContaining({
      scheduleId: 'sync-schedule-1',
      tenantId: 'tenant-1',
      organizationId: 'org-1',
      _idempotencyKey: expect.stringMatching(/^scheduler-schedule-1-\d+$/),
    }))
    expect(schedule.lastRunAt).toBeInstanceOf(Date)
    expect(em.flush).toHaveBeenCalled()
    expect(mockEmitSchedulerEvent).toHaveBeenCalledWith(
      'scheduler.job.completed',
      expect.objectContaining({
        id: 'schedule-1',
        queueJobId: 'target-job-1',
        queueName: 'data-sync-scheduled',
      }),
    )
  })

  it('skips feature-gated schedules without enqueueing when the tenant lacks the feature', async () => {
    const schedule = createSchedule()
    const em: MockEntityManager = {
      findOne: jest.fn().mockResolvedValue(schedule),
      flush: jest.fn().mockResolvedValue(undefined),
    }
    const rbacService = {
      tenantHasFeature: jest.fn().mockResolvedValue(false),
    }
    const ctx = createContext(em, rbacService)

    await executeScheduleWorker(createJob() as never, ctx as never)

    expect(rbacService.tenantHasFeature).toHaveBeenCalledWith(
      'tenant-1',
      'data_sync.run',
      { organizationId: 'org-1' },
    )
    expect(mockQueue.enqueue).not.toHaveBeenCalled()
    expect(mockQueue.close).not.toHaveBeenCalled()
    expect(em.flush).not.toHaveBeenCalled()
    expect(schedule.lastRunAt).toBeUndefined()
    expect(mockEmitSchedulerEvent).toHaveBeenCalledWith(
      'scheduler.job.skipped',
      expect.objectContaining({
        id: 'schedule-1',
        reason: 'Feature not enabled: data_sync.run',
      }),
    )
  })
})
