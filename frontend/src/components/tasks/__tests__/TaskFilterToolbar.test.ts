import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import TaskFilterToolbar from '../TaskFilterToolbar.vue'

describe('TaskFilterToolbar Component Tests', () => {
  const defaultProps = {
    filterStatus: 'all',
    filterTaskType: 'all',
    filterAccountId: null,
    searchQuery: '',
    statusFilterOptions: [
      { label: '全部状态', value: 'all' },
      { label: '运行中', value: 'running' },
    ],
    typeFilterOptions: [
      { label: '全部类型', value: 'all' },
      { label: '下载', value: 'download_link' },
    ],
    accountFilterOptions: [{ label: '全部账号', value: '' }],
    statusCounts: {
      running: 2,
      queued: 1,
      paused: 0,
      completed: 5,
      failed: 1,
      cancelled: 0,
    },
  }

  it('renders semantic data-testid interactive elements', () => {
    const wrapper = mount(TaskFilterToolbar, {
      props: defaultProps,
    })

    expect(wrapper.find('[data-testid="task-filter-status"]').exists()).toBe(true)
    expect(wrapper.find('[data-testid="task-filter-type"]').exists()).toBe(true)
    expect(wrapper.find('[data-testid="task-filter-account"]').exists()).toBe(true)
    expect(wrapper.find('[data-testid="task-search-input"]').exists()).toBe(true)
    expect(wrapper.find('[data-testid="create-task-btn"]').exists()).toBe(true)
  })

  it('emits create event when create button is clicked', async () => {
    const wrapper = mount(TaskFilterToolbar, {
      props: defaultProps,
    })

    const createBtn = wrapper.find('[data-testid="create-task-btn"]')
    await createBtn.trigger('click')

    expect(wrapper.emitted('create')).toBeTruthy()
    expect(wrapper.emitted('create')!.length).toBe(1)
  })

  it('emits pause-queue and reset events', async () => {
    const wrapper = mount(TaskFilterToolbar, {
      props: defaultProps,
    })

    const buttons = wrapper.findAll('button')
    const resetBtn = buttons.find((b) => b.text().includes('重置'))
    expect(resetBtn).toBeDefined()
    await resetBtn!.trigger('click')

    expect(wrapper.emitted('reset')).toBeTruthy()

    const pauseQueueBtn = buttons.find((b) => b.text().includes('暂停队列'))
    expect(pauseQueueBtn).toBeDefined()
    await pauseQueueBtn!.trigger('click')

    expect(wrapper.emitted('pause-queue')).toBeTruthy()
  })
})
