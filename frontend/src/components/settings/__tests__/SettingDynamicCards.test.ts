import { describe, expect, it, vi } from 'vitest'
import { mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import SettingDynamicCards from '../SettingDynamicCards.vue'
import type { SettingCard } from '@/utils/settingsMetadata'

describe('SettingDynamicCards', () => {
  it('renders both the transit explainer card and editable transit controls', async () => {
    setActivePinia(createPinia())

    const cards: SettingCard[] = [
      {
        key: 'transit',
        title: '⚡ 多 Bot 联合加速配置',
        items: [
          {
            key: 'enable_transit_acceleration',
            title: '启用多 Bot 联合中转加速',
            description: 'desc-1',
            meta: { type: 'switch' },
          },
          {
            key: 'transit_chat_id',
            title: '私有中转群组 ID',
            description: 'desc-2',
          },
          {
            key: 'transit_min_file_size_mb',
            title: '默认中转加速触发门槛 (MB)',
            description: 'desc-3',
          },
        ],
      },
    ]

    const wrapper = mount(SettingDynamicCards, {
      props: {
        cards,
        formValues: {
          enable_transit_acceleration: 'false',
          transit_chat_id: '-100123456',
          transit_min_file_size_mb: '20',
        },
      },
    })

    expect(wrapper.find('.transit-explainer-card').exists()).toBe(true)
    expect(wrapper.text()).toContain('enable_transit_acceleration')
    expect(wrapper.text()).toContain('transit_chat_id')
    expect(wrapper.text()).toContain('transit_min_file_size_mb')

    await wrapper.get('[role="switch"]').trigger('click')
    expect(wrapper.emitted('update:value')?.[0]).toEqual(['enable_transit_acceleration', 'true'])

    wrapper.unmount()
    vi.restoreAllMocks()
  })

  it('exposes item.title as an accessible name on switch, select, number, and text controls', () => {
    setActivePinia(createPinia())

    const cards: SettingCard[] = [
      {
        key: 'a11y',
        title: '无障碍校验卡片',
        items: [
          {
            key: 'enable_transit_acceleration',
            title: '启用开关标题',
            description: 'd1',
            meta: { type: 'switch' },
          },
          {
            key: 'pikpak_default_cleanup_mode',
            title: '下拉选择标题',
            description: 'd2',
            meta: {
              options: [
                { label: '保留', value: 'none' },
                { label: '归档', value: 'archive' },
              ],
            },
          },
          {
            key: 'max_global_concurrency',
            title: '数字输入标题',
            description: 'd3',
          },
          {
            key: 'transit_chat_id',
            title: '文本输入标题',
            description: 'd4',
          },
        ],
      },
    ]

    const wrapper = mount(SettingDynamicCards, {
      props: {
        cards,
        formValues: {
          enable_transit_acceleration: 'true',
          pikpak_default_cleanup_mode: 'archive',
          max_global_concurrency: '5',
          transit_chat_id: '-1001',
        },
      },
    })

    expect(wrapper.get('[role="switch"]').attributes('aria-label')).toBe('启用开关标题')
    expect(wrapper.get('.n-base-selection-label[tabindex]').attributes('aria-label')).toBe('下拉选择标题')
    const inputs = wrapper.findAll('input')
    expect(inputs.map((el) => el.attributes('aria-label'))).toEqual(['数字输入标题', '文本输入标题'])

    wrapper.unmount()
    vi.restoreAllMocks()
  })
})
