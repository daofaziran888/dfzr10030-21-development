<template>
  <section class="run-panel" data-testid="validator-runs">
    <header class="run-head">
      <div>
        <h3>校验运行包</h3>
        <p class="run-desc">
          按校验字段、采样比例和阻断级别执行一次校验；运行包与异常样本清单分开保存，异常样本都能回到本次采样范围和阻断规则。
        </p>
      </div>
      <div class="run-actions">
        <button class="btn" data-testid="refresh-runs" type="button" @click="loadRuns">刷新运行包</button>
        <button class="btn" data-testid="export-runs" type="button" @click="exportRuns">导出运行包</button>
      </div>
    </header>

    <form class="run-form" data-testid="run-form" @submit.prevent="createRun">
      <label>
        <span>规则范围</span>
        <select v-model="form.scope" data-testid="run-scope">
          <option value="仅已启用规则">仅已启用规则</option>
          <option value="全部规则">全部规则</option>
        </select>
      </label>
      <label>
        <span>指定规则编号（可选）</span>
        <input v-model="form.ruleCodes" data-testid="run-rule-codes" placeholder="如 VALI-0002，多个用逗号分隔" />
      </label>
      <label>
        <span>采样比例</span>
        <input v-model="form.ratio" data-testid="run-ratio" placeholder="按规则配置 / 50% / 0.5" />
      </label>
      <label>
        <span>本次样本来源</span>
        <select v-model="form.source" data-testid="run-source">
          <option value="pool">采集结果样本池（12 条）</option>
          <option value="empty">本次空采样（没有采集结果）</option>
        </select>
      </label>
      <button class="btn primary" data-testid="create-run" type="submit" :disabled="creating">
        {{ creating ? '执行中…' : '执行校验并生成运行包' }}
      </button>
    </form>

    <p v-if="message" class="run-message" :class="{ error: errorMessage }" data-testid="run-message">
      {{ message }}
    </p>

    <table class="data-table">
      <thead>
        <tr>
          <th>运行包编号</th>
          <th>创建时间</th>
          <th>数据版本</th>
          <th>采样比例</th>
          <th>采样数量</th>
          <th>异常样本数</th>
          <th>阻断数量</th>
          <th>整体结论</th>
          <th>下游阻断</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="run in runs"
          :key="run.id"
          :data-testid="`run-row-${run.id}`"
          :data-conclusion="run['整体结论']"
          :class="{ selected: selected?.id === run.id }"
        >
          <td>{{ run['运行包编号'] }}</td>
          <td>{{ run['创建时间'] }}</td>
          <td>{{ run['数据版本'] }}</td>
          <td>{{ run['采样比例'] }}</td>
          <td>{{ run['采样数量'] }}</td>
          <td>{{ run['异常样本数'] }}</td>
          <td>{{ run['阻断数量'] }}</td>
          <td>
            <span class="conclusion" :data-conclusion="run['整体结论']">{{ run['整体结论'] }}</span>
          </td>
          <td>{{ run['下游阻断'] ? '是' : '否' }}</td>
          <td class="row-actions">
            <button class="link" type="button" :data-testid="`view-run-${run.id}`" @click="viewRun(run.id)">
              查看运行包
            </button>
            <button class="link" type="button" :data-testid="`relay-run-${run.id}`" @click="relayRun(run.id)">
              监控核对
            </button>
          </td>
        </tr>
        <tr v-if="!runs.length">
          <td colspan="10" class="empty-state" data-testid="runs-empty">
            暂无校验运行包，可先选择采样比例并执行校验
          </td>
        </tr>
      </tbody>
    </table>

    <article v-if="selected" class="run-detail" data-testid="run-detail">
      <header class="run-detail-head">
        <div>
          <strong>{{ selected['运行包编号'] }}</strong>
          <span class="conclusion" :data-conclusion="selected['整体结论']">{{ selected['整体结论'] }}</span>
          <span class="run-meta">
            数据版本 {{ selected['数据版本'] }} · 口径 {{ selected['口径'] }} ·
            采样 {{ selected['采样数量'] }}/{{ selected['样本池数量'] }} 条 · 阻断 {{ selected['阻断数量'] }} 条
          </span>
        </div>
        <div class="run-actions">
          <button class="btn ghost" data-testid="export-run-abnormal" type="button" @click="exportAbnormal">
            导出异常样本清单
          </button>
          <button class="btn ghost" data-testid="close-run-detail" type="button" @click="selected = null">收起</button>
        </div>
      </header>

      <p v-if="!selected['整体通过'] && selected['阻断数量'] > 0" class="run-warning" data-testid="run-warning">
        阻断规则已命中 {{ selected['阻断数量'] }} 个样本，本次整体结论不能视为通过，下游读取被阻断。
      </p>
      <p v-else-if="selected['无法判定']" class="run-warning" data-testid="run-warning">
        本次采样为空或没有可执行规则，结论为无法判定；历史阻断依据仅可查看，本次仍阻断所有下游读取。
      </p>

      <div class="run-columns">
        <section>
          <h4>采样与规则执行</h4>
          <table class="data-table compact">
            <thead>
              <tr><th>规则编号</th><th>校验字段</th><th>阻断级别</th><th>采样比例</th><th>采样数</th><th>命中</th></tr>
            </thead>
            <tbody>
              <tr v-for="(rule, ruleIndex) in selected['规则结果']" :key="ruleIndex">
                <td>{{ rule['规则编号'] }}</td>
                <td>{{ rule['校验字段'] }}</td>
                <td>{{ rule['阻断级别'] }}</td>
                <td>{{ rule['采样比例'] }}</td>
                <td>{{ rule['采样数量'] }}</td>
                <td>{{ rule['命中数量'] }}</td>
              </tr>
              <tr v-if="!selected['规则结果'].length">
                <td colspan="6" class="empty-state">没有可执行的校验规则，本次无法判定</td>
              </tr>
            </tbody>
          </table>

          <h4>边界原因与数据版本</h4>
          <ul class="boundary-list" data-testid="run-boundary">
            <li v-for="(reason, index) in selected['边界原因']" :key="index">{{ reason }}</li>
            <li v-if="!selected['边界原因'].length">本次运行未触发边界，按当前口径记录</li>
          </ul>

          <template v-if="selected['历史阻断依据'].length">
            <h4>历史阻断依据（仅供查看）</h4>
            <ul class="boundary-list history" data-testid="run-history">
              <li v-for="item in selected['历史阻断依据']" :key="`${item['运行包编号']}-${item['样本编号']}`">
                {{ item['运行包编号'] }} · {{ item['样本编号'] }} · {{ item['校验规则'] }}（{{ item['阻断级别'] }}）：
                {{ item['异常原因'] }}；数据版本 {{ item['数据版本'] }}
              </li>
            </ul>
          </template>

          <template v-if="selected['监控回查']">
            <h4>监控回查记录</h4>
            <ul class="boundary-list" data-testid="run-monitor-record">
              <li>
                {{ selected['监控回查']['记录编号'] }} · 采集监控服务
                {{ selected['监控回查']['监控服务版本'] }} 核对运行包与异常样本清单 ·
                异常 {{ selected['监控回查']['异常样本数'] }} 条 · 阻断 {{ selected['监控回查']['阻断数量'] }} 条 ·
                当日告警计入 {{ selected['监控回查']['当日告警计入'] }}
              </li>
              <li>回查路径：采集监控 → 校验待办台账 → 来源运行包 {{ selected['运行包编号'] }}</li>
            </ul>
          </template>

          <template v-if="selected['规则差异'] && selected['规则差异'].length">
            <h4>监控回写差异</h4>
            <ul class="boundary-list">
              <li v-for="(item, index) in selected['规则差异']" :key="index">
                {{ item['规则编号'] }} · {{ item['状态'] }}：{{ item['原因'] }}
              </li>
            </ul>
          </template>
        </section>

        <section>
          <h4>异常样本清单（{{ abnormal.length }} 条）</h4>
          <table class="data-table compact">
            <thead>
              <tr><th>样本编号</th><th>字段</th><th>字段值</th><th>阻断规则</th><th>阻断级别</th><th>异常原因</th><th>采样依据</th></tr>
            </thead>
            <tbody>
              <tr
                v-for="(item, index) in abnormal"
                :key="`${item['样本编号']}-${item['规则编号']}-${index}`"
                :data-testid="`abnormal-${item['样本编号']}`"
                :data-blocking="item['是否阻断']"
              >
                <td>{{ item['样本编号'] }}</td>
                <td>{{ item['字段'] }}</td>
                <td>{{ item['字段值'] || '—' }}</td>
                <td>{{ item['规则编号'] }}</td>
                <td>{{ item['阻断级别'] }}{{ item['是否阻断'] ? '（阻断）' : '' }}</td>
                <td>{{ item['异常原因'] }}</td>
                <td class="scope-cell">{{ item['依据'] }}</td>
              </tr>
              <tr v-if="!abnormal.length">
                <td colspan="7" class="empty-state">本次采样范围内没有异常样本</td>
              </tr>
            </tbody>
          </table>
        </section>
      </div>
    </article>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { request } from '@/api/client'

type RunSummary = {
  id: number
  '运行包编号': string
  '创建时间': string
  '数据版本': string
  '口径': string
  '采样比例': string
  '采样数量': number
  '样本池数量': number
  '异常样本数': number
  '阻断数量': number
  '整体结论': string
  '整体通过': boolean
  '下游阻断': boolean
  '无法判定': boolean
}
type Abnormal = Record<string, string | number | boolean>
type RunDetail = RunSummary & {
  '规则结果': Record<string, string | number | boolean>[]
  '异常样本清单': Abnormal[]
  '边界原因': string[]
  '历史阻断依据': Abnormal[]
  '规则差异': Record<string, string>[]
  '监控回查': Record<string, string | number> | null
}

const ENDPOINT = '/api/validator/runs'
const runs = ref<RunSummary[]>([])
const selected = ref<RunDetail | null>(null)
const abnormal = ref<Abnormal[]>([])
const creating = ref(false)
const message = ref('')
const errorMessage = ref(false)
const form = ref({ scope: '仅已启用规则', ruleCodes: '', ratio: '按规则配置', source: 'pool' })

async function loadRuns() {
  try {
    const response = await request(ENDPOINT)
    if (!response.ok) throw new Error('校验运行包列表读取失败')
    const payload = await response.json()
    runs.value = payload.items ?? []
  } catch (error) {
    message.value = error instanceof Error ? error.message : '校验运行包列表读取失败'
    errorMessage.value = true
  }
}

async function createRun() {
  creating.value = true
  message.value = ''
  errorMessage.value = false
  try {
    const values: Record<string, unknown> = {
      "规则范围": form.value.scope,
      "采样比例": form.value.ratio.trim() || '按规则配置',
    }
    if (form.value.ruleCodes.trim()) values["规则编号"] = form.value.ruleCodes.trim()
    if (form.value.source === 'empty') values["样本"] = []
    const response = await request(ENDPOINT, {
      method: 'POST',
      body: JSON.stringify({ values }),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '校验运行包生成失败')
    await loadRuns()
    if (payload.entry?.id) await viewRun(payload.entry.id)
    message.value = payload.message
  } catch (error) {
    errorMessage.value = true
    message.value = error instanceof Error ? error.message : '校验运行包生成失败'
  } finally {
    creating.value = false
  }
}

async function viewRun(runId: number) {
  message.value = ''
  errorMessage.value = false
  try {
    const response = await request(`${ENDPOINT}/${runId}`)
    if (!response.ok) throw new Error('校验运行包详情读取失败')
    selected.value = await response.json()
    abnormal.value = selected.value?.['异常样本清单'] ?? []
  } catch (error) {
    errorMessage.value = true
    message.value = error instanceof Error ? error.message : '校验运行包详情读取失败'
  }
}

async function relayRun(runId: number) {
  message.value = ''
  errorMessage.value = false
  try {
    const response = await request(`/api/monitor/validation/runs/${runId}/relay`, { method: 'POST' })
    const payload = await response.json()
    if (!response.ok) throw new Error(payload.detail || '采集监控核对失败')
    message.value = payload.message
    if (selected.value?.id === runId) await viewRun(runId)
  } catch (error) {
    errorMessage.value = true
    message.value = error instanceof Error ? error.message : '采集监控核对失败'
  }
}

async function exportRuns() {
  await download(`${ENDPOINT}/export`, 'validator-runs.csv')
}

async function exportAbnormal() {
  if (!selected.value) return
  await download(`${ENDPOINT}/${selected.value.id}/abnormal/export`,
    `validator-run-${selected.value.id}-abnormal.csv`)
}

async function download(url: string, filename: string) {
  message.value = ''
  errorMessage.value = false
  try {
    const response = await request(url)
    if (!response.ok) throw new Error('导出失败，请稍后重试')
    const objectUrl = URL.createObjectURL(await response.blob())
    const link = document.createElement('a')
    link.href = objectUrl
    link.download = filename
    link.click()
    URL.revokeObjectURL(objectUrl)
  } catch (error) {
    errorMessage.value = true
    message.value = error instanceof Error ? error.message : '导出失败'
  }
}

onMounted(loadRuns)
</script>

<style scoped>
.run-panel { margin-top: 28px; }
.run-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 16px; }
.run-head h3 { margin: 0; font-size: 16px; }
.run-desc { color: var(--muted); font-size: 12px; margin: 4px 0 0; max-width: 720px; }
.run-actions { display: flex; gap: 8px; flex-shrink: 0; }
.run-form { display: flex; flex-wrap: wrap; gap: 12px; align-items: flex-end; margin: 12px 0; padding: 12px; background: #fff; border: 1px solid var(--border); border-radius: 8px; }
.run-form label { display: flex; flex-direction: column; gap: 4px; font-size: 12px; color: var(--muted); }
.run-form input, .run-form select { padding: 6px 10px; border: 1px solid var(--border); border-radius: 6px; font-size: 13px; min-width: 150px; }
.run-message { font-size: 13px; color: #15803d; margin: 0 0 10px; }
.run-message.error { color: #b42318; }
tr.selected { background: #eef4ff; }
.conclusion { display: inline-block; padding: 1px 8px; border-radius: 10px; font-size: 12px; background: #e2e8f0; }
.conclusion[data-conclusion="通过"] { background: #dcfce7; color: #166534; }
.conclusion[data-conclusion="告警"] { background: #fef3c7; color: #92400e; }
.conclusion[data-conclusion="阻断"] { background: #fee2e2; color: #b42318; }
.conclusion[data-conclusion="无法判定"] { background: #ede9fe; color: #6d28d9; }
.run-detail { margin-top: 14px; padding: 14px; background: #fff; border: 1px solid var(--border); border-radius: 8px; }
.run-detail-head { display: flex; justify-content: space-between; gap: 16px; align-items: flex-start; }
.run-meta { color: var(--muted); font-size: 12px; margin-left: 10px; }
.run-warning { background: #fff7ed; border: 1px solid #fed7aa; color: #9a3412; padding: 8px 12px; border-radius: 6px; font-size: 13px; }
.run-columns { display: grid; grid-template-columns: minmax(320px, 1fr) minmax(420px, 1.4fr); gap: 18px; margin-top: 8px; }
.run-columns h4 { font-size: 13px; margin: 12px 0 6px; }
.data-table.compact th, .data-table.compact td { padding: 5px 6px; font-size: 12px; }
.scope-cell { color: var(--muted); font-size: 11px; }
.boundary-list { margin: 0; padding-left: 18px; font-size: 12px; color: #334155; }
.boundary-list li { margin-bottom: 4px; }
.boundary-list.history { color: #92400e; }
@media (max-width: 1100px) { .run-columns { grid-template-columns: 1fr; } }
</style>
