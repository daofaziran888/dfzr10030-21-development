<template>
  <section class="page" data-module="monitor">
    <header class="page-head">
      <div><h2>采集监控</h2><p class="page-desc">维护监控面板，围绕面板编号、采集源总数、今日采集量、成功率做登记、筛选与状态流转。</p></div>
    </header>
    <div class="analysis-summary">
      <article v-for="m in summaryMetrics" :key="m.label" class="analysis-card" data-testid="kpi-card">
        <span style="font-size:12px;color:var(--muted)">{{ m.label }}</span>
        <strong style="font-size:22px;display:block">{{ m.value }}</strong>
        <span :class="m.trend > 0 ? 'trend-up' : 'trend-down'" style="font-size:12px">{{ m.trend > 0 ? '↑' : '↓' }}{{ Math.abs(m.trend) }}%</span>
      </article>
    </div>
    <form class="analysis-form" @submit.prevent="runAnalysis">
      <label v-for="field in paramFields" :key="field"><span>{{ field }}</span>
        <select v-if="field.includes('方法')||field.includes('策略')||field.includes('粒度')" v-model="params[field]"><option v-for="o in options[field]" :key="o" :value="o">{{ o }}</option></select>
        <input v-else v-model="params[field]" :placeholder="`请输入${field}`" />
      </label>
      <button class="btn primary" type="submit" data-testid="run-analysis">运行分析</button>
    </form>
    <div v-if="chartData.length" class="analysis-chart">
      <div v-for="(bar, i) in chartData" :key="i" class="analysis-bar" :style="{ height: barHeight(bar.value) + 'px' }"><span>{{ bar.label }}</span></div>
    </div>
    <table class="data-table" v-if="results.length">
      <thead><tr><th v-for="c in resultColumns" :key="c">{{ c }}</th></tr></thead>
      <tbody><tr v-for="row in results" :key="row.id"><td v-for="c in resultColumns" :key="c">{{ row[c] ?? '—' }}</td></tr></tbody>
    </table>
    <p v-if="!results.length && !loading" style="text-align:center;color:var(--muted)">请配置参数后运行分析</p>

    <section class="validation-block" data-testid="validation-monitor">
      <header class="validation-head">
        <div>
          <h3>校验运行包监控</h3>
          <p class="validation-desc">
            采集监控服务读取校验运行包，把异常样本和无法判定状态计入当日告警；待办台账可直接回到来源运行包。
            <span v-if="validation?.['服务版本']" data-testid="monitor-service-version">
              当前版本：{{ validation['服务版本'] }}（校验服务 {{ validation['校验服务版本'] }}）
            </span>
          </p>
        </div>
        <button class="btn" data-testid="relay-runs" type="button" @click="relayRuns">核对未接力运行包</button>
      </header>
      <div class="validation-cards">
        <article v-for="card in validationCards" :key="card.label" class="analysis-card" :data-testid="`validation-card-${card.label}`">
          <span style="font-size:12px;color:var(--muted)">{{ card.label }}</span>
          <strong style="font-size:22px;display:block">{{ card.value }}</strong>
          <span style="font-size:12px;color:var(--muted)">{{ card.hint }}</span>
        </article>
      </div>
      <p v-if="validationMessage" class="validation-message" data-testid="validation-message">{{ validationMessage }}</p>
      <p v-if="validation?.['空态原因']" class="validation-empty" data-testid="validation-empty">{{ validation['空态原因'] }}</p>
      <p v-else-if="validation?.['接力空态原因']" class="validation-empty" data-testid="validation-relay-empty">{{ validation['接力空态原因'] }}</p>
      <table class="data-table" v-if="validation && validation['待办'].length">
        <thead>
          <tr><th>待办编号</th><th>来源运行包</th><th>异常样本</th><th>阻断规则</th><th>异常原因</th><th>阻断级别</th><th>处理状态</th><th>数据版本</th></tr>
        </thead>
        <tbody>
          <tr v-for="todo in validation['待办']" :key="todo['待办编号']" :data-testid="`todo-${todo['待办编号']}`">
            <td>{{ todo['待办编号'] }}</td>
            <td>{{ todo['来源运行包'] }}</td>
            <td>{{ todo['来源样本'] }}</td>
            <td>{{ todo['校验规则'] }}</td>
            <td>{{ todo['异常原因'] }}</td>
            <td>{{ todo['阻断级别'] }}</td>
            <td>{{ todo['处理状态'] }}</td>
            <td>{{ todo['数据版本'] }}</td>
          </tr>
        </tbody>
      </table>
      <p v-else class="validation-empty" data-testid="validation-todo-empty">
        {{ validation?.['待办空态原因'] || '暂无校验待办' }}
      </p>
    </section>
  </section>
</template>
<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { request } from '@/api/client'
const ENDPOINT = '/api/monitor'
const validation = ref<any>(null)
const validationMessage = ref('')
const validationCards = computed(() => {
  const v = validation.value
  return [
    { label: '今日运行包', value: v?.['今日运行包数'] ?? 0, hint: `累计 ${v?.['运行包数量'] ?? 0} 个` },
    { label: '阻断数量', value: v?.['阻断数量'] ?? 0, hint: `${v?.['阻断运行包数'] ?? 0} 个运行包被阻断` },
    { label: '当日告警', value: v?.['当日告警'] ?? 0, hint: `异常样本 ${v?.['异常样本数'] ?? 0} + 无法判定 ${v?.['无法判定数'] ?? 0}` },
    { label: '待处理待办', value: v?.['待处理待办'] ?? 0, hint: `台账共 ${v?.['待办数'] ?? 0} 条` },
  ]
})
async function loadValidation() {
  try {
    const res = await request(`${ENDPOINT}/validation`)
    if (!res.ok) throw new Error('校验监控读取失败')
    validation.value = await res.json()
  } catch (e) {
    validationMessage.value = e instanceof Error ? e.message : '校验监控读取失败'
  }
}
async function relayRuns() {
  validationMessage.value = ''
  try {
    const res = await request(`${ENDPOINT}/validation/relay`, { method: 'POST' })
    const data = await res.json()
    if (!res.ok) throw new Error(data.detail || '核对失败')
    validationMessage.value = `已核对 ${data['核对运行包数']} 个运行包，新增待办 ${data['新增待办数']} 条`
    await loadValidation()
  } catch (e) {
    validationMessage.value = e instanceof Error ? e.message : '核对失败'
  }
}
const summaryMetrics = ref<{label:string;value:number;trend:number}[]>([])
const results = ref<any[]>([]); const chartData = ref<{label:string;value:number}[]>([])
const loading = ref(false)
const params = ref<Record<string,string>>({})
const paramFields = ["面板编号", "采集源总数", "今日采集量", "成功率", "平均延迟", "失败重试", "异常告警", "更新时间"].filter((f: string) => !['编号','名称','版本'].includes(f)).slice(0, 4)
const options: Record<string,string[]> = { '统计粒度': ['按日','按周','按月'], '分组字段': ['全量','按类型','按区域'], '比较方法': ['环比','同比','绝对值'], '时间范围': ['最近7天','最近30天','最近90天'] }
const resultColumns = computed(() => ["面板编号", "采集源总数", "今日采集量", "成功率", "平均延迟", "失败重试", "异常告警", "更新时间"].slice(0, 5))
function barHeight(v: number) { const max = Math.max(...chartData.value.map(d => d.value), 1); return Math.max(10, (v / max) * 140) }
async function fetchSummary() { try { const res = await request(`${ENDPOINT}/summary`); summaryMetrics.value = (await res.json()).metrics || [] } catch(e) {} }
async function runAnalysis() {
  loading.value = true
  try { const res = await request(`${ENDPOINT}/run`, { method: 'POST', body: JSON.stringify({ values: params.value }) }); const data = await res.json(); results.value = data.items || []; chartData.value = (data.items || []).map((r: any) => ({ label: Object.values(r)[0] as string, value: Object.values(r).filter(v => typeof v === 'number')[0] as number || 0 })).slice(0, 6) } catch(e) { results.value = [] }
  loading.value = false
}
onMounted(() => { void fetchSummary(); void loadValidation() })
</script>
<style scoped>
.validation-block { margin-top: 28px; }
.validation-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 16px; }
.validation-head h3 { margin: 0; font-size: 16px; }
.validation-desc { color: var(--muted); font-size: 12px; margin: 4px 0 0; max-width: 720px; }
.validation-cards { display: flex; gap: 12px; flex-wrap: wrap; margin: 12px 0; }
.validation-message { font-size: 13px; color: #15803d; }
.validation-empty { font-size: 12px; color: var(--muted); text-align: center; }
</style>
