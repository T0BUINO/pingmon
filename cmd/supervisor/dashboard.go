package main

const dashboardHTML = `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>PingMon Supervisor</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f7f9;
      --panel: #ffffff;
      --line: #d9dee7;
      --text: #18202b;
      --muted: #657184;
      --ok: #11875d;
      --warn: #b76b00;
      --bad: #c73737;
      --blue: #2458d3;
      --ink: #0e1624;
      --shadow: 0 10px 28px rgba(24, 32, 43, .08);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font: 14px/1.45 system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    button, select, input { font: inherit; }
    .shell {
      min-height: 100vh;
      display: grid;
      grid-template-columns: 280px minmax(0, 1fr);
    }
    aside {
      border-right: 1px solid var(--line);
      background: #fbfcfe;
      padding: 18px;
      position: sticky;
      top: 0;
      height: 100vh;
      overflow: auto;
    }
    main {
      min-width: 0;
      padding: 18px 22px 28px;
    }
    .brand {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 18px;
    }
    .brand h1 {
      margin: 0;
      font-size: 20px;
      letter-spacing: 0;
      color: var(--ink);
    }
    .live {
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 4px 9px;
      color: var(--muted);
      background: #fff;
      font-size: 12px;
      white-space: nowrap;
    }
    .live.ok { color: var(--ok); border-color: rgba(17, 135, 93, .3); }
    .live.warn { color: var(--warn); border-color: rgba(183, 107, 0, .3); }
    .controls {
      display: grid;
      gap: 10px;
      margin-bottom: 16px;
    }
    .field { display: grid; gap: 5px; }
    .field span {
      color: var(--muted);
      font-size: 12px;
    }
    select, input {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 7px;
      background: #fff;
      color: var(--text);
      padding: 8px 9px;
    }
    .agent-list {
      display: grid;
      gap: 7px;
    }
    .agent-row {
      width: 100%;
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 6px;
      align-items: center;
      border: 1px solid transparent;
      background: transparent;
      text-align: left;
      border-radius: 7px;
      padding: 8px;
      cursor: pointer;
      color: var(--text);
    }
    .agent-row:hover, .agent-row.active {
      background: #eef3ff;
      border-color: #c9d7ff;
    }
    .agent-row strong {
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      font-size: 13px;
    }
    .agent-row small {
      grid-column: 1 / -1;
      color: var(--muted);
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .dot {
      width: 9px;
      height: 9px;
      border-radius: 50%;
      background: var(--muted);
    }
    .dot.online { background: var(--ok); }
    .dot.offline { background: var(--bad); }
    .topbar {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 16px;
      margin-bottom: 18px;
    }
    .title h2 {
      margin: 0;
      font-size: 24px;
      letter-spacing: 0;
      color: var(--ink);
    }
    .title p {
      margin: 4px 0 0;
      color: var(--muted);
    }
    .actions {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    .icon-btn, .danger-btn {
      min-height: 36px;
      border: 1px solid var(--line);
      border-radius: 7px;
      background: #fff;
      color: var(--text);
      padding: 7px 11px;
      cursor: pointer;
    }
    .icon-btn:hover { border-color: #b8c4d6; }
    .danger-btn {
      color: var(--bad);
      border-color: rgba(199, 55, 55, .35);
    }
    .danger-btn[hidden] { display: none; }
    .metrics {
      display: grid;
      grid-template-columns: repeat(5, minmax(120px, 1fr));
      gap: 10px;
      margin-bottom: 14px;
    }
    .metric, .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: var(--shadow);
    }
    .metric {
      padding: 12px;
      min-height: 82px;
    }
    .metric span {
      display: block;
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 8px;
    }
    .metric strong {
      font-size: 23px;
      color: var(--ink);
      letter-spacing: 0;
    }
    .grid {
      display: grid;
      grid-template-columns: minmax(0, 1.25fr) minmax(360px, .75fr);
      gap: 14px;
      align-items: start;
    }
    .panel {
      min-width: 0;
      overflow: hidden;
    }
    .panel-head {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      padding: 12px 14px;
      border-bottom: 1px solid var(--line);
    }
    .panel-head h3 {
      margin: 0;
      font-size: 15px;
      letter-spacing: 0;
      color: var(--ink);
    }
    .panel-head span {
      color: var(--muted);
      font-size: 12px;
    }
    .chart {
      width: 100%;
      height: 260px;
      display: block;
      background: linear-gradient(#fff, #fafbfc);
    }
    table {
      width: 100%;
      border-collapse: collapse;
    }
    th, td {
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
      white-space: nowrap;
    }
    th {
      color: var(--muted);
      font-weight: 600;
      font-size: 12px;
      background: #fbfcfe;
    }
    td.wrap {
      white-space: normal;
      min-width: 180px;
    }
    .badge {
      display: inline-flex;
      align-items: center;
      min-height: 22px;
      border-radius: 999px;
      padding: 2px 8px;
      font-size: 12px;
      background: #eef1f5;
      color: var(--muted);
      white-space: nowrap;
    }
    .badge.ok { background: rgba(17, 135, 93, .12); color: var(--ok); }
    .badge.warn { background: rgba(183, 107, 0, .14); color: var(--warn); }
    .badge.bad { background: rgba(199, 55, 55, .12); color: var(--bad); }
    .empty {
      padding: 22px;
      color: var(--muted);
      text-align: center;
    }
    @media (max-width: 980px) {
      .shell { grid-template-columns: 1fr; }
      aside {
        position: static;
        height: auto;
        border-right: 0;
        border-bottom: 1px solid var(--line);
      }
      main { padding: 16px; }
      .metrics { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .grid { grid-template-columns: 1fr; }
    }
    @media (max-width: 620px) {
      .topbar { display: grid; }
      .actions { justify-content: flex-start; }
      .metrics { grid-template-columns: 1fr; }
      th, td { padding: 8px; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <aside>
      <div class="brand">
        <h1>PingMon</h1>
        <span id="liveState" class="live">连接中</span>
      </div>
      <div class="controls">
        <label class="field">
          <span>时间范围</span>
          <select id="rangeSelect">
            {{range .Ranges}}<option value="{{.}}" {{if eq . $.DefaultRange}}selected{{end}}>{{.}}</option>{{end}}
          </select>
        </label>
        <label class="field">
          <span>自定义范围</span>
          <input id="customRange" placeholder="例如 45m、6h、10d、2w、3mo">
        </label>
      </div>
      <div id="agentList" class="agent-list"></div>
    </aside>
    <main>
      <div class="topbar">
        <div class="title">
          <h2 id="viewTitle">Supervisor</h2>
          <p id="viewSubtitle">加载数据中</p>
        </div>
        <div class="actions">
          <button id="backButton" class="icon-btn" hidden title="返回全部节点">全部节点</button>
          <button id="refreshButton" class="icon-btn" title="刷新">刷新</button>
          <button id="deleteAgentButton" class="danger-btn" hidden title="删除离线节点数据">删除节点</button>
        </div>
      </div>
      <section class="metrics" id="metrics"></section>
      <section class="grid">
        <div class="panel">
          <div class="panel-head">
            <h3>趋势</h3>
            <span id="chartMeta"></span>
          </div>
          <svg id="chart" class="chart" role="img" aria-label="监控趋势"></svg>
        </div>
        <div class="panel">
          <div class="panel-head">
            <h3>异常</h3>
            <span id="problemMeta"></span>
          </div>
          <div style="overflow:auto">
            <table>
              <thead>
                <tr><th>时间</th><th>级别</th><th>节点</th><th>目标</th><th>成功率</th></tr>
              </thead>
              <tbody id="problemBody"></tbody>
            </table>
          </div>
        </div>
      </section>
      <section class="panel" style="margin-top:14px">
        <div class="panel-head">
          <h3>目标</h3>
          <span id="targetMeta"></span>
        </div>
        <div style="overflow:auto">
          <table>
            <thead>
              <tr><th>节点</th><th>目标</th><th>地址</th><th>标签</th><th>样本</th><th>成功率</th><th>延迟</th><th>最后检查</th></tr>
            </thead>
            <tbody id="targetBody"></tbody>
          </table>
        </div>
      </section>
    </main>
  </div>
  <script>
    const initialAgent = {{printf "%q" .SelectedAgent}};
    const state = {
      range: document.getElementById('rangeSelect').value || '24h',
      agent: initialAgent,
      data: null
    };
    const els = {
      live: document.getElementById('liveState'),
      range: document.getElementById('rangeSelect'),
      customRange: document.getElementById('customRange'),
      agentList: document.getElementById('agentList'),
      title: document.getElementById('viewTitle'),
      subtitle: document.getElementById('viewSubtitle'),
      metrics: document.getElementById('metrics'),
      chart: document.getElementById('chart'),
      chartMeta: document.getElementById('chartMeta'),
      targetBody: document.getElementById('targetBody'),
      targetMeta: document.getElementById('targetMeta'),
      problemBody: document.getElementById('problemBody'),
      problemMeta: document.getElementById('problemMeta'),
      refresh: document.getElementById('refreshButton'),
      back: document.getElementById('backButton'),
      del: document.getElementById('deleteAgentButton')
    };
    function fmtPct(value) {
      return Number.isFinite(value) ? (value * 100).toFixed(1) + '%' : '--';
    }
    function fmtMs(value) {
      return value > 0 ? value.toFixed(1) + ' ms' : '--';
    }
    function fmtTime(value) {
      if (!value) return '--';
      return new Date(value).toLocaleString();
    }
    function esc(value) {
      return String(value ?? '').replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
    }
    function validRange(raw) {
      return /^\d+(m|h|d|w|mo)$/.test(String(raw || '').trim().toLowerCase());
    }
    async function loadOverview() {
      const url = new URL('/api/overview', location.origin);
      url.searchParams.set('range', state.range);
      if (state.agent) url.searchParams.set('agent', state.agent);
      const res = await fetch(url);
      if (!res.ok) throw new Error(await res.text() || res.statusText);
      state.data = await res.json();
      render();
    }
    function metric(label, value) {
      return '<div class="metric"><span>' + esc(label) + '</span><strong>' + esc(value) + '</strong></div>';
    }
    function renderMetrics(summary) {
      els.metrics.innerHTML = [
        metric('在线节点', summary.agents_online + ' / ' + summary.agents_total),
        metric('目标', summary.targets),
        metric('成功率', fmtPct(summary.success_rate)),
        metric('平均延迟', fmtMs(summary.average_latency_ms)),
        metric('异常样本', summary.problems)
      ].join('');
    }
    function renderAgents(agents) {
      const all = document.createElement('button');
      all.className = 'agent-row' + (state.agent ? '' : ' active');
      all.innerHTML = '<strong>全部节点</strong><span class="dot online"></span><small>查看全局状态</small>';
      all.onclick = () => selectAgent('');
      els.agentList.replaceChildren(all);
      if (!agents.length) {
        const empty = document.createElement('div');
        empty.className = 'empty';
        empty.textContent = '暂无节点';
        els.agentList.appendChild(empty);
        return;
      }
      agents.forEach(agent => {
        const button = document.createElement('button');
        button.className = 'agent-row' + (state.agent === agent.agent ? ' active' : '');
        button.innerHTML =
          '<strong>' + esc(agent.agent) + '</strong>' +
          '<span class="dot ' + esc(agent.status) + '"></span>' +
          '<small>' + esc(agent.agent_ip || '未知 IP') + ' · ' + fmtPct(agent.success_rate) + ' · ' + fmtTime(agent.updated_at) + '</small>';
        button.onclick = () => selectAgent(agent.agent);
        els.agentList.appendChild(button);
      });
    }
    function renderTargets(targets) {
      els.targetMeta.textContent = targets.length + ' 个目标';
      els.targetBody.innerHTML = '';
      if (!targets.length) {
        els.targetBody.innerHTML = '<tr><td colspan="8" class="empty">暂无目标数据</td></tr>';
        return;
      }
      targets.forEach(row => {
        const rateClass = row.success_rate >= .99 ? 'ok' : row.success_rate > 0 ? 'warn' : 'bad';
        const labels = (row.labels || []).map(label => '<span class="badge">' + esc(label) + '</span>').join(' ');
        els.targetBody.insertAdjacentHTML('beforeend',
          '<tr><td>' + esc(row.agent) + '</td><td class="wrap">' + esc(row.target_name) + '</td><td>' + esc(row.address + ':' + row.port) + '</td>' +
          '<td class="wrap">' + labels + '</td><td>' + row.samples + '</td><td><span class="badge ' + rateClass + '">' + fmtPct(row.success_rate) + '</span></td>' +
          '<td>' + fmtMs(row.average_latency_ms) + '</td><td>' + fmtTime(row.last_checked_at) + '</td></tr>');
      });
    }
    function renderProblems(problems) {
      els.problemMeta.textContent = problems.length + ' 条';
      els.problemBody.innerHTML = '';
      if (!problems.length) {
        els.problemBody.innerHTML = '<tr><td colspan="5" class="empty">当前范围没有异常</td></tr>';
        return;
      }
      problems.forEach(row => {
        const cls = row.severity === 'ERROR' ? 'bad' : 'warn';
        els.problemBody.insertAdjacentHTML('beforeend',
          '<tr><td>' + fmtTime(row.checked_at) + '</td><td><span class="badge ' + cls + '">' + esc(row.severity) + '</span></td>' +
          '<td>' + esc(row.agent) + '</td><td class="wrap">' + esc(row.target_name + ' ' + row.address + ':' + row.port) + '</td><td>' + fmtPct(row.success_rate) + '</td></tr>');
      });
    }
    function renderChart(points) {
      const svg = els.chart;
      const width = Math.max(320, svg.clientWidth || 720);
      const height = 260;
      svg.setAttribute('viewBox', '0 0 ' + width + ' ' + height);
      svg.innerHTML = '';
      els.chartMeta.textContent = points.length + ' 点';
      if (!points.length) {
        svg.innerHTML = '<text x="50%" y="50%" dominant-baseline="middle" text-anchor="middle" fill="#657184">暂无趋势数据</text>';
        return;
      }
      const pad = {l: 42, r: 16, t: 16, b: 30};
      const minT = Math.min(...points.map(p => new Date(p.timestamp).getTime()));
      const maxT = Math.max(...points.map(p => new Date(p.timestamp).getTime()));
      const maxLatency = Math.max(1, ...points.map(p => p.average_latency_ms || 0));
      const groups = new Map();
      points.forEach(p => {
        const key = state.agent ? (p.target_name || 'target') : (p.agent || 'agent');
        if (!groups.has(key)) groups.set(key, []);
        groups.get(key).push(p);
      });
      const colors = ['#2458d3', '#11875d', '#b76b00', '#c73737', '#5b5fc7', '#008999'];
      function x(t) {
        if (maxT === minT) return pad.l;
        return pad.l + (new Date(t).getTime() - minT) / (maxT - minT) * (width - pad.l - pad.r);
      }
      function y(v) {
        return height - pad.b - (v / maxLatency) * (height - pad.t - pad.b);
      }
      const axis = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      axis.setAttribute('d', 'M' + pad.l + ' ' + pad.t + 'V' + (height - pad.b) + 'H' + (width - pad.r));
      axis.setAttribute('fill', 'none');
      axis.setAttribute('stroke', '#d9dee7');
      svg.appendChild(axis);
      Array.from(groups.entries()).slice(0, 8).forEach(([name, rows], index) => {
        rows.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
        const path = rows.map((p, i) => (i ? 'L' : 'M') + x(p.timestamp).toFixed(1) + ' ' + y(p.average_latency_ms || 0).toFixed(1)).join(' ');
        const el = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        el.setAttribute('d', path);
        el.setAttribute('fill', 'none');
        el.setAttribute('stroke', colors[index % colors.length]);
        el.setAttribute('stroke-width', '2');
        el.appendChild(document.createElementNS('http://www.w3.org/2000/svg', 'title')).textContent = name;
        svg.appendChild(el);
      });
    }
    function render() {
      const data = state.data;
      renderAgents(data.agents || []);
      renderMetrics(data.summary || {});
      renderTargets(data.targets || []);
      renderProblems(data.problems || []);
      renderChart(data.series || []);
      els.title.textContent = state.agent || 'Supervisor';
      els.subtitle.textContent = data.range + ' · 生成于 ' + fmtTime(data.generated_at);
      els.back.hidden = !state.agent;
      const selected = (data.agents || []).find(agent => agent.agent === state.agent);
      els.del.hidden = !selected || selected.status !== 'offline';
      const url = new URL(location.href);
      url.searchParams.set('range', state.range);
      if (state.agent) url.searchParams.set('agent', state.agent); else url.searchParams.delete('agent');
      history.replaceState(null, '', url);
      document.cookie = 'pingmon_range=' + encodeURIComponent(state.range) + '; Max-Age=31536000; Path=/; SameSite=Lax';
    }
    function selectAgent(agent) {
      state.agent = agent;
      loadOverview().catch(showError);
    }
    function showError(err) {
      els.subtitle.textContent = '加载失败：' + err.message;
      els.live.textContent = '失败';
      els.live.className = 'live warn';
    }
    els.range.onchange = () => {
      state.range = els.range.value;
      loadOverview().catch(showError);
    };
    els.customRange.onkeydown = event => {
      if (event.key !== 'Enter') return;
      const value = els.customRange.value.trim().toLowerCase();
      if (!validRange(value)) {
        els.customRange.focus();
        return;
      }
      state.range = value;
      loadOverview().catch(showError);
    };
    els.refresh.onclick = () => loadOverview().catch(showError);
    els.back.onclick = () => selectAgent('');
    els.del.onclick = async () => {
      if (!state.agent || !confirm('删除 ' + state.agent + ' 的历史数据？')) return;
      const url = '/api/agents?agent=' + encodeURIComponent(state.agent);
      const res = await fetch(url, {method: 'DELETE'});
      if (!res.ok) throw new Error(await res.text() || res.statusText);
      state.agent = '';
      await loadOverview();
    };
    function connectLive() {
      const ws = new WebSocket((location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/ws');
      ws.onopen = () => {
        els.live.textContent = '实时';
        els.live.className = 'live ok';
      };
      ws.onmessage = event => {
        if (event.data === 'connected') return;
        loadOverview().catch(showError);
      };
      ws.onclose = () => {
        els.live.textContent = '重连中';
        els.live.className = 'live warn';
        setTimeout(connectLive, 3000);
      };
      ws.onerror = () => ws.close();
    }
    loadOverview().catch(showError);
    connectLive();
    window.addEventListener('resize', () => state.data && renderChart(state.data.series || []));
  </script>
</body>
</html>`
