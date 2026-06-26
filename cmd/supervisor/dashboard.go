package main

const dashboardHTML = `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>PingMon 连通性面板</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f4f6f8;
      --surface: #ffffff;
      --surface-soft: #f9fafb;
      --line: #d8dee8;
      --text: #17202c;
      --muted: #667386;
      --ok: #13835d;
      --warn: #a56500;
      --bad: #be3a3a;
      --blue: #2457c5;
      --cyan: #007d89;
      --shadow: 0 12px 30px rgba(23, 32, 44, .08);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font: 14px/1.45 system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    button, select, input { font: inherit; }
    .app {
      min-height: 100vh;
      display: grid;
      grid-template-columns: 292px minmax(0, 1fr);
    }
    aside {
      height: 100vh;
      position: sticky;
      top: 0;
      overflow: auto;
      border-right: 1px solid var(--line);
      background: #fbfcfd;
      padding: 18px;
    }
    main {
      min-width: 0;
      padding: 20px 24px 32px;
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
    }
    .live {
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 4px 10px;
      color: var(--muted);
      background: #fff;
      font-size: 12px;
      white-space: nowrap;
    }
    .live.ok { color: var(--ok); border-color: rgba(19, 131, 93, .32); }
    .live.warn { color: var(--warn); border-color: rgba(165, 101, 0, .32); }
    .filters {
      display: grid;
      gap: 10px;
      margin-bottom: 18px;
    }
    .field {
      display: grid;
      gap: 5px;
    }
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
      padding: 8px 10px;
    }
    .server-list {
      display: grid;
      gap: 7px;
    }
    .server {
      width: 100%;
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 5px 8px;
      align-items: center;
      border: 1px solid transparent;
      border-radius: 8px;
      background: transparent;
      padding: 9px;
      text-align: left;
      cursor: pointer;
      color: var(--text);
    }
    .server:hover, .server.active {
      border-color: #c7d5fa;
      background: #eef3ff;
    }
    .server strong, .server small {
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .server strong { font-size: 13px; }
    .server small {
      grid-column: 1 / -1;
      color: var(--muted);
    }
    .dot {
      width: 9px;
      height: 9px;
      border-radius: 50%;
      background: var(--muted);
    }
    .dot.online { background: var(--ok); }
    .dot.offline { background: var(--bad); }
    .top {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 16px;
      margin-bottom: 16px;
    }
    .title h2 {
      margin: 0;
      font-size: 24px;
      letter-spacing: 0;
    }
    .title p {
      margin: 5px 0 0;
      color: var(--muted);
    }
    .actions {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    .btn {
      min-height: 36px;
      border: 1px solid var(--line);
      border-radius: 7px;
      background: #fff;
      color: var(--text);
      padding: 7px 11px;
      cursor: pointer;
    }
    .btn:hover { border-color: #b6c2d2; }
    .btn.danger {
      color: var(--bad);
      border-color: rgba(190, 58, 58, .35);
    }
    .btn[hidden] { display: none; }
    .summary {
      display: grid;
      grid-template-columns: repeat(5, minmax(128px, 1fr));
      gap: 10px;
      margin-bottom: 14px;
    }
    .metric, .panel {
      background: var(--surface);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: var(--shadow);
    }
    .metric {
      min-height: 84px;
      padding: 12px;
    }
    .metric span {
      display: block;
      margin-bottom: 8px;
      color: var(--muted);
      font-size: 12px;
    }
    .metric strong {
      font-size: 24px;
      letter-spacing: 0;
    }
    .layout {
      display: grid;
      grid-template-columns: minmax(0, 1.35fr) minmax(360px, .65fr);
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
      background: var(--surface-soft);
    }
    .panel-head h3 {
      margin: 0;
      font-size: 15px;
      letter-spacing: 0;
    }
    .panel-head span {
      color: var(--muted);
      font-size: 12px;
    }
    .chart {
      width: 100%;
      height: 280px;
      display: block;
      background: linear-gradient(#fff, #fbfcfd);
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
      font-size: 12px;
      font-weight: 650;
      background: var(--surface-soft);
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
      background: #edf1f5;
      color: var(--muted);
      white-space: nowrap;
    }
    .badge.ok { background: rgba(19, 131, 93, .12); color: var(--ok); }
    .badge.warn { background: rgba(165, 101, 0, .14); color: var(--warn); }
    .badge.bad { background: rgba(190, 58, 58, .12); color: var(--bad); }
    .empty {
      padding: 24px;
      color: var(--muted);
      text-align: center;
    }
    @media (max-width: 1020px) {
      .app { grid-template-columns: 1fr; }
      aside {
        position: static;
        height: auto;
        border-right: 0;
        border-bottom: 1px solid var(--line);
      }
      main { padding: 16px; }
      .summary { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .layout { grid-template-columns: 1fr; }
    }
    @media (max-width: 640px) {
      .top { display: grid; }
      .actions { justify-content: flex-start; }
      .summary { grid-template-columns: 1fr; }
      th, td { padding: 8px; }
    }
  </style>
</head>
<body>
  <div class="app">
    <aside>
      <div class="brand">
        <h1>PingMon</h1>
        <span id="liveState" class="live">连接中</span>
      </div>
      <div class="filters">
        <label class="field">
          <span>观察窗口</span>
          <select id="rangeSelect">
            {{range .Ranges}}<option value="{{.}}" {{if eq . $.DefaultRange}}selected{{end}}>{{.}}</option>{{end}}
          </select>
        </label>
        <label class="field">
          <span>自定义窗口</span>
          <input id="customRange" placeholder="45m / 6h / 10d / 2w / 3mo">
        </label>
      </div>
      <div id="serverList" class="server-list"></div>
    </aside>
    <main>
      <div class="top">
        <div class="title">
          <h2 id="viewTitle">服务器连通性</h2>
          <p id="viewSubtitle">正在载入连通性数据</p>
        </div>
        <div class="actions">
          <button id="backButton" class="btn" hidden title="查看全部服务器">全部服务器</button>
          <button id="refreshButton" class="btn" title="刷新">刷新</button>
          <button id="deleteAgentButton" class="btn danger" hidden title="删除离线服务器历史数据">删除离线服务器</button>
        </div>
      </div>

      <section class="summary" id="summary"></section>

      <section class="layout">
        <div class="panel">
          <div class="panel-head">
            <h3>出站连通性趋势</h3>
            <span id="chartMeta"></span>
          </div>
          <svg id="chart" class="chart" role="img" aria-label="服务器出站连通性趋势"></svg>
        </div>
        <div class="panel">
          <div class="panel-head">
            <h3>连接失败与抖动</h3>
            <span id="problemMeta"></span>
          </div>
          <div style="overflow:auto">
            <table>
              <thead>
                <tr><th>时间</th><th>级别</th><th>服务器</th><th>探测点</th><th>连通率</th></tr>
              </thead>
              <tbody id="problemBody"></tbody>
            </table>
          </div>
        </div>
      </section>

      <section class="panel" style="margin-top:14px">
        <div class="panel-head">
          <h3>探测点连通性</h3>
          <span id="probeMeta"></span>
        </div>
        <div style="overflow:auto">
          <table>
            <thead>
              <tr><th>服务器</th><th>探测点</th><th>地址</th><th>标签</th><th>样本</th><th>连通率</th><th>延迟</th><th>最后探测</th></tr>
            </thead>
            <tbody id="probeBody"></tbody>
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
      serverList: document.getElementById('serverList'),
      title: document.getElementById('viewTitle'),
      subtitle: document.getElementById('viewSubtitle'),
      summary: document.getElementById('summary'),
      chart: document.getElementById('chart'),
      chartMeta: document.getElementById('chartMeta'),
      probeBody: document.getElementById('probeBody'),
      probeMeta: document.getElementById('probeMeta'),
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
    function renderSummary(summary) {
      els.summary.innerHTML = [
        metric('在线服务器', summary.agents_online + ' / ' + summary.agents_total),
        metric('探测点', summary.probe_points ?? summary.targets),
        metric('整体连通率', fmtPct(summary.success_rate)),
        metric('平均连接延迟', fmtMs(summary.average_latency_ms)),
        metric('失败/抖动样本', summary.problems)
      ].join('');
    }
    function renderServers(servers) {
      const all = document.createElement('button');
      all.className = 'server' + (state.agent ? '' : ' active');
      all.innerHTML = '<strong>全部服务器</strong><span class="dot online"></span><small>汇总所有 agent 的出站连通性</small>';
      all.onclick = () => selectServer('');
      els.serverList.replaceChildren(all);
      if (!servers.length) {
        const empty = document.createElement('div');
        empty.className = 'empty';
        empty.textContent = '暂无服务器上报';
        els.serverList.appendChild(empty);
        return;
      }
      servers.forEach(server => {
        const button = document.createElement('button');
        button.className = 'server' + (state.agent === server.agent ? ' active' : '');
        button.innerHTML =
          '<strong>' + esc(server.agent) + '</strong>' +
          '<span class="dot ' + esc(server.status) + '"></span>' +
          '<small>' + esc(server.agent_ip || '未知 IP') + ' · ' + fmtPct(server.success_rate) + ' · ' + fmtTime(server.updated_at) + '</small>';
        button.onclick = () => selectServer(server.agent);
        els.serverList.appendChild(button);
      });
    }
    function renderProbes(probes) {
      els.probeMeta.textContent = probes.length + ' 个探测点';
      els.probeBody.innerHTML = '';
      if (!probes.length) {
        els.probeBody.innerHTML = '<tr><td colspan="8" class="empty">当前窗口没有连通性样本</td></tr>';
        return;
      }
      probes.forEach(row => {
        const rateClass = row.success_rate >= .99 ? 'ok' : row.success_rate > 0 ? 'warn' : 'bad';
        const labels = (row.labels || []).map(label => '<span class="badge">' + esc(label) + '</span>').join(' ');
        els.probeBody.insertAdjacentHTML('beforeend',
          '<tr><td>' + esc(row.agent) + '</td><td class="wrap">' + esc(row.probe_name || row.target_name) + '</td><td>' + esc(row.address + ':' + row.port) + '</td>' +
          '<td class="wrap">' + labels + '</td><td>' + row.samples + '</td><td><span class="badge ' + rateClass + '">' + fmtPct(row.success_rate) + '</span></td>' +
          '<td>' + fmtMs(row.average_latency_ms) + '</td><td>' + fmtTime(row.last_checked_at) + '</td></tr>');
      });
    }
    function renderProblems(problems) {
      els.problemMeta.textContent = problems.length + ' 条';
      els.problemBody.innerHTML = '';
      if (!problems.length) {
        els.problemBody.innerHTML = '<tr><td colspan="5" class="empty">当前窗口没有连接失败</td></tr>';
        return;
      }
      problems.forEach(row => {
        const cls = row.severity === 'ERROR' ? 'bad' : 'warn';
        els.problemBody.insertAdjacentHTML('beforeend',
          '<tr><td>' + fmtTime(row.checked_at) + '</td><td><span class="badge ' + cls + '">' + esc(row.severity) + '</span></td>' +
          '<td>' + esc(row.agent) + '</td><td class="wrap">' + esc((row.probe_name || row.target_name) + ' · ' + row.address + ':' + row.port) + '</td><td>' + fmtPct(row.success_rate) + '</td></tr>');
      });
    }
    function renderChart(points) {
      const svg = els.chart;
      const width = Math.max(320, svg.clientWidth || 720);
      const height = 280;
      svg.setAttribute('viewBox', '0 0 ' + width + ' ' + height);
      svg.innerHTML = '';
      els.chartMeta.textContent = points.length + ' 个聚合点';
      if (!points.length) {
        svg.innerHTML = '<text x="50%" y="50%" dominant-baseline="middle" text-anchor="middle" fill="#667386">暂无趋势数据</text>';
        return;
      }
      const pad = {l: 44, r: 16, t: 18, b: 32};
      const minT = Math.min(...points.map(p => new Date(p.timestamp).getTime()));
      const maxT = Math.max(...points.map(p => new Date(p.timestamp).getTime()));
      const maxLatency = Math.max(1, ...points.map(p => p.average_latency_ms || 0));
      const groups = new Map();
      points.forEach(p => {
        const key = state.agent ? (p.target_name || '探测点') : (p.agent || '服务器');
        if (!groups.has(key)) groups.set(key, []);
        groups.get(key).push(p);
      });
      const colors = ['#2457c5', '#13835d', '#a56500', '#be3a3a', '#5b5fc7', '#007d89'];
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
      axis.setAttribute('stroke', '#d8dee8');
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
      renderServers(data.agents || []);
      renderSummary(data.summary || {});
      renderProbes(data.targets || []);
      renderProblems(data.problems || []);
      renderChart(data.series || []);
      els.title.textContent = state.agent ? state.agent + ' 出站连通性' : '服务器连通性';
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
    function selectServer(agent) {
      state.agent = agent;
      loadOverview().catch(showError);
    }
    function showError(err) {
      els.subtitle.textContent = '载入失败：' + err.message;
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
    els.back.onclick = () => selectServer('');
    els.del.onclick = async () => {
      if (!state.agent || !confirm('删除 ' + state.agent + ' 的历史连通性数据？')) return;
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
