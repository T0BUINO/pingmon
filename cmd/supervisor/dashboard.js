    const colors = ['#2563eb', '#16a34a', '#dc2626', '#9333ea', '#d97706', '#0891b2', '#be123c', '#4f46e5'];
    const maxProblemLogRows = 200;
    const minChartGapMs = 5 * 60 * 1000;
    const selectedAgent = document.body.dataset.agent || '';
    const defaultOfflineAfterSeconds = Number(document.body.dataset.offlineAfter || 90);
    let selectedRange = document.body.dataset.selectedRange || '24h';
    let detailChart = null;
    let miniCharts = new Set();
    let miniChartObserver = null;
    let selectedLabels = null;
    let currentRows = [];
    let currentAgents = [];
    let currentAgentRows = [];
    let chartFullRange = null;
    let chartViewRange = null;
    let targetVisibility = new Map();
    let liveRefreshTimer = null;
    let dashboardRefreshSequence = 0;
    let pinchStart = null;
    let panStart = null;
    document.querySelectorAll('.local-time').forEach(cell => {
      const date = new Date(cell.dataset.time);
      if (!Number.isNaN(date.getTime())) cell.textContent = date.toLocaleString();
    });
    async function loadResults(agent = selectedAgent) {
      const agentParam = agent ? '&agent=' + encodeURIComponent(agent) : '';
      const res = await fetch('/api/results?dashboard=1&range=' + encodeURIComponent(selectedRange) + agentParam, {cache: 'no-store'});
      if (!res.ok) throw new Error('结果数据加载失败，状态码：' + res.status);
      const rows = await res.json();
      const normalized = normalizeResultRows(rows);
      return normalized.reverse();
    }
    async function loadAgents() {
      const res = await fetch('/api/agents');
      if (!res.ok) throw new Error('节点状态加载失败，状态码：' + res.status);
      return await res.json();
    }
    async function deleteAgent(agent) {
      if (!agent) return;
      if (!confirm('将删除 ' + agent + ' 的所有历史结果和离线记录，不能撤销。')) return;
      const res = await fetch('/api/agents?agent=' + encodeURIComponent(agent), {method: 'DELETE'});
      if (!res.ok) throw new Error('删除结果失败，状态码：' + res.status);
      currentAgents = currentAgents.filter(item => item.agent !== agent);
      currentRows = currentRows.filter(row => row.agent !== agent);
      if (selectedAgent === agent) {
        location.href = '/dashboard?range=' + encodeURIComponent(selectedRange);
        return;
      }
      renderDashboardRows(currentRows);
    }
    function parseRangeMillis(raw) {
      const value = String(raw || '24h').trim().toLowerCase();
      const match = value.match(/^(\d+)(m|h|d|w|mo)$/);
      if (!match) return 24 * 60 * 60 * 1000;
      const amount = Number(match[1]);
      const multipliers = {
        m: 60 * 1000,
        h: 60 * 60 * 1000,
        d: 24 * 60 * 60 * 1000,
        w: 7 * 24 * 60 * 60 * 1000,
        mo: 30 * 24 * 60 * 60 * 1000
      };
      return amount > 0 ? amount * multipliers[match[2]] : 24 * 60 * 60 * 1000;
    }
    function rowFingerprint(row) {
      return [
        row.agent,
        row.agent_ip || '',
        row.target_name,
        row.address,
        row.port,
        JSON.stringify(row.labels || []),
        row.checked_at,
        row.success_count,
        row.failure_count,
        row.average_latency_ms,
        row.success_rate,
        row.error || ''
      ].join('|');
    }
    function normalizeResultRows(rows) {
      if (!Array.isArray(rows)) return [];
      return rows.map(normalizeResultRow).filter(Boolean);
    }
function normalizeResultRow(row) {
    if (!Array.isArray(row)) return row && typeof row === 'object' ? row : null;
    const normalized = {
        agent: row[0] || '',
        agent_ip: row[1] || '',
        target_name: row[2] || '',
        address: row[3] || '',
        port: row[4] || 0,
        labels: Array.isArray(row[5]) ? row[5] : [],
        checked_at: decodeCompactTime(row[6]),
        success_count: row[7] || 0,
        failure_count: row[8] || 0,
        average_latency_ms: row[9] || 0,
        success_rate: row[10] || 0,
        error: row[11] || ''
    };
    return normalized;
}
    function decodeCompactTime(value) {
      if (typeof value !== 'string' || value.length < 10 || !/^[0-9a-z]+$/i.test(value)) return value || '';
      const ns = parseBase36BigInt(value);
      if (ns === null) return value;
      const billion = 1000000000n;
      const million = 1000000n;
      const seconds = ns / billion;
      const nanos = ns % billion;
      const millis = seconds * 1000n + nanos / million;
      const date = new Date(Number(millis));
      if (Number.isNaN(date.getTime())) return value;
      const whole = date.toISOString().replace(/\.\d{3}Z$/, '');
      if (nanos === 0n) return whole + 'Z';
      const fraction = nanos.toString().padStart(9, '0').replace(/0+$/, '');
      return whole + '.' + fraction + 'Z';
    }
    function parseBase36BigInt(value) {
      let total = 0n;
      for (const char of value.toLowerCase()) {
        const code = char.charCodeAt(0);
        let digit;
        if (code >= 48 && code <= 57) digit = code - 48;
        else if (code >= 97 && code <= 122) digit = code - 87;
        else return null;
        total = total * 36n + BigInt(digit);
      }
      return total;
    }
    function rowsForCurrentView(rows) {
      const cutoff = Date.now() - parseRangeMillis(selectedRange);
      return rows.filter(row => {
        const ts = timeValue(row);
        return ts !== null && ts >= cutoff && (!selectedAgent || row.agent === selectedAgent);
      });
    }
    function sortRowsByTime(rows) {
      return rows.slice().sort((a, b) => (timeValue(a) || 0) - (timeValue(b) || 0));
    }
    function mergeRows(existing, incoming) {
      const seen = new Set(existing.map(rowFingerprint));
      const merged = existing.slice();
      for (const row of rowsForCurrentView(incoming)) {
        const fingerprint = rowFingerprint(row);
        if (seen.has(fingerprint)) continue;
        seen.add(fingerprint);
        merged.push(row);
      }
      return sortRowsByTime(rowsForCurrentView(merged));
    }
    function targetKey(row) {
      return row.target_name + ' (' + row.address + ':' + row.port + ')';
    }
    function rowLabels(row) {
      return Array.isArray(row.labels) ? row.labels.filter(label => label) : [];
    }
    function availableLabels(rows) {
      const labels = new Set();
      rows.forEach(row => rowLabels(row).forEach(label => labels.add(label)));
      return Array.from(labels).sort((a, b) => a.localeCompare(b));
    }
    function filterRowsByLabels(rows, labels) {
      if (labels === null) return rows;
      if (!labels.size) return [];
      return rows.filter(row => rowLabels(row).some(label => labels.has(label)));
    }
    function timeValue(row) {
      const ts = new Date(row.checked_at).getTime();
      return Number.isNaN(ts) ? null : ts;
    }
    function medianInterval(points) {
      if (points.length < 3) return minChartGapMs;
      const intervals = [];
      for (let i = 1; i < points.length; i++) {
        const gap = points[i].x - points[i - 1].x;
        if (gap > 0) intervals.push(gap);
      }
      if (!intervals.length) return minChartGapMs;
      intervals.sort((a, b) => a - b);
      return intervals[Math.floor(intervals.length / 2)];
    }
    function formatTimeTick(value) {
      const date = new Date(Number(value));
      if (Number.isNaN(date.getTime())) return '';
      const range = selectedRange.toLowerCase();
      if (range.endsWith('m')) return date.toLocaleTimeString([], {hour: '2-digit', minute: '2-digit', second: '2-digit'});
      if (range.endsWith('h')) return date.toLocaleTimeString([], {hour: '2-digit', minute: '2-digit'});
      if (range === '24h') return date.toLocaleString([], {month: '2-digit', day: '2-digit', hour: '2-digit'});
      return date.toLocaleDateString([], {month: '2-digit', day: '2-digit'});
    }
    class CanvasLineChart {
      constructor(container, data, options) {
        this.container = container;
        this.data = data || {datasets: []};
        this.options = options || {};
        this.visibility = new Map();
        this.raf = 0;
        this.canvas = document.createElement('canvas');
        this.canvas.setAttribute('role', 'img');
        this.canvas.setAttribute('aria-label', '\u5ef6\u8fdf\u56fe\u8868');
        this.container.replaceChildren(this.canvas);
        this.ctx = this.canvas.getContext('2d');
        this.emptyState = null;
        if (!this.options.mini) {
          this.emptyState = document.createElement('div');
          this.emptyState.className = 'chart-empty';
          this.emptyState.textContent = '当前周期暂无数据';
          this.container.appendChild(this.emptyState);
        }
        this.hoverX = null;
        this.hoverLine = document.createElement('div');
        this.hoverLine.className = 'chart-hover-line';
        this.container.appendChild(this.hoverLine);
        this.tooltipCache = new Map();
        this.lastTooltipX = null;
        this.lastTooltipPixel = null;
        this.lastPointerClientY = null;
        this.lastArea = null;
        this.lastXRange = null;
        this.tooltipActive = false;
        this.scales = {x: {getValueForPixel: pixel => this.valueForPixel(pixel)}};
        this._resizeTimer = 0;
        this.resizeObserver = new ResizeObserver(() => {
          clearTimeout(this._resizeTimer);
          this._resizeTimer = setTimeout(() => this.update(), 150);
        });
        this.resizeObserver.observe(this.container);
        this.container.addEventListener('pointermove', event => this.scheduleTooltip(event));
        this.container.addEventListener('pointerleave', () => hideChartTooltip(this));
        this.container.addEventListener('pointercancel', () => hideChartTooltip(this));
        if (!this.options.deferUpdate) this.update();
      }
      destroy() {
        cancelAnimationFrame(this.raf);
        cancelAnimationFrame(this.tooltipRaf);
        clearTimeout(this._resizeTimer);
        if (this.resizeObserver) this.resizeObserver.disconnect();
        this.container.replaceChildren();
      }
      setData(data) {
        this.data = data || {datasets: []};
        this.invalidateTooltipCache();
        this.update();
      }
      setDatasetVisibility(index, visible) {
        this.visibility.set(index, visible);
        this.invalidateTooltipCache();
      }
      isDatasetVisible(index) {
        return this.visibility.has(index) ? this.visibility.get(index) : true;
      }
      chartArea() {
        const mini = this.options.mini;
        const rect = this.container.getBoundingClientRect();
        const width = Math.max(1, rect.width || this.container.clientWidth || 1);
        const height = Math.max(1, rect.height || this.container.clientHeight || (mini ? 86 : 390));
        const margin = mini ? {top: 6, right: 4, bottom: 6, left: 4} : {top: 14, right: 18, bottom: 30, left: 42};
        return {width, height, margin, left: margin.left, right: width - margin.right, top: margin.top, bottom: height - margin.bottom};
      }
      visibleDatasets() {
        return this.data.datasets.filter((_, index) => this.isDatasetVisible(index));
      }
      invalidateTooltipCache() {
        this.tooltipCache.clear();
        this.lastTooltipX = null;
        this.lastTooltipPixel = null;
      }
      xRange() {
        const xOptions = this.options.scales?.x || {};
        if (Number.isFinite(xOptions.min) && Number.isFinite(xOptions.max) && xOptions.min < xOptions.max) {
          return {min: xOptions.min, max: xOptions.max};
        }
        const range = dataRange(this.data);
        if (range) return range;
        const span = Math.max(minChartGapMs, parseRangeMillis(selectedRange));
        const max = Date.now();
        return {min: max - span, max};
      }
      yRange(xRange) {
        let max = 0;
        this.visibleDatasets().forEach(dataset => {
          this.pointWindow(dataset.data, xRange).forEach(point => {
            if (point.x < xRange.min || point.x > xRange.max) return;
            if (point.y !== null && Number.isFinite(point.y)) max = Math.max(max, point.y);
          });
        });
        const padded = max > 0 ? max * 1.08 : 1;
        const step = this.niceStep(padded / 5);
        return {min: 0, max: Math.ceil(padded / step) * step};
      }
      valueForPixel(pixel, area, range) {
        area = area || this.lastArea || this.chartArea();
        range = range || this.lastXRange || this.xRange();
        const usable = Math.max(1, area.right - area.left);
        const ratio = Math.min(1, Math.max(0, (pixel - area.left) / usable));
        return range.min + (range.max - range.min) * ratio;
      }
      pointToPixel(point, xRange, yRange, area) {
        const x = area.left + (point.x - xRange.min) / Math.max(1, xRange.max - xRange.min) * (area.right - area.left);
        const y = area.bottom - (point.y - yRange.min) / Math.max(1, yRange.max - yRange.min) * (area.bottom - area.top);
        return {x, y};
      }
      segmentVertexData(segment, xRange, yRange, area) {
        if (!segment.length) return [];
        if (segment.length < 3 || this.options.smooth === false) {
          const px = segment.map(p => this.pointToPixel(p, xRange, yRange, area));
          if (px.length < 2) return [{x0: px[0].x, y0: px[0].y, c1x: px[0].x, c1y: px[0].y, c2x: px[0].x, c2y: px[0].y, x1: px[0].x, y1: px[0].y}];
          const result = [];
          for (let i = 0; i < px.length - 1; i++) {
            result.push({x0: px[i].x, y0: px[i].y, c1x: px[i].x, c1y: px[i].y, c2x: px[i+1].x, c2y: px[i+1].y, x1: px[i+1].x, y1: px[i+1].y});
          }
          return result;
        }
        const px = segment.map(p => this.pointToPixel(p, xRange, yRange, area));
        const n = px.length;
        const slopes = new Float64Array(n - 1);
        const tangents = new Float64Array(n);
        for (let i = 0; i < n - 1; i++) {
          const dx = px[i + 1].x - px[i].x;
          slopes[i] = dx === 0 ? 0 : (px[i + 1].y - px[i].y) / dx;
        }
        tangents[0] = slopes[0];
        tangents[n - 1] = slopes[n - 2];
        for (let i = 1; i < n - 1; i++) {
          tangents[i] = slopes[i - 1] * slopes[i] <= 0 ? 0 : (slopes[i - 1] + slopes[i]) / 2;
        }
        for (let i = 0; i < n - 1; i++) {
          if (slopes[i] === 0) { tangents[i] = 0; tangents[i + 1] = 0; continue; }
          const a = tangents[i] / slopes[i];
          const b = tangents[i + 1] / slopes[i];
          const h = Math.hypot(a, b);
          if (h > 3) { const s = 3 / h; tangents[i] = s * a * slopes[i]; tangents[i + 1] = s * b * slopes[i]; }
        }
        const result = [];
        for (let i = 0; i < n - 1; i++) {
          const dx = px[i + 1].x - px[i].x;
          result.push({
            x0: px[i].x, y0: px[i].y,
            c1x: px[i].x + dx / 3, c1y: px[i].y + tangents[i] * dx / 3,
            c2x: px[i + 1].x - dx / 3, c2y: px[i + 1].y - tangents[i + 1] * dx / 3,
            x1: px[i + 1].x, y1: px[i + 1].y
          });
        }
        return result;
      }
      pointWindow(points, xRange) {
        let start = 0, end = points.length;
        let lo = 0, hi = points.length;
        while (lo < hi) { const mid = Math.floor((lo + hi) / 2); if (points[mid].x < xRange.min) lo = mid + 1; else hi = mid; }
        start = Math.max(0, lo - 1);
        lo = 0; hi = points.length;
        while (lo < hi) { const mid = Math.floor((lo + hi) / 2); if (points[mid].x <= xRange.max) lo = mid + 1; else hi = mid; }
        end = Math.min(points.length, lo + 1);
        return points.slice(start, end);
      }
      drawDataset(dataset, xRange, yRange, area) {
        const ctx = this.ctx;
        const pts = this.pointWindow(dataset.data, xRange).filter(p => p.y !== null && Number.isFinite(p.y));
        if (pts.length < 1) return;
        const segments = this.segmentVertexData(pts, xRange, yRange, area);
        ctx.beginPath();
        ctx.moveTo(segments[0].x0, segments[0].y0);
        for (const seg of segments) {
          ctx.bezierCurveTo(seg.c1x, seg.c1y, seg.c2x, seg.c2y, seg.x1, seg.y1);
        }
        ctx.stroke();
      }
      niceStep(rawStep) {
        const exponent = Math.floor(Math.log10(Math.max(1, rawStep)));
        const base = Math.pow(10, exponent);
        const fraction = rawStep / base;
        const niceFraction = fraction <= 1 ? 1 : fraction <= 2 ? 2 : fraction <= 5 ? 5 : 10;
        return niceFraction * base;
      }
      yTicks(yRange) {
        const targetCount = 5;
        const max = Math.max(1, yRange.max);
        const step = this.niceStep(max / targetCount);
        const top = Math.ceil(max / step) * step;
        const ticks = [];
        for (let value = 0; value <= top + step / 2; value += step) ticks.push(value);
        return ticks.length >= 2 ? ticks : [0, top || step];
      }
      xStepMs(span) {
        const steps = [60*1000,5*60*1000,15*60*1000,30*60*1000,60*60*1000,2*60*60*1000,3*60*60*1000,6*60*60*1000,12*60*60*1000,24*60*60*1000,2*24*60*60*1000,7*24*60*60*1000,14*24*60*60*1000,30*24*60*60*1000];
        const target = window.innerWidth <= 760 ? 4 : 7;
        const dataStep = this.dataStepMs();
        return steps.find(step => step >= dataStep && span / step <= target) || steps[steps.length - 1];
      }
      dataStepMs() {
        const gaps = this.visibleDatasets().map(d => d.typicalGap).filter(g => Number.isFinite(g) && g > 0).sort((a, b) => a - b);
        return gaps.length ? gaps[Math.floor(gaps.length / 2)] : minChartGapMs;
      }
      xTicks(xRange) {
        const span = Math.max(1, xRange.max - xRange.min);
        const step = this.xStepMs(span);
        const ticks = [xRange.min];
        let value = Math.ceil(xRange.min / step) * step;
        while (value < xRange.max) { if (value > xRange.min) ticks.push(value); value += step; }
        if (ticks[ticks.length - 1] !== xRange.max) ticks.push(xRange.max);
        return ticks;
      }
      visibleXTicks(ticks, xRange, area) {
        const placed = [];
        const measure = value => Math.max(38, formatTimeTick(value).length * 7 + 10);
        const pixelFor = value => area.left + (value - xRange.min) / Math.max(1, xRange.max - xRange.min) * (area.right - area.left);
        for (const value of ticks) {
          const x = pixelFor(value);
          const w = measure(value);
          const start = x - w / 2;
          const end = x + w / 2;
          const last = placed[placed.length - 1];
          if (!last || start > last.end + 8 || value === xRange.max) {
            if (value === xRange.max && last && start <= last.end + 8 && last.value !== xRange.min) placed.pop();
            placed.push({value, x, start, end});
          }
        }
        return placed.map(item => item.value);
      }
      renderAxes(area, xRange, yRange) {
        const ctx = this.ctx;
        ctx.strokeStyle = '#e7ebf1';
        ctx.lineWidth = 1;
        ctx.setLineDash([]);
        this.yTicks(yRange).forEach(value => {
          const y = area.bottom - (value - yRange.min) / Math.max(1, yRange.max - yRange.min) * (area.bottom - area.top);
          if (y < area.top - 1 || y > area.bottom + 1) return;
          ctx.beginPath();
          ctx.moveTo(area.left, y);
          ctx.lineTo(area.right, y);
          ctx.stroke();
          ctx.fillStyle = '#64748b';
          ctx.font = '11px system-ui, sans-serif';
          ctx.textAlign = 'left';
          ctx.textBaseline = value === 0 ? 'top' : 'middle';
          ctx.fillText(value === 0 ? '0' : Math.round(value) + 'ms', 4, value === 0 ? y + 2 : y);
        });
        const xTicks = this.visibleXTicks(this.xTicks(xRange), xRange, area);
        xTicks.forEach((value, index) => {
          const x = area.left + (value - xRange.min) / Math.max(1, xRange.max - xRange.min) * (area.right - area.left);
          const anchor = index === 0 ? 'left' : index === xTicks.length - 1 ? 'right' : 'center';
          ctx.fillStyle = '#64748b';
          ctx.font = '11px system-ui, sans-serif';
          ctx.textAlign = anchor;
          ctx.textBaseline = 'top';
          // Keep the full glyph box inside the canvas. Drawing at height - 8
          // with a top baseline clips an 11px label, especially on mobile DPRs.
          ctx.fillText(formatTimeTick(value), x, area.bottom + 8);
        });
      }
      update() {
        this.invalidateTooltipCache();
        cancelAnimationFrame(this.raf);
        this.raf = requestAnimationFrame(() => {
          const area = this.chartArea();
          if (area.width < 2 || area.height < 2) return;
          const xRange = this.xRange();
          const yRange = this.yRange(xRange);
          const hasVisibleData = this.visibleDatasets().some(dataset =>
            this.pointWindow(dataset.data, xRange).some(point => point.y !== null && Number.isFinite(point.y))
          );
          if (this.emptyState) this.emptyState.style.display = hasVisibleData ? 'none' : 'flex';
          this.lastArea = area;
          this.lastXRange = xRange;
          const dpr = window.devicePixelRatio || 1;
          this.canvas.width = Math.round(area.width * dpr);
          this.canvas.height = Math.round(area.height * dpr);
          this.canvas.style.width = area.width + 'px';
          this.canvas.style.height = area.height + 'px';
          const ctx = this.ctx;
          ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
          ctx.clearRect(0, 0, area.width, area.height);
          if (!this.options.mini) this.renderAxes(area, xRange, yRange);
          ctx.save();
          ctx.beginPath();
          ctx.rect(area.left, area.top, area.right - area.left, area.bottom - area.top);
          ctx.clip();
          this.data.datasets.forEach((dataset, index) => {
            if (!this.isDatasetVisible(index)) return;
            ctx.strokeStyle = dataset.borderColor;
            ctx.lineWidth = this.options.mini ? 1.6 : 2;
            ctx.lineJoin = 'round';
            ctx.setLineDash([]);
            this.drawDataset(dataset, xRange, yRange, area);
          });
          ctx.restore();
          this.updateHoverLine();
        });
      }
      nearestAnchorTime(xValue) {
        let best = null;
        this.visibleDatasets().forEach(dataset => {
          const nearest = this.nearestInDataset(dataset, xValue);
          if (!nearest) return;
          if (!best || nearest.distance < best.distance) best = nearest;
        });
        return best ? best.point.x : null;
      }
      updateHoverLine() {
        if (!this.hoverLine) return;
        const area = this.lastArea || this.chartArea();
        const xRange = this.lastXRange || this.xRange();
        if (this.hoverX === null || this.hoverX < xRange.min || this.hoverX > xRange.max) {
          this.hoverLine.style.opacity = '0';
          return;
        }
        const x = area.left + (this.hoverX - xRange.min) / Math.max(1, xRange.max - xRange.min) * (area.right - area.left);
        this.hoverLine.style.top = area.top.toFixed(1) + 'px';
        this.hoverLine.style.bottom = (area.height - area.bottom).toFixed(1) + 'px';
        this.hoverLine.style.opacity = '1';
        this.hoverLine.style.transform = 'translate3d(' + x.toFixed(1) + 'px, 0, 0)';
      }
      nearestInDataset(dataset, xValue) {
        const points = dataset.data;
        let lo = 0, hi = points.length - 1;
        while (lo < hi) { const mid = Math.floor((lo + hi) / 2); if (points[mid].x < xValue) lo = mid + 1; else hi = mid; }
        let best = null;
        [lo - 2, lo - 1, lo, lo + 1, lo + 2].forEach(index => {
          const point = points[index];
          if (!point || point.y === null) return;
          const distance = Math.abs(point.x - xValue);
          if (!best || distance < best.distance) best = {dataset, point, distance};
        });
        return best;
      }
      tooltipPoints(clientX) {
        const rect = this.container.getBoundingClientRect();
        const pointerX = this.valueForPixel(clientX - rect.left, this.lastArea, this.lastXRange);
        const xValue = this.nearestAnchorTime(pointerX);
        if (xValue === null) return {xValue: pointerX, items: []};
        if (this.tooltipCache.has(xValue)) return this.tooltipCache.get(xValue);
        const items = [];
        this.visibleDatasets().forEach(dataset => {
          const nearest = this.nearestInDataset(dataset, xValue);
          if (!nearest) return;
          const typicalGap = dataset.typicalGap || minChartGapMs;
          const maxDistance = Math.max(minChartGapMs, typicalGap * 1.5);
          if (nearest.distance <= maxDistance) items.push(nearest);
        });
        items.sort((a, b) => b.point.y - a.point.y || a.dataset.label.localeCompare(b.dataset.label));
        const result = {xValue, items};
        this.tooltipCache.set(xValue, result);
        return result;
      }
      scheduleTooltip(event) {
        const clientX = event.clientX;
        const clientY = event.clientY;
        this.tooltipActive = true;
        const pixel = Math.round(clientX);
        if (pixel === this.lastTooltipPixel && Math.abs(clientY - (this.lastPointerClientY || clientY)) < 2) return;
        this.lastTooltipPixel = pixel;
        this.lastPointerClientY = clientY;
        cancelAnimationFrame(this.tooltipRaf);
        this.tooltipRaf = requestAnimationFrame(() => {
          if (this.tooltipActive) showChartTooltip(this, clientX, clientY);
        });
      }
    }
    function lttb(points, targetCount) {
      const n = points.length;
      if (n <= targetCount || targetCount < 3) return points.slice();
      const result = new Array(targetCount);
      result[0] = points[0];
      result[targetCount - 1] = points[n - 1];
      const bucketSize = (n - 2) / (targetCount - 2);
      let idx = 0;
      for (let i = 0; i < targetCount - 2; i++) {
        const start = Math.floor(i * bucketSize) + 1;
        const end = Math.min(n - 1, Math.floor((i + 1) * bucketSize) + 1);
        const nextStart = Math.floor((i + 2) * bucketSize) + 1;
        let endCap = Math.min(n - 1, nextStart);
        while (endCap > start && endCap < n && points[endCap].y === null) endCap--;
        if (endCap <= start || !Number.isFinite(points[endCap].y)) {
          result[i + 1] = points[Math.floor((start + end) / 2)];
          idx = i + 1;
          continue;
        }
        const avgX = points[endCap].x;
        const avgY = points[endCap].y;
        let maxArea = -1;
        for (let j = start; j < end; j++) {
          if (points[j].y === null || !Number.isFinite(points[j].y)) continue;
          const area = Math.abs((points[j].x - avgX) * (result[idx].y - points[j].y) - (points[j].x - result[idx].x) * (avgY - points[j].y));
          if (area > maxArea) { maxArea = area; result[i + 1] = points[j]; }
        }
        if (maxArea < 0) result[i + 1] = points[Math.floor((start + end) / 2)];
        idx = i + 1;
      }
      return result;
    }
    function buildDatasets(rows) {
      const grouped = new Map();
      for (const row of rows) {
        if (row.success_count <= 0) continue;
        const ts = timeValue(row);
        if (ts === null) continue;
        const key = targetKey(row);
        if (!grouped.has(key)) grouped.set(key, []);
        grouped.get(key).push({x: ts, y: row.average_latency_ms});
      }
      const datasets = Array.from(grouped.entries()).map(([label, points], index) => {
        points.sort((a, b) => a.x - b.x);
        points = lttb(points, 600);
        const typicalGap = medianInterval(points);
        return {
          label,
          data: points,
          borderColor: colors[index % colors.length],
          typicalGap
        };
      });
      return {datasets};
    }
    function dataRange(chartData) {
      let min = Infinity;
      let max = -Infinity;
      chartData.datasets.forEach(dataset => {
        dataset.data.forEach(point => {
          if (point.y === null) return;
          min = Math.min(min, point.x);
          max = Math.max(max, point.x);
        });
      });
      return Number.isFinite(min) && Number.isFinite(max) && min < max ? {min, max} : null;
    }
    function clampViewRange(range) {
      if (!chartFullRange || !range) return null;
      const fullSpan = chartFullRange.max - chartFullRange.min;
      let span = Math.min(range.max - range.min, fullSpan);
      const minSpan = Math.max(60 * 1000, fullSpan / 200);
      span = Math.max(span, Math.min(minSpan, fullSpan));
      let min = range.min;
      let max = range.max;
      if (min < chartFullRange.min) {
        min = chartFullRange.min;
        max = min + span;
      }
      if (max > chartFullRange.max) {
        max = chartFullRange.max;
        min = max - span;
      }
      return {min, max};
    }
    function updateZoomButtons() {
      const zoomIn = document.getElementById('zoomInButton');
      const zoomOut = document.getElementById('zoomOutButton');
      const zoomReset = document.getElementById('zoomResetButton');
      if (!zoomIn || !zoomOut || !zoomReset) return;
      const canZoom = Boolean(chartFullRange);
      zoomIn.disabled = !canZoom;
      zoomOut.disabled = !canZoom || chartViewRange === null;
      zoomReset.disabled = !canZoom || chartViewRange === null;
    }
    function applyDetailChartRange(mode) {
      if (!detailChart) return;
      const xScale = detailChart.options.scales.x;
      if (chartViewRange) {
        xScale.min = chartViewRange.min;
        xScale.max = chartViewRange.max;
      } else {
        delete xScale.min;
        delete xScale.max;
      }
      detailChart.update(mode || 'none');
      updateZoomButtons();
    }
    function syncChartRange(chartData) {
      const nextFullRange = dataRange(chartData);
      chartFullRange = nextFullRange;
      chartViewRange = clampViewRange(chartViewRange);
      if (!chartFullRange) chartViewRange = null;
    }
    function zoomDetailChart(factor, center) {
      if (!chartFullRange) return;
      const current = chartViewRange || chartFullRange;
      const currentSpan = current.max - current.min;
      const fullSpan = chartFullRange.max - chartFullRange.min;
      const minSpan = Math.max(60 * 1000, fullSpan / 200);
      const nextSpan = Math.max(Math.min(currentSpan * factor, fullSpan), Math.min(minSpan, fullSpan));
      if (nextSpan >= fullSpan) {
        chartViewRange = null;
        applyDetailChartRange();
        return;
      }
      const pivot = Number.isFinite(center) ? center : (current.min + current.max) / 2;
      const ratio = (pivot - current.min) / currentSpan;
      chartViewRange = clampViewRange({
        min: pivot - nextSpan * ratio,
        max: pivot + nextSpan * (1 - ratio)
      });
      applyDetailChartRange();
    }
    function touchDistance(touches) {
      const dx = touches[0].clientX - touches[1].clientX;
      const dy = touches[0].clientY - touches[1].clientY;
      return Math.hypot(dx, dy);
    }
    function touchCenterX(touches) {
      return (touches[0].clientX + touches[1].clientX) / 2;
    }
    function chartValueAtClientX(chart, clientX) {
      const rect = chart.canvas.getBoundingClientRect();
      const scale = chart.scales.x;
      return scale.getValueForPixel(clientX - rect.left);
    }
    function panChartByPixels(chart, dx) {
      if (!panStart || !chartViewRange) return;
      const area = chart.chartArea();
      const width = Math.max(1, area.right - area.left);
      const span = panStart.range.max - panStart.range.min;
      const delta = -dx / width * span;
      chartViewRange = clampViewRange({
        min: panStart.range.min + delta,
        max: panStart.range.max + delta
      });
      applyDetailChartRange();
    }
    function chartTooltip() {
      let tooltip = document.getElementById('chartTooltip');
      if (tooltip) return tooltip;
      tooltip = document.createElement('div');
      tooltip.id = 'chartTooltip';
      tooltip.className = 'chart-tooltip';
      document.body.appendChild(tooltip);
      return tooltip;
    }
    function hideChartTooltip(chart) {
      const tooltip = document.getElementById('chartTooltip');
      if (tooltip) tooltip.style.opacity = '0';
      if (chart) {
        chart.tooltipActive = false;
        cancelAnimationFrame(chart.tooltipRaf);
        chart.hoverX = null;
        chart.lastTooltipPixel = null;
        chart.lastPointerClientY = null;
        chart.updateHoverLine();
      }
    }
    function showChartTooltip(chart, clientX, clientY) {
      const tooltipData = chart.tooltipPoints(clientX);
      const tooltip = chartTooltip();
      if (!tooltipData.items.length) {
        tooltip.style.opacity = '0';
        chart.hoverX = null;
        chart.updateHoverLine();
        chart.lastTooltipX = null;
        return;
      }
      chart.hoverX = tooltipData.xValue;
      chart.updateHoverLine();
      const positionTooltip = () => {
        tooltip.style.opacity = '1';
        tooltip.style.left = '0px';
        tooltip.style.top = '0px';
        const tooltipRect = tooltip.getBoundingClientRect();
        let left = clientX + 14;
        let top = clientY + 14;
        if (left + tooltipRect.width > window.innerWidth - 12) {
          left = clientX - tooltipRect.width - 14;
        }
        if (top + tooltipRect.height > window.innerHeight - 12) {
          top = window.innerHeight - tooltipRect.height - 12;
        }
        tooltip.style.left = Math.max(12, left) + 'px';
        tooltip.style.top = Math.max(12, top) + 'px';
      };
      if (chart.lastTooltipX === tooltipData.xValue) {
        positionTooltip();
        return;
      }
      chart.lastTooltipX = tooltipData.xValue;
      const compact = window.matchMedia('(max-width: 760px), (pointer: coarse)').matches;
      const maxItems = compact ? 8 : 18;
      let html = '<div class="chart-tooltip-title">' + new Date(tooltipData.xValue).toLocaleString() + '</div>';
      const visible = tooltipData.items.slice(0, maxItems);
      for (const item of visible) {
        html += '<div class="chart-tooltip-row"><span class="chart-tooltip-swatch" style="background:' + item.dataset.borderColor + '"></span><span class="chart-tooltip-name">' + item.dataset.label + '</span><span class="chart-tooltip-value">' + item.point.y.toFixed(2) + ' ms</span></div>';
      }
      const hiddenCount = tooltipData.items.length - maxItems;
      if (hiddenCount > 0) html += '<div class="chart-tooltip-more">\u8fd8\u6709 ' + hiddenCount + ' \u9879</div>';
      tooltip.innerHTML = html;
      positionTooltip();
    }
    function attachChartZoomHandlers(chart) {
      const canvas = chart.canvas;
      canvas.addEventListener('touchstart', event => {
        hideChartTooltip(chart);
        if (event.touches.length === 1 && chartViewRange) {
          panStart = {
            x: event.touches[0].clientX,
            y: event.touches[0].clientY,
            range: chartViewRange
          };
          return;
        }
        if (event.touches.length === 2) {
          pinchStart = {
            distance: touchDistance(event.touches),
            range: chartViewRange || chartFullRange,
            center: chartValueAtClientX(chart, touchCenterX(event.touches))
          };
          panStart = null;
        }
      }, {passive: true});
      canvas.addEventListener('touchmove', event => {
        if (panStart && chartViewRange && event.touches.length === 1) {
          const touch = event.touches[0];
          const dx = touch.clientX - panStart.x;
          const dy = touch.clientY - panStart.y;
          if (Math.abs(dx) < 6 || Math.abs(dx) < Math.abs(dy)) return;
          event.preventDefault();
          panChartByPixels(chart, dx);
          return;
        }
        if (!pinchStart || event.touches.length !== 2 || !pinchStart.range) return;
        event.preventDefault();
        const distance = touchDistance(event.touches);
        if (distance <= 0) return;
        const factor = pinchStart.distance / distance;
        const span = pinchStart.range.max - pinchStart.range.min;
        const nextSpan = span * factor;
        const ratio = (pinchStart.center - pinchStart.range.min) / span;
        chartViewRange = clampViewRange({
          min: pinchStart.center - nextSpan * ratio,
          max: pinchStart.center + nextSpan * (1 - ratio)
        });
        applyDetailChartRange();
      }, {passive: false});
      canvas.addEventListener('touchend', event => {
        if (event.touches.length < 2) pinchStart = null;
        if (event.touches.length === 0) panStart = null;
        hideChartTooltip(chart);
      });
      canvas.addEventListener('touchcancel', () => {
        panStart = null;
        pinchStart = null;
        hideChartTooltip(chart);
      });
      canvas.addEventListener('wheel', event => {
        if (!event.ctrlKey && !event.metaKey) return;
        hideChartTooltip(chart);
        event.preventDefault();
        zoomDetailChart(event.deltaY < 0 ? 0.75 : 1.35, chartValueAtClientX(chart, event.clientX));
      }, {passive: false});
      canvas.addEventListener('mousedown', event => {
        if (event.button !== 0 || !chartViewRange) return;
        event.preventDefault();
        panStart = {
          x: event.clientX,
          y: event.clientY,
          range: chartViewRange
        };
        canvas.classList.add('dragging');
      });
      window.addEventListener('mousemove', event => {
        if (!panStart || !chartViewRange || event.buttons !== 1) return;
        event.preventDefault();
        panChartByPixels(chart, event.clientX - panStart.x);
      });
      window.addEventListener('mouseup', () => {
        panStart = null;
        canvas.classList.remove('dragging');
      });
      window.addEventListener('blur', () => {
        panStart = null;
        pinchStart = null;
        canvas.classList.remove('dragging');
        hideChartTooltip(chart);
      });
      window.addEventListener('scroll', () => hideChartTooltip(chart), {passive: true});
    }
    function summarizeAgent(rows) {
      const latest = rows[rows.length - 1];
      const totalSuccess = rows.reduce((sum, row) => sum + row.success_count, 0);
      const totalFailure = rows.reduce((sum, row) => sum + row.failure_count, 0);
      const total = totalSuccess + totalFailure;
      const successRate = total ? totalSuccess / total : 0;
      const latencies = rows.filter(row => row.success_count > 0).map(row => row.average_latency_ms);
      const averageLatency = latencies.length ? latencies.reduce((a, b) => a + b, 0) / latencies.length : 0;
      const targets = new Set(rows.map(targetKey));
      return {latest, successRate, averageLatency, targetCount: targets.size};
    }
    function findAgentStatus(agent) {
      return currentAgents.find(item => item.agent === agent) || null;
    }
    function agentStatusLabel(status, summary) {
      const lastSeen = agentLastSeenTime(status, summary);
      const offlineAfter = Number((status && status.offline_after_seconds) || defaultOfflineAfterSeconds || 90) * 1000;
      if (lastSeen !== null && Date.now() - lastSeen > offlineAfter) return {text: '离线', className: 'offline'};
      if (!summary) return {text: '暂无数据', className: 'idle'};
      return summary.successRate >= 0.99 ? {text: '正常', className: 'ok'} : {text: '异常', className: 'bad'};
    }
    function agentStateBadgeHTML(state) {
      return '<span class="status ' + state.className + '">' + state.text + '</span>';
    }
    function agentLastSeenTime(status, summary) {
      const raw = status && status.last_seen_at ? status.last_seen_at : summary && summary.latest && summary.latest.checked_at;
      if (!raw) return null;
      const ts = new Date(raw).getTime();
      return Number.isNaN(ts) ? null : ts;
    }
    function lastSeenText(status, summary) {
      const ts = agentLastSeenTime(status, summary);
      return ts === null ? '未知' : new Date(ts).toLocaleString();
    }
    function agentIPText(status, summary) {
      return (status && status.agent_ip) || (summary && summary.latest && summary.latest.agent_ip) || '未知';
    }
    function metric(label, value) {
      const div = document.createElement('div');
      div.className = 'metric';
      const span = document.createElement('span');
      span.textContent = label;
      const strong = document.createElement('strong');
      strong.textContent = value;
      div.append(span, strong);
      return div;
    }
    function scheduleLowPriority(fn) {
      if ('requestIdleCallback' in window) {
        window.requestIdleCallback(fn, {timeout: 800});
        return;
      }
      requestAnimationFrame(() => setTimeout(fn, 0));
    }
    function destroyMiniCharts() {
      if (miniChartObserver) {
        miniChartObserver.disconnect();
        miniChartObserver = null;
      }
      miniCharts.forEach(chart => chart.destroy());
      miniCharts.clear();
    }
    function destroyMiniChartSurface(surface) {
      if (!surface) return;
      if (miniChartObserver) miniChartObserver.unobserve(surface);
      surface.__chartRequest = (surface.__chartRequest || 0) + 1;
      if (surface.__miniChart) {
        miniCharts.delete(surface.__miniChart);
        surface.__miniChart.destroy();
        delete surface.__miniChart;
      }
      delete surface.__chartAgent;
    }
    function renderMiniChart(surface, rows) {
      if (!surface.isConnected) return;
      const chartData = buildDatasets(rows);
      if (!chartData.datasets.length) {
        destroyMiniChartSurface(surface);
        surface.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--muted);font-size:12px">暂无数据</div>';
        return;
      }
      if (surface.__miniChart) {
        surface.__miniChart.setData(chartData);
        return;
      }
      const miniChart = new CanvasLineChart(surface, chartData, {mini: true, smooth: true, deferUpdate: true, scales: {x: {}}});
      surface.__miniChart = miniChart;
      miniCharts.add(miniChart);
      miniChart.update();
    }
    async function loadMiniChart(surface, agent) {
      if (!surface || !surface.isConnected) return;
      const request = (surface.__chartRequest || 0) + 1;
      surface.__chartRequest = request;
      try {
        const rows = await loadResults(agent);
        if (!surface.isConnected || surface.__chartRequest !== request || surface.closest('.agent-card')?.dataset.agent !== agent) return;
        renderMiniChart(surface, rows);
      } catch (err) {
        if (!surface.isConnected || surface.__chartRequest !== request || surface.__miniChart) return;
        surface.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--muted);font-size:12px">加载失败</div>';
        console.warn(err);
      }
    }
    function queueMiniChart(surface, agent) {
      if (!surface) return;
      if (surface.__miniChart) {
        loadMiniChart(surface, agent);
        return;
      }
      if (!('IntersectionObserver' in window)) {
        scheduleLowPriority(() => loadMiniChart(surface, agent));
        return;
      }
      if (!miniChartObserver) {
        miniChartObserver = new IntersectionObserver(entries => {
          entries.forEach(entry => {
            if (!entry.isIntersecting) return;
            if (miniChartObserver) miniChartObserver.unobserve(entry.target);
            const agent = entry.target.__chartAgent || '';
            delete entry.target.__chartAgent;
            scheduleLowPriority(() => loadMiniChart(entry.target, agent));
          });
        }, {rootMargin: '320px 0px'});
      }
      surface.__chartAgent = agent;
      miniChartObserver.observe(surface);
    }
    function renderLanding(rows) {
      const wrap = document.getElementById('agentCards');
      const existingCards = new Map(Array.from(wrap.querySelectorAll('.agent-card')).map(card => [card.dataset.agent, card]));
      const groups = new Map();
      for (const row of rows) {
        if (!groups.has(row.agent)) groups.set(row.agent, []);
        groups.get(row.agent).push(row);
      }
      const agentNames = new Set(currentAgents.map(agent => agent.agent).filter(Boolean));
      groups.forEach((_, agent) => agentNames.add(agent));
      if (!agentNames.size) {
        destroyMiniCharts();
        wrap.innerHTML = '<div class="panel">暂无节点在线数据</div>';
        return;
      }
      wrap.querySelectorAll('.panel').forEach(panel => panel.remove());
      const activeAgents = new Set();
      Array.from(agentNames).sort((a, b) => a.localeCompare(b)).forEach(agent => {
        const agentRows = groups.get(agent) || [];
        const summary = agentRows.length ? summarizeAgent(agentRows) : null;
        const statusInfo = findAgentStatus(agent);
        const state = agentStatusLabel(statusInfo, summary);
        const detailURL = '/dashboard?agent=' + encodeURIComponent(agent) + '&range=' + encodeURIComponent(selectedRange);
        activeAgents.add(agent);
        let card = existingCards.get(agent);
        if (!card) {
          card = document.createElement('div');
          card.className = 'agent-card';
          card.tabIndex = 0;
          card.setAttribute('role', 'link');
          card.innerHTML =
            '<div class="card-head"><div><div class="agent-name"></div><div class="subtle"></div></div><span class="status"></span></div>' +
            '<div class="metrics"></div><div class="mini-chart chart-surface"></div>';
          card.addEventListener('click', event => {
            if (card.dataset.caching === '1') { event.preventDefault(); return; }
            location.href = card.dataset.href;
          });
          card.addEventListener('keydown', event => {
            if (event.key !== 'Enter' && event.key !== ' ') return;
            event.preventDefault();
            if (card.dataset.caching === '1') return;
            location.href = card.dataset.href;
          });
        }
        card.dataset.agent = agent;
        card.dataset.href = detailURL;
        card.querySelector('.agent-name').textContent = agent;
        card.querySelector('.subtle').textContent = '节点 IP：' + agentIPText(statusInfo, summary) + ' · 最后在线：' + lastSeenText(statusInfo, summary);
        const status = card.querySelector('.status');
        status.textContent = state.text;
        status.className = 'status ' + state.className;
        card.dataset.caching = '0';
        const metrics = card.querySelector('.metrics');
        metrics.replaceChildren();
        metrics.append(metric('目标数', summary ? String(summary.targetCount) : '0'));
        metrics.append(metric('成功率', summary ? (summary.successRate * 100).toFixed(1) + '%' : '--'));
        metrics.append(metric('平均延迟', summary ? summary.averageLatency.toFixed(1) + ' ms' : '--'));
        wrap.appendChild(card);
        const chartSurface = card.querySelector('.mini-chart');
        if (agentRows.length) {
          queueMiniChart(chartSurface, agent);
        } else if (cacheState === 'building' || cacheState === 'none') {
          destroyMiniChartSurface(chartSurface);
          chartSurface.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--muted);font-size:12px">缓存生成中&hellip;</div>';
        } else {
          destroyMiniChartSurface(chartSurface);
          chartSurface.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--muted);font-size:12px">暂无数据</div>';
        }
      });
      existingCards.forEach((card, agent) => {
        if (activeAgents.has(agent)) return;
        destroyMiniChartSurface(card.querySelector('.mini-chart'));
        card.remove();
      });
    }
    function renderAgentInfo(rows) {
      const wrap = document.getElementById('agentInfo');
      wrap.innerHTML = '';
      const statusInfo = findAgentStatus(selectedAgent);
      const summary = rows.length ? summarizeAgent(rows) : null;
      const state = agentStatusLabel(statusInfo, summary);
      const deleteButton = document.getElementById('deleteAgentButton');
      if (deleteButton) deleteButton.hidden = state.className !== 'offline';
      const subtitle = document.getElementById('pageSubtitle');
      subtitle.innerHTML = '最后在线：' + lastSeenText(statusInfo, summary) + '<span class="live-badge" id="liveState">实时</span>' + agentStateBadgeHTML(state);
      wrap.append(metric('节点名称', (summary && summary.latest.agent) || (statusInfo && statusInfo.agent) || selectedAgent));
      wrap.append(metric('节点 IP', agentIPText(statusInfo, summary)));
      wrap.append(metric('监测目标', summary ? String(summary.targetCount) : '0'));
      wrap.append(metric('最后在线', lastSeenText(statusInfo, summary)));
    }
    function problemSeverity(row) {
      return row.success_count === 0 || row.success_rate === 0 ? 'ERROR' : 'WARN';
    }
    function isProblemRow(row) {
      return row.failure_count > 0 || row.success_rate < 1 || Boolean(row.error);
    }
    function renderProblemLog(rows) {
      const tbody = document.getElementById('problemLogBody');
      const meta = document.getElementById('logMeta');
      if (!tbody) return;
      const problems = rows.filter(isProblemRow).slice().reverse().slice(0, maxProblemLogRows);
      tbody.innerHTML = '';
      if (meta) meta.textContent = '仅显示最近 ' + problems.length + ' 条 WARN / ERROR，最多 ' + maxProblemLogRows + ' 条';
      if (!problems.length) {
        const tr = document.createElement('tr');
        const td = document.createElement('td');
        td.colSpan = 8;
        td.textContent = '暂无 WARN / ERROR';
        tr.appendChild(td);
        tbody.appendChild(tr);
        return;
      }
      for (const row of problems) {
        const tr = document.createElement('tr');
        const cells = [
          new Date(row.checked_at).toLocaleString(),
          problemSeverity(row),
          row.agent_ip || '未知',
          row.target_name,
          row.address + ':' + row.port,
          (row.success_rate * 100).toFixed(1) + '%',
          row.success_count > 0 ? row.average_latency_ms.toFixed(2) + ' ms' : '--',
          row.error || ''
        ];
        cells.forEach((value, index) => {
          const td = document.createElement('td');
          td.textContent = value;
          if (index === 1) td.className = value === 'ERROR' ? 'bad' : 'warn';
          if (index === 5) td.className = row.success_rate > 0.99 ? 'ok' : 'bad';
          tr.appendChild(td);
        });
        tbody.appendChild(tr);
      }
    }
    function renderToggles(chart) {
      const wrap = document.getElementById('targetToggles');
      wrap.innerHTML = '';
      if (!chart.data.datasets.length) {
        wrap.textContent = '当前 label 下暂无可绘制目标';
        return;
      }
      const visibleLabels = new Set(chart.data.datasets.map(dataset => dataset.label));
      targetVisibility = new Map(Array.from(targetVisibility.entries()).filter(([label]) => visibleLabels.has(label)));
      chart.data.datasets.forEach((dataset, index) => {
        const label = document.createElement('label');
        const input = document.createElement('input');
        input.type = 'checkbox';
        input.checked = targetVisibility.has(dataset.label) ? targetVisibility.get(dataset.label) : true;
        chart.setDatasetVisibility(index, input.checked);
        input.addEventListener('change', () => {
          targetVisibility.set(dataset.label, input.checked);
          chart.setDatasetVisibility(index, input.checked);
          chart.update();
        });
        label.append(input, document.createTextNode(dataset.label));
        wrap.appendChild(label);
      });
    }
    function updateDetailChart(rows) {
      const chartData = buildDatasets(filterRowsByLabels(rows, selectedLabels));
      syncChartRange(chartData);
      if (detailChart) {
        detailChart.data = chartData;
        renderToggles(detailChart);
        applyDetailChartRange('none');
        return;
      }
      detailChart = new CanvasLineChart(document.getElementById('latency'), chartData, {mini: false, smooth: true, deferUpdate: true, scales: {x: {}}});
      attachChartZoomHandlers(detailChart);
      renderToggles(detailChart);
      applyDetailChartRange('none');
    }
    function renderLabelFilters(rows) {
      const wrap = document.getElementById('labelFilters');
      if (!wrap) return;
      const labels = availableLabels(rows);
      wrap.innerHTML = '';
      if (!labels.length) {
        wrap.textContent = '暂无 label';
        selectedLabels = null;
        return;
      }
      const valid = new Set(labels);
      if (selectedLabels === null) {
        selectedLabels = new Set(labels);
      } else {
        selectedLabels = new Set(Array.from(selectedLabels).filter(label => valid.has(label)));
      }
      labels.forEach(labelText => {
        const label = document.createElement('label');
        const input = document.createElement('input');
        input.type = 'checkbox';
        input.checked = selectedLabels.has(labelText);
        input.addEventListener('change', () => {
          if (input.checked) {
            selectedLabels.add(labelText);
          } else {
            selectedLabels.delete(labelText);
          }
          updateDetailChart(currentAgentRows);
        });
        label.append(input, document.createTextNode('label: ' + labelText));
        wrap.appendChild(label);
      });
    }
    function renderDashboardRows(rows) {
      currentRows = sortRowsByTime(rowsForCurrentView(rows));
      if (!selectedAgent) {
        renderLanding(currentRows);
        return;
      }
      if (!currentRows.length) {
        currentAgentRows = [];
        renderAgentInfo(currentRows);
        renderProblemLog(currentRows);
        renderLabelFilters(currentRows);
        updateDetailChart(currentRows);
        return;
      }
      currentAgentRows = currentRows;
      renderAgentInfo(currentRows);
      renderProblemLog(currentRows);
      renderLabelFilters(currentRows);
      updateDetailChart(currentRows);
    }
    async function refreshDashboard() {
      const sequence = ++dashboardRefreshSequence;
      const scrollX = window.scrollX;
      const scrollY = window.scrollY;
      const [rows, agents] = await Promise.all([loadResults(), loadAgents()]);
      if (sequence !== dashboardRefreshSequence) return;
      currentAgents = agents;
      _agentHash = agentHash(agents);
      renderDashboardRows(rows);
      requestAnimationFrame(() => window.scrollTo(scrollX, scrollY));
    }
    function scheduleLiveRefresh() {
      clearTimeout(liveRefreshTimer);
      liveRefreshTimer = window.setTimeout(() => {
        liveRefreshTimer = null;
        refreshDashboard().catch(handleRefreshError);
      }, 120);
    }
    function handleRefreshError(err) {
      const targetToggles = document.getElementById('targetToggles');
      const agentCards = document.getElementById('agentCards');
      if (targetToggles) targetToggles.textContent = '加载失败：' + err.message;
      if (agentCards) agentCards.innerHTML = '<div class="panel">加载失败：' + err.message + '</div>';
    }
    function updateLiveState(text, reconnecting) {
      const liveState = document.getElementById('liveState');
      if (!liveState) return;
      liveState.textContent = text;
      liveState.classList.toggle('reconnecting', reconnecting);
    }
    document.getElementById('refreshButton').addEventListener('click', () => {
      refreshDashboard().catch(handleRefreshError);
    });
    const deleteAgentButton = document.getElementById('deleteAgentButton');
    if (deleteAgentButton) {
      deleteAgentButton.addEventListener('click', () => deleteAgent(selectedAgent).catch(handleRefreshError));
    }
    const zoomInButton = document.getElementById('zoomInButton');
    const zoomOutButton = document.getElementById('zoomOutButton');
    const zoomResetButton = document.getElementById('zoomResetButton');
    if (zoomInButton) zoomInButton.addEventListener('click', () => zoomDetailChart(0.65));
    if (zoomOutButton) zoomOutButton.addEventListener('click', () => zoomDetailChart(1.55));
    if (zoomResetButton) zoomResetButton.addEventListener('click', () => {
      chartViewRange = null;
      applyDetailChartRange();
    });
    updateZoomButtons();
    const rangeMenu = document.getElementById('rangeMenu');
    const rangeButton = document.getElementById('rangeButton');
    const rangeCustomForm = document.getElementById('rangeCustomForm');
    const rangeCustomInput = document.getElementById('rangeCustomInput');
    const rangeCustomApply = document.getElementById('rangeCustomApply');
    const backButton = document.getElementById('backButton');
    const rangePresets = new Set(Array.from(document.querySelectorAll('.range-option')).map(option => option.dataset.range));
    const rangeCookieName = 'pingmon_range';
    const customRangeCookieName = 'pingmon_custom_range';
    rangeButton.addEventListener('click', () => rangeMenu.classList.toggle('open'));
    function setCookie(name, value, maxAgeSeconds) {
      document.cookie = name + '=' + encodeURIComponent(value) + '; Max-Age=' + maxAgeSeconds + '; Path=/; SameSite=Lax';
    }
    function getCookie(name) {
      const prefix = name + '=';
      return document.cookie.split(';').map(part => part.trim()).find(part => part.startsWith(prefix))?.slice(prefix.length) || '';
    }
    function normalizeRange(raw) {
      const value = String(raw || '').trim().toLowerCase();
      return /^\d+(m|h|d|w|mo)$/.test(value) && parseRangeMillis(value) > 0 ? value : '';
    }
    function applyRange(nextRange) {
      if (!selectedAgent && parseRangeMillis(nextRange) > 24 * 60 * 60 * 1000) {
        nextRange = '24h';
      }
      selectedRange = nextRange;
      rangeButton.textContent = selectedRange;
      setCookie(rangeCookieName, selectedRange, 365 * 24 * 60 * 60);
      if (rangeCustomInput) rangeCustomInput.value = rangePresets.has(selectedRange) ? '' : selectedRange;
      if (rangePresets.has(selectedRange)) {
        setCookie(customRangeCookieName, '', 0);
      } else {
        setCookie(customRangeCookieName, selectedRange, 365 * 24 * 60 * 60);
      }
      document.querySelectorAll('.range-option').forEach(item => item.classList.toggle('active', item.dataset.range === selectedRange));
      rangeMenu.classList.remove('open');
      rangeMenu.classList.remove('invalid');
      hideChartTooltip(detailChart);
      chartViewRange = null;
      const url = new URL(location.href);
      url.searchParams.set('range', selectedRange);
      history.replaceState(null, '', url);
      if (backButton) backButton.href = '/dashboard?range=' + encodeURIComponent(selectedRange);
      refreshDashboard().catch(handleRefreshError);
    }
    document.querySelectorAll('.range-option').forEach(option => {
      option.addEventListener('click', () => {
        applyRange(option.dataset.range);
      });
    });
    if (!selectedAgent) {
      document.querySelectorAll('.range-option').forEach(option => {
        if (parseRangeMillis(option.dataset.range) > 24 * 60 * 60 * 1000) {
          option.hidden = true;
        }
      });
    }
    if (rangeCustomForm && rangeCustomInput && rangeCustomApply) {
      function submitCustomRange() {
        const nextRange = normalizeRange(rangeCustomInput.value);
        if (!nextRange) {
          rangeMenu.classList.add('invalid');
          rangeCustomInput.focus();
          return;
        }
        applyRange(nextRange);
      }
      rangeCustomApply.addEventListener('click', submitCustomRange);
      rangeCustomInput.addEventListener('keydown', event => {
        if (event.key !== 'Enter') return;
        event.preventDefault();
        submitCustomRange();
      });
      rangeCustomInput.addEventListener('input', () => rangeMenu.classList.remove('invalid'));
      const savedCustomRange = normalizeRange(decodeURIComponent(getCookie(customRangeCookieName)));
      if (savedCustomRange && rangePresets.has(selectedRange)) {
        rangeCustomInput.value = savedCustomRange;
      }
    }
    document.addEventListener('click', event => {
      if (!rangeMenu.contains(event.target)) rangeMenu.classList.remove('open');
      const chartTooltip = document.getElementById('chartTooltip');
      if (chartTooltip && !event.target.closest('.chart-surface')) hideChartTooltip(detailChart);
    });
    document.addEventListener('touchstart', event => {
      if (!rangeMenu.contains(event.target)) rangeMenu.classList.remove('open');
      if (!event.target.closest('.chart-surface')) hideChartTooltip(detailChart);
    }, {passive: true});
    window.addEventListener('resize', () => {
      rangeMenu.classList.remove('open');
      hideChartTooltip(detailChart);
    });
    refreshDashboard().catch(handleRefreshError);
    let _agentHash = '';
    function agentHash(agents) {
      return agents.map(a => a.agent + '\x00' + a.status).sort().join('|');
    }
    window.setInterval(async () => {
      try {
        const agents = await loadAgents();
        const hash = agentHash(agents);
        if (hash !== _agentHash) {
          _agentHash = hash;
          currentAgents = agents;
          renderDashboardRows(currentRows);
        }
      } catch (err) {
        console.warn(err);
      }
    }, 15000);
    function connectLiveRefresh() {
      const proto = location.protocol === 'https:' ? 'wss://' : 'ws://';
      const ws = new WebSocket(proto + location.host + '/ws');
      ws.onopen = () => {
        updateLiveState('实时', false);
      };
      ws.onmessage = event => {
        if (event.data === 'connected') return;
        if (event.data === 'refresh') {
          refreshDashboard().catch(handleRefreshError);
          return;
        }
        try {
          const message = JSON.parse(event.data);
          if (message.type === 'results') scheduleLiveRefresh();
        } catch (err) {
          console.warn('未知实时消息', err);
        }
      };
      ws.onclose = () => {
        updateLiveState('重连中', true);
        setTimeout(connectLiveRefresh, 3000);
      };
      ws.onerror = () => ws.close();
    }
    connectLiveRefresh();
