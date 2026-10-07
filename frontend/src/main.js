import { PRESETS } from './presets.js';
import {
  testKafkaConnection,
  previewTemplate,
  startJob,
  pauseJob,
  resumeJob,
  stopJob,
  getJobStatus,
  subscribeToStats
} from './api.js';

// Application State
const state = {
  currentStatus: 'Idle',
  rateMode: 'msg_per_sec', // 'msg_per_sec' | 'interval_ms'
  limitMode: 'continuous', // 'continuous' | 'fixed'
  rateHistory: new Array(30).fill(0),
  selectedPresetId: 'ecommerce',
  activeHeaders: []
};

// DOM Elements
const el = {
  brokerStatusCard: document.getElementById('broker-status-card'),
  brokerStatusText: document.getElementById('broker-status-text'),
  btnTestConnection: document.getElementById('btn-test-connection'),

  jobStatusBadge: document.getElementById('job-status-badge'),
  jobStatusText: document.getElementById('job-status-text'),

  metricSent: document.getElementById('metric-sent'),
  metricTargetProgress: document.getElementById('metric-target-progress'),
  metricRate: document.getElementById('metric-rate'),
  metricRateTarget: document.getElementById('metric-rate-target'),
  metricFailed: document.getElementById('metric-failed'),
  metricErrorRate: document.getElementById('metric-error-rate'),
  metricElapsed: document.getElementById('metric-elapsed'),
  progressBarFill: document.getElementById('progress-bar-fill'),
  sparklineCanvas: document.getElementById('sparkline-chart'),

  btnStart: document.getElementById('btn-start-job'),
  btnPause: document.getElementById('btn-pause-job'),
  btnStop: document.getElementById('btn-stop-job'),
  btnReset: document.getElementById('btn-reset-job'),
  statusAlertBox: document.getElementById('status-alert-box'),
  statusAlertText: document.getElementById('status-alert-text'),

  inputRateValue: document.getElementById('input-rate-value'),
  inputQuickSlider: document.getElementById('input-quick-slider'),
  rateValueLabel: document.getElementById('rate-value-label'),
  fixedCountContainer: document.getElementById('fixed-count-container'),
  inputTotalMessages: document.getElementById('input-total-messages'),

  inputBootstrapServers: document.getElementById('input-bootstrap-servers'),
  inputTopic: document.getElementById('input-topic'),
  selectKeyStrategy: document.getElementById('select-key-strategy'),
  keyFieldContainer: document.getElementById('key-field-container'),
  keyFieldLabel: document.getElementById('key-field-label'),
  inputKeyValue: document.getElementById('input-key-value'),

  toggleAdvancedKafka: document.getElementById('toggle-advanced-kafka'),
  advancedKafkaSettings: document.getElementById('advanced-kafka-settings'),
  selectAcks: document.getElementById('select-acks'),
  selectCompression: document.getElementById('select-compression'),
  inputLingerMs: document.getElementById('input-linger-ms'),
  inputBatchSize: document.getElementById('input-batch-size'),
  btnAddHeader: document.getElementById('btn-add-header'),
  headersList: document.getElementById('headers-list'),

  presetsChips: document.getElementById('presets-chips'),
  btnSavePreset: document.getElementById('btn-save-preset'),
  templateEditor: document.getElementById('template-editor'),
  btnFormatJson: document.getElementById('btn-format-json'),
  btnPreviewPayload: document.getElementById('btn-preview-payload'),

  previewBox: document.getElementById('preview-box'),
  previewValidationBadge: document.getElementById('preview-validation-badge'),
  previewSamplesContainer: document.getElementById('preview-samples-container'),
  btnClosePreview: document.getElementById('btn-close-preview'),

  chkAutoScroll: document.getElementById('chk-auto-scroll'),
  btnClearStream: document.getElementById('btn-clear-stream'),
  emptyStreamPlaceholder: document.getElementById('empty-stream-placeholder'),
  messagesList: document.getElementById('messages-list')
};

// Canvas 2D context for sparkline
const ctx = el.sparklineCanvas.getContext('2d');

// --- Initialization ---
function init() {
  renderPresets();
  applyPreset('ecommerce');
  setupEventListeners();
  setupTokenPalette();
  drawSparkline();
  checkBrokerConnection();

  // Subscribe to real-time SSE stream
  subscribeToStats(handleStatsUpdate, (err) => {
    console.warn('SSE stream disconnected, polling fallback active', err);
  });

  // Background fallback poll every 1 second
  setInterval(async () => {
    try {
      const res = await getJobStatus();
      if (res.data) handleStatsUpdate(res.data);
    } catch (_) {}
  }, 1000);

}

// --- Presets Management ---
function renderPresets() {
  el.presetsChips.innerHTML = '';
  PRESETS.forEach(p => {
    const chip = document.createElement('button');
    chip.type = 'button';
    chip.className = `preset-chip ${p.id === state.selectedPresetId ? 'active' : ''}`;
    chip.dataset.id = p.id;
    chip.textContent = p.name;
    chip.onclick = () => applyPreset(p.id);
    el.presetsChips.appendChild(chip);
  });
}

function applyPreset(presetId) {
  const p = PRESETS.find(item => item.id === presetId);
  if (!p) return;

  state.selectedPresetId = presetId;
  renderPresets();

  el.templateEditor.value = p.template;
  el.inputTopic.value = p.topic;
  el.inputRateValue.value = p.rate;
  el.inputQuickSlider.value = p.rate;
  updateRateDisplay();

  // Key strategy
  el.selectKeyStrategy.value = p.keyStrategy.type;
  handleKeyStrategyChange();
  if (p.keyStrategy.value) {
    el.inputKeyValue.value = p.keyStrategy.value;
  }

  // Headers
  state.activeHeaders = [...(p.headers || [])];
  renderHeaders();
}

// --- Dynamic Header Rows ---
function renderHeaders() {
  el.headersList.innerHTML = '';
  state.activeHeaders.forEach((h, index) => {
    const row = document.createElement('div');
    row.className = 'header-row';
    row.innerHTML = `
      <input type="text" class="form-input" placeholder="Header Key" value="${escapeHtml(h.key)}" data-idx="${index}" data-field="key">
      <input type="text" class="form-input" placeholder="Header Value" value="${escapeHtml(h.value)}" data-idx="${index}" data-field="value">
      <button type="button" class="btn-icon" data-remove="${index}" title="Remove header">✕</button>
    `;
    el.headersList.appendChild(row);
  });

  el.headersList.querySelectorAll('input').forEach(inp => {
    inp.addEventListener('input', (e) => {
      const idx = e.target.dataset.idx;
      const field = e.target.dataset.field;
      state.activeHeaders[idx][field] = e.target.value;
    });
  });

  el.headersList.querySelectorAll('button[data-remove]').forEach(btn => {
    btn.addEventListener('click', (e) => {
      const idx = parseInt(e.currentTarget.dataset.remove, 10);
      state.activeHeaders.splice(idx, 1);
      renderHeaders();
    });
  });
}

// --- Token Palette Insertion ---
function setupTokenPalette() {
  document.querySelectorAll('.token-chip').forEach(chip => {
    chip.addEventListener('click', () => {
      const token = chip.dataset.token;
      insertTokenAtCursor(token);
    });
  });
}

function insertTokenAtCursor(text) {
  const textarea = el.templateEditor;
  const start = textarea.selectionStart;
  const end = textarea.selectionEnd;
  const val = textarea.value;

  textarea.value = val.substring(0, start) + text + val.substring(end);
  textarea.selectionStart = textarea.selectionEnd = start + text.length;
  textarea.focus();
}

// --- Kafka Connection Testing ---
async function checkBrokerConnection() {
  el.brokerStatusCard.className = 'status-badge checking';
  el.brokerStatusText.textContent = 'Pinging Broker...';

  try {
    const servers = el.inputBootstrapServers.value.trim();
    const res = await testKafkaConnection(servers);
    if (res.data && res.data.success) {
      el.brokerStatusCard.className = 'status-badge connected';
      el.brokerStatusText.textContent = `Connected (${res.data.latency_ms}ms, ${res.data.brokers_count} broker${res.data.brokers_count > 1 ? 's' : ''})`;
    } else {
      el.brokerStatusCard.className = 'status-badge disconnected';
      const err = res.data?.error || 'Connection failed';
      el.brokerStatusText.textContent = `Broker unreachable (${err.substring(0, 24)}...)`;
    }
  } catch (err) {
    el.brokerStatusCard.className = 'status-badge disconnected';
    el.brokerStatusText.textContent = 'Connection error';
  }
}

// --- Key Strategy Switcher ---
function handleKeyStrategyChange() {
  const strategy = el.selectKeyStrategy.value;
  if (strategy === 'extract_field') {
    el.keyFieldContainer.classList.remove('hidden');
    el.keyFieldLabel.textContent = 'JSON Field Name';
    el.inputKeyValue.placeholder = 'e.g. order_id';
  } else if (strategy === 'static') {
    el.keyFieldContainer.classList.remove('hidden');
    el.keyFieldLabel.textContent = 'Static Key String';
    el.inputKeyValue.placeholder = 'e.g. partition-key-1';
  } else {
    el.keyFieldContainer.classList.add('hidden');
  }
}

// --- Telemetry & SSE Stats Handler ---
function handleStatsUpdate(stats) {
  state.currentStatus = stats.status;
  updateStatusBadge(stats.status);

  // Counters
  el.metricSent.textContent = stats.sent_count.toLocaleString();
  el.metricFailed.textContent = stats.failed_count.toLocaleString();
  
  const total = stats.sent_count + stats.failed_count;
  const errRate = total > 0 ? ((stats.failed_count / total) * 100).toFixed(1) : '0.0';
  el.metricErrorRate.textContent = `${errRate}% error rate`;

  // Throughput
  const rate = stats.current_rate || 0.0;
  el.metricRate.innerHTML = `${rate.toFixed(1)} <span class="kpi-unit">msg/s</span>`;

  // Sparkline buffer
  state.rateHistory.shift();
  state.rateHistory.push(rate);
  drawSparkline();

  // Elapsed
  el.metricElapsed.textContent = formatDuration(stats.elapsed_seconds || 0);

  // Target and Progress
  if (stats.target_count) {
    el.metricTargetProgress.textContent = `of ${stats.target_count.toLocaleString()} messages`;
    const pct = Math.min(100, Math.round((stats.sent_count / stats.target_count) * 100));
    el.progressBarFill.style.width = `${pct}%`;
  } else {
    el.metricTargetProgress.textContent = 'of ∞ (continuous)';
    el.progressBarFill.style.width = stats.status === 'Running' ? '100%' : '0%';
  }

  // Update controls
  updateButtonsForStatus(stats.status);

  // Update errors if any
  if (stats.last_error && stats.status === 'Running') {
    showAlert(stats.last_error);
  } else if (!stats.last_error) {
    hideAlert();
  }

  // Render recent messages
  if (stats.recent_messages && stats.recent_messages.length > 0) {
    renderRecentMessages(stats.recent_messages);
  }
}

function updateStatusBadge(status) {
  el.jobStatusText.textContent = status.toUpperCase();
  el.jobStatusBadge.className = 'status-pill';

  switch (status) {
    case 'Running':
      el.jobStatusBadge.classList.add('status-running');
      break;
    case 'Paused':
      el.jobStatusBadge.classList.add('status-paused');
      break;
    case 'Completed':
      el.jobStatusBadge.classList.add('status-completed');
      break;
    default:
      el.jobStatusBadge.classList.add('status-idle');
  }
}

function updateButtonsForStatus(status) {
  if (status === 'Running') {
    el.btnStart.disabled = true;
    el.btnPause.disabled = false;
    el.btnPause.innerHTML = `
      <svg viewBox="0 0 24 24" width="16" height="16" fill="currentColor"><rect x="6" y="4" width="4" height="16"/><rect x="14" y="4" width="4" height="16"/></svg>
      Pause
    `;
    el.btnStop.disabled = false;
  } else if (status === 'Paused') {
    el.btnStart.disabled = true;
    el.btnPause.disabled = false;
    el.btnPause.innerHTML = `
      <svg viewBox="0 0 24 24" width="16" height="16" fill="currentColor"><polygon points="5 3 19 12 5 21 5 3"/></svg>
      Resume
    `;
    el.btnStop.disabled = false;
  } else {
    // Idle, Stopped, Completed, Failed
    el.btnStart.disabled = false;
    el.btnPause.disabled = true;
    el.btnStop.disabled = true;
  }
}

// --- Sparkline Renderer ---
function drawSparkline() {
  const width = el.sparklineCanvas.width;
  const height = el.sparklineCanvas.height;
  ctx.clearRect(0, 0, width, height);

  const data = state.rateHistory;
  const max = Math.max(...data, 10);
  const step = width / (data.length - 1);

  ctx.beginPath();
  data.forEach((val, i) => {
    const x = i * step;
    const y = height - (val / max) * (height - 6) - 3;
    if (i === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });

  ctx.strokeStyle = '#38bdf8';
  ctx.lineWidth = 2;
  ctx.lineCap = 'round';
  ctx.stroke();

  // Gradient fill under curve
  ctx.lineTo(width, height);
  ctx.lineTo(0, height);
  ctx.closePath();
  const grad = ctx.createLinearGradient(0, 0, 0, height);
  grad.addColorStop(0, 'rgba(56, 189, 248, 0.25)');
  grad.addColorStop(1, 'rgba(56, 189, 248, 0.0)');
  ctx.fillStyle = grad;
  ctx.fill();
}

// --- Message Inspector Renderer ---
function renderRecentMessages(messages) {
  el.emptyStreamPlaceholder.classList.add('hidden');
  el.messagesList.innerHTML = '';

  messages.forEach(msg => {
    const item = document.createElement('div');
    item.className = 'message-item';

    const timeStr = msg.timestamp ? new Date(msg.timestamp).toLocaleTimeString() : '--:--:--';
    const keyStr = msg.key ? `Key: ${msg.key}` : 'No Key';
    const previewStr = msg.payload_preview.length > 120 
      ? msg.payload_preview.substring(0, 120) + '...' 
      : msg.payload_preview;

    item.innerHTML = `
      <span class="msg-time">${timeStr}</span>
      <span class="msg-partition">Part: ${msg.partition} | Off: ${msg.offset}</span>
      <span class="msg-key" title="${escapeHtml(msg.key || '')}">${escapeHtml(keyStr)}</span>
      <span class="msg-preview" title="${escapeHtml(msg.payload_preview)}">${escapeHtml(previewStr)}</span>
      <button type="button" class="msg-copy-btn">Copy</button>
    `;

    item.querySelector('.msg-copy-btn').onclick = () => {
      navigator.clipboard.writeText(msg.payload_preview);
      item.querySelector('.msg-copy-btn').textContent = 'Copied!';
      setTimeout(() => { item.querySelector('.msg-copy-btn').textContent = 'Copy'; }, 1500);
    };

    el.messagesList.appendChild(item);
  });

  if (el.chkAutoScroll.checked) {
    el.messagesList.scrollTop = el.messagesList.scrollHeight;
  }
}

// --- Event Listeners Setup ---
function setupEventListeners() {
  // Rate slider and input sync
  el.inputRateValue.addEventListener('input', (e) => {
    el.inputQuickSlider.value = e.target.value;
    updateRateDisplay();
  });
  el.inputQuickSlider.addEventListener('input', (e) => {
    el.inputRateValue.value = e.target.value;
    updateRateDisplay();
  });

  // Rate mode toggle
  document.querySelectorAll('.segmented-control .segment-btn[data-mode]').forEach(btn => {
    btn.addEventListener('click', (e) => {
      document.querySelectorAll('.segmented-control .segment-btn[data-mode]').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      state.rateMode = btn.dataset.mode;
      if (state.rateMode === 'msg_per_sec') {
        el.rateValueLabel.textContent = 'Target Throughput (msg/s)';
      } else {
        el.rateValueLabel.textContent = 'Interval Delay (ms)';
      }
      updateRateDisplay();
    });
  });

  // Limit mode toggle
  document.querySelectorAll('#limit-control .segment-btn[data-limit]').forEach(btn => {
    btn.addEventListener('click', (e) => {
      document.querySelectorAll('#limit-control .segment-btn[data-limit]').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      state.limitMode = btn.dataset.limit;
      if (state.limitMode === 'fixed') {
        el.fixedCountContainer.classList.remove('hidden');
      } else {
        el.fixedCountContainer.classList.add('hidden');
      }
    });
  });

  // Key strategy selector
  el.selectKeyStrategy.addEventListener('change', handleKeyStrategyChange);

  // Advanced Kafka toggle
  el.toggleAdvancedKafka.addEventListener('click', () => {
    const isCollapsed = el.advancedKafkaSettings.classList.toggle('collapsed');
    el.toggleAdvancedKafka.textContent = isCollapsed ? 'Advanced Options ▾' : 'Hide Options ▴';
  });

  // Add Header button
  el.btnAddHeader.addEventListener('click', () => {
    state.activeHeaders.push({ key: '', value: '' });
    renderHeaders();
  });

  // Test connection button
  el.btnTestConnection.addEventListener('click', checkBrokerConnection);

  // JSON format button
  el.btnFormatJson.addEventListener('click', () => {
    try {
      const obj = JSON.parse(el.templateEditor.value);
      el.templateEditor.value = JSON.stringify(obj, null, 2);
    } catch (_) {
      showAlert('Cannot format: Template has unquoted tokens or invalid JSON syntax. Normal for template tokens.');
    }
  });

  // Preview button
  el.btnPreviewPayload.addEventListener('click', async () => {
    const template = el.templateEditor.value;
    try {
      const res = await previewTemplate(template, 3);
      if (res.data) {
        showPreviewModal(res.data);
      }
    } catch (err) {
      showAlert('Failed to generate preview: ' + err.message);
    }
  });

  el.btnClosePreview.addEventListener('click', () => {
    el.previewBox.classList.add('hidden');
  });

  // Clear Stream
  el.btnClearStream.addEventListener('click', () => {
    el.messagesList.innerHTML = '';
    el.emptyStreamPlaceholder.classList.remove('hidden');
  });

  // Job Action: Start
  el.btnStart.addEventListener('click', handleStartJob);

  // Job Action: Pause / Resume
  el.btnPause.addEventListener('click', async () => {
    if (state.currentStatus === 'Running') {
      await pauseJob();
      updateStatusBadge('Paused');
      updateButtonsForStatus('Paused');
    } else if (state.currentStatus === 'Paused') {
      await resumeJob();
      updateStatusBadge('Running');
      updateButtonsForStatus('Running');
    }
    const res = await getJobStatus();
    if (res.data) handleStatsUpdate(res.data);
  });

  // Job Action: Stop
  el.btnStop.addEventListener('click', async () => {
    await stopJob();
    updateStatusBadge('Stopped');
    updateButtonsForStatus('Stopped');
    const res = await getJobStatus();
    if (res.data) handleStatsUpdate(res.data);
  });


  // Job Action: Reset
  el.btnReset.addEventListener('click', () => {
    state.rateHistory = new Array(30).fill(0);
    drawSparkline();
    el.metricSent.textContent = '0';
    el.metricFailed.textContent = '0';
    el.metricRate.innerHTML = '0.0 <span class="kpi-unit">msg/s</span>';
    el.metricElapsed.textContent = '00:00:00';
    el.progressBarFill.style.width = '0%';
    el.messagesList.innerHTML = '';
    el.emptyStreamPlaceholder.classList.remove('hidden');
  });
}

function updateRateDisplay() {
  const val = parseFloat(el.inputRateValue.value) || 100;
  if (state.rateMode === 'msg_per_sec') {
    el.metricRateTarget.textContent = `Target: ${val} msg/s`;
  } else {
    el.metricRateTarget.textContent = `Interval: ${val} ms`;
  }
}

// --- Job Start Handler ---
async function handleStartJob() {
  hideAlert();
  const template = el.templateEditor.value.trim();
  if (!template) {
    showAlert('Please provide a valid payload template.');
    return;
  }

  // Key strategy builder
  const keyType = el.selectKeyStrategy.value;
  let keyStrategy;
  if (keyType === 'none') {
    keyStrategy = { type: 'none' };
  } else if (keyType === 'random_uuid') {
    keyStrategy = { type: 'random_uuid' };
  } else if (keyType === 'static') {
    keyStrategy = { type: 'static', value: el.inputKeyValue.value.trim() };
  } else if (keyType === 'extract_field') {
    keyStrategy = { type: 'extract_field', value: el.inputKeyValue.value.trim() || 'id' };
  }

  const filteredHeaders = state.activeHeaders.filter(h => h.key && h.key.trim().length > 0);

  const payload = {
    kafka: {
      bootstrap_servers: el.inputBootstrapServers.value.trim() || 'kafka:29092',
      topic: el.inputTopic.value.trim() || 'mock-events',
      acks: el.selectAcks.value,
      compression: el.selectCompression.value,
      linger_ms: parseInt(el.inputLingerMs.value, 10) || 5,
      batch_size: parseInt(el.inputBatchSize.value, 10) || 65536,
    },
    payload_template: template,
    key_strategy: keyStrategy,
    headers: filteredHeaders,
    rate_limit: {
      mode: state.rateMode,
      value: parseFloat(el.inputRateValue.value) || 100.0,
    },
    total_messages: state.limitMode === 'fixed' ? parseInt(el.inputTotalMessages.value, 10) : null,
    concurrency: 2,
  };

  try {
    const res = await startJob(payload);
    if (!res.success) {
      showAlert(res.message || 'Failed to start stream.');
    } else {
      updateStatusBadge('Running');
      updateButtonsForStatus('Running');
      setTimeout(async () => {
        const st = await getJobStatus();
        if (st.data) handleStatsUpdate(st.data);
      }, 200);
    }
  } catch (err) {
    showAlert('Could not reach backend: ' + err.message);
  }

}

// --- Preview Modal Display ---
function showPreviewModal(data) {
  el.previewBox.classList.remove('hidden');
  if (data.is_valid_json) {
    el.previewValidationBadge.className = 'badge-valid';
    el.previewValidationBadge.textContent = 'Valid JSON Output';
  } else {
    el.previewValidationBadge.className = 'badge-invalid';
    el.previewValidationBadge.textContent = 'Non-JSON Raw Text';
  }

  el.previewSamplesContainer.innerHTML = '';
  data.samples.forEach(sample => {
    const block = document.createElement('pre');
    block.className = 'sample-block';
    block.textContent = sample;
    el.previewSamplesContainer.appendChild(block);
  });
}

function showAlert(msg) {
  el.statusAlertBox.classList.remove('alert-hidden');
  el.statusAlertText.textContent = msg;
}

function hideAlert() {
  el.statusAlertBox.classList.add('alert-hidden');
}

function formatDuration(secs) {
  const s = Math.floor(secs);
  const hrs = Math.floor(s / 3600);
  const mins = Math.floor((s % 3600) / 60);
  const remSecs = s % 60;
  return `${hrs.toString().padStart(2, '0')}:${mins.toString().padStart(2, '0')}:${remSecs.toString().padStart(2, '0')}`;
}

function escapeHtml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

// Kick off
init();
