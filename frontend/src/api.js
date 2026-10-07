// Use relative path for all requests to route through Nginx reverse proxy (same-origin)
const API_BASE = '';

export async function testKafkaConnection(bootstrapServers) {
  const res = await fetch(`${API_BASE}/api/kafka/test-connection`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ bootstrap_servers: bootstrapServers }),
  });
  return res.json();
}

export async function previewTemplate(template, count = 3) {
  const res = await fetch(`${API_BASE}/api/template/preview`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ template, count }),
  });
  return res.json();
}

export async function startJob(config) {
  const res = await fetch(`${API_BASE}/api/jobs/start`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(config),
  });
  return res.json();
}

export async function pauseJob() {
  const res = await fetch(`${API_BASE}/api/jobs/pause`, {
    method: 'POST',
  });
  return res.json();
}

export async function resumeJob() {
  const res = await fetch(`${API_BASE}/api/jobs/resume`, {
    method: 'POST',
  });
  return res.json();
}

export async function stopJob() {
  const res = await fetch(`${API_BASE}/api/jobs/stop`, {
    method: 'POST',
  });
  return res.json();
}

export async function getJobStatus() {
  const res = await fetch(`${API_BASE}/api/jobs/status`);
  return res.json();
}

export function subscribeToStats(onMessage, onError) {
  const eventSource = new EventSource(`${API_BASE}/api/jobs/stream`);
  
  eventSource.addEventListener('stats', (event) => {
    try {
      const data = JSON.parse(event.data);
      onMessage(data);
    } catch (e) {
      console.error('Failed to parse SSE stats payload', e);
    }
  });

  eventSource.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data);
      onMessage(data);
    } catch (e) {
      console.error('Failed to parse SSE payload', e);
    }
  };

  eventSource.onerror = (err) => {
    if (onError) onError(err);
  };

  return eventSource;
}

