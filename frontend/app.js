// Auto-detects local vs production
const CONFIG = window.VECTORYN_CONFIG || {
  GATEWAY_URL: 'http://localhost:8080',
  API_KEY:     'your_secret_key_here',
};

const state = {
  queryCount:    0,
  ingestCount:   0,
  totalLatency:  0,
  isSearching:   false,
  chats:         [],
  activeChatId:  null,
  serviceStatus: { gateway: null, ingestion: null, search: null },
  kafkaWarning:  false,
};

const $ = id => document.getElementById(id);

/* ── VALIDATION HELPERS ─────────────────────────────────────── */
const VALIDATION = {
  MIN_CONTENT_LENGTH: 10,
  MAX_CONTENT_LENGTH: 2_000_000,
  MIN_QUERY_LENGTH:   2,
  MAX_QUERY_LENGTH:   2000,
  VALID_DOC_ID:       /^[a-zA-Z0-9_\-\.]+$/,

  content(text) {
    if (!text || !text.trim()) return 'El contenido no puede estar vacío.';
    if (text.trim().length < this.MIN_CONTENT_LENGTH)
      return `El contenido debe tener al menos ${this.MIN_CONTENT_LENGTH} caracteres.`;
    if (text.length > this.MAX_CONTENT_LENGTH)
      return `El contenido excede el límite de 2,000,000 caracteres.`;
    return null;
  },

  docId(id) {
    if (!id) return null; // optional
    if (id.length > 100) return 'El ID del documento no puede superar 100 caracteres.';
    if (!this.VALID_DOC_ID.test(id))
      return 'El ID solo puede contener letras, números, guiones, puntos y guiones bajos.';
    return null;
  },

  query(text) {
    if (!text || !text.trim()) return 'Escribe una pregunta.';
    if (text.trim().length < this.MIN_QUERY_LENGTH)
      return `La pregunta debe tener al menos ${this.MIN_QUERY_LENGTH} caracteres.`;
    if (text.length > this.MAX_QUERY_LENGTH)
      return `La pregunta no puede superar ${this.MAX_QUERY_LENGTH} caracteres.`;
    return null;
  },
};

/* ── TAB SWITCHING ──────────────────────────────────────────── */
function switchTab(tab, btn) {
  document.querySelectorAll('.nav-tab').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  ['chat', 'upload', 'status'].forEach(t => {
    const el = $(`tab-${t}`);
    if (el) el.classList.toggle('hidden', t !== tab);
  });
}

/* ── HEALTH CHECK ───────────────────────────────────────────── */
async function checkHealth() {
  const indicator = $('systemBadge');
  if (indicator) indicator.dataset.checking = 'true';

  try {
    const res  = await fetch(`${CONFIG.GATEWAY_URL}/health`, {
      signal: AbortSignal.timeout(5000),
    });
    const data = await res.json().catch(() => ({}));

    const gwOk  = res.ok;
    const ingOk = data.ingestion === 'ok';
    const srOk  = data.search === 'ok';

    state.serviceStatus = { gateway: gwOk, ingestion: ingOk, search: srOk };

    setStatus('svc-gateway',   gwOk,  data.gateway  || (gwOk ? 'ok' : 'error'));
    setStatus('svc-ingestion', ingOk, data.ingestion || 'unknown');
    setStatus('svc-search',    srOk,  data.search    || 'unknown');
    // Qdrant y Kafka se infieren del servicio de ingestion/search
    setStatus('svc-qdrant',    ingOk && srOk, ingOk && srOk ? 'connected' : 'unknown');
    setStatus('svc-kafka',     ingOk, ingOk ? 'connected' : 'not reachable');
    setStatus('svc-redis',     srOk,  srOk  ? 'connected' : 'unknown');

    const allOk = gwOk && ingOk && srOk;
    const someOk = gwOk && (ingOk || srOk);

    setBadge(
      allOk ? 'ok' : someOk ? 'warn' : 'error',
      allOk ? 'Todos los servicios activos'
            : someOk ? 'Algunos servicios degradados'
            : 'Sin conexión al gateway'
    );

    // Mostrar warning de Kafka si el servicio de ingestion reporta problemas
    if (!ingOk && gwOk) {
      showKafkaWarning();
    } else {
      hideKafkaWarning();
    }

  } catch (e) {
    state.serviceStatus = { gateway: false, ingestion: false, search: false };
    ['svc-gateway','svc-ingestion','svc-search','svc-qdrant','svc-kafka','svc-redis']
      .forEach(id => setStatus(id, false, 'no connection'));
    setBadge('error', 'Sin conexión al gateway');
  } finally {
    if (indicator) delete indicator.dataset.checking;
  }
}

function setStatus(id, up, detail) {
  const el = $(id);
  if (!el) return;
  el.classList.remove('up', 'down', 'warn');
  el.classList.add(up ? 'up' : 'down');
  const detailEl = el.querySelector('.svc-detail');
  if (detailEl && detail) detailEl.textContent = detail;
}

function setBadge(st, text) {
  const el = $('systemBadge');
  if (!el) return;
  el.className = `system-badge ${st}`;
  el.querySelector('span').textContent = text;
}

function showKafkaWarning() {
  if (state.kafkaWarning) return;
  state.kafkaWarning = true;
  const w = $('kafkaWarning');
  if (w) w.classList.remove('hidden');
}

function hideKafkaWarning() {
  state.kafkaWarning = false;
  const w = $('kafkaWarning');
  if (w) w.classList.add('hidden');
}

/* ── CHAT MANAGEMENT ────────────────────────────────────────── */
function newChat() {
  const chat = { id: Date.now(), title: 'Nueva conversación', messages: [] };
  state.chats.unshift(chat);
  state.activeChatId = chat.id;
  renderHistory();
  showWelcome();
}

function loadChat(id) {
  state.activeChatId = id;
  renderHistory();
  const chat = state.chats.find(c => c.id === id);
  if (!chat) return;
  showChatArea();
  const box = $('chatBox');
  box.innerHTML = '';
  chat.messages.forEach(m => {
    if (m.role === 'user') renderUserBubble(m.content, m.time);
    else renderAIBubble(m.content, m.time);
  });
  scrollToBottom();
}

function renderHistory() {
  const list = $('historyList');
  if (!list) return;
  if (!state.chats.length) {
    list.innerHTML = `<div class="history-empty">
      <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>
      <span>Aún no hay conversaciones</span>
    </div>`;
    return;
  }
  list.innerHTML = state.chats.map(c => `
    <div class="history-item ${c.id === state.activeChatId ? 'active' : ''}" onclick="loadChat(${c.id})">
      <svg class="history-icon" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>
      ${esc(c.title)}
    </div>`).join('');
}

function showWelcome() {
  $('welcomeScreen').classList.remove('hidden');
  $('chatArea').classList.add('hidden');
}

function showChatArea() {
  $('welcomeScreen').classList.add('hidden');
  $('chatArea').classList.remove('hidden');
}

function useSuggestion(btn) {
  const q = btn.textContent.trim();
  $('chatQuery').value = q;
  autoResize($('chatQuery'));
  handleSearch();
}

/* ── INGESTION ──────────────────────────────────────────────── */
async function handleIngest() {
  const contentEl = $('docContent');
  const docIdEl   = $('docId');
  const content   = contentEl?.value?.trim();
  const docId     = docIdEl?.value?.trim();
  const btn       = $('ingestBtn');
  const btnText   = $('ingestBtnText');

  // Clear previous errors
  clearFieldError('docContent');
  clearFieldError('docId');

  // Validate
  const contentErr = VALIDATION.content(content);
  if (contentErr) {
    showFieldError('docContent', contentErr);
    showFeedback(contentErr, 'error');
    contentEl.focus();
    return;
  }

  const idErr = VALIDATION.docId(docId);
  if (idErr) {
    showFieldError('docId', idErr);
    showFeedback(idErr, 'error');
    docIdEl.focus();
    return;
  }

  // Check if services are available
  if (state.serviceStatus.ingestion === false) {
    showFeedback('⚠ El servicio de ingestion no está disponible. Revisa la pestaña System.', 'warn');
    return;
  }

  btn.disabled = true;
  btn.classList.add('loading');
  btnText.textContent = 'Indexando…';

  const payload = { id: docId || `doc_${Date.now()}`, content };

  try {
    const res = await fetch(`${CONFIG.GATEWAY_URL}/ingest`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json', 'X-API-Key': CONFIG.API_KEY },
      body:    JSON.stringify(payload),
      signal:  AbortSignal.timeout(30_000),
    });

    if (res.status === 401) throw new Error('API Key inválida. Verifica tu configuración.');
    if (res.status === 429) throw new Error('Límite de peticiones alcanzado. Espera un momento.');
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.detail || `Error del servidor (${res.status})`);
    }

    const data = await res.json();
    state.ingestCount++;
    $('stat-ingested').textContent = state.ingestCount;
    addDocToList(data.doc_id || payload.id, content.length);
    contentEl.value = '';
    docIdEl.value   = '';
    onContentInput(contentEl);

    let msg = `✓ Documento indexado correctamente (ID: ${data.doc_id || payload.id}).`;
    if (data.anonymized) msg += ' Se enmascaró información sensible (PII).';
    msg += ' Disponible para consulta en ~5 segundos.';
    showFeedback(msg, 'success');

  } catch (err) {
    if (err.name === 'TimeoutError') {
      showFeedback('Tiempo de espera agotado. El servidor tardó demasiado en responder.', 'error');
    } else {
      showFeedback(err.message, 'error');
    }
  } finally {
    btn.disabled = false;
    btn.classList.remove('loading');
    btnText.textContent = 'Indexar documento';
  }
}

function showFieldError(fieldId, msg) {
  const field = $(fieldId);
  if (!field) return;
  field.classList.add('field-error');
  let errEl = field.parentElement.querySelector('.field-error-msg');
  if (!errEl) {
    errEl = document.createElement('span');
    errEl.className = 'field-error-msg';
    field.parentElement.appendChild(errEl);
  }
  errEl.textContent = msg;
}

function clearFieldError(fieldId) {
  const field = $(fieldId);
  if (!field) return;
  field.classList.remove('field-error');
  const errEl = field.parentElement?.querySelector('.field-error-msg');
  if (errEl) errEl.remove();
}

function showFeedback(msg, type) {
  const el = $('ingestFeedback');
  if (!el) return;
  el.textContent = msg;
  el.className = `feedback ${type}`;
  el.classList.remove('hidden');
  // Auto-hide successes after 8s, keep errors visible
  if (type === 'success') {
    setTimeout(() => { el.className = 'feedback hidden'; }, 8000);
  }
}

function onContentInput(el) {
  const cc = $('charCount');
  const len = el.value.length;
  if (cc) {
    cc.textContent = len.toLocaleString('es');
    cc.classList.toggle('char-count-warn', len > 1_500_000);
    cc.classList.toggle('char-count-error', len > 2_000_000);
  }
  // Live validation
  if (len > 0) clearFieldError('docContent');
}

function onDocIdInput(el) {
  if (el.value.length > 0) clearFieldError('docId');
}

function addDocToList(id, charLen) {
  const container = $('docsIndexed');
  const list = $('docsList');
  if (!container || !list) return;
  container.style.display = '';
  const item = document.createElement('div');
  item.className = 'doc-item';
  item.innerHTML = `
    <svg class="doc-item-icon" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="20 6 9 17 4 12"/></svg>
    <span class="doc-item-name">${esc(id)}</span>
    <span class="doc-item-chars">${charLen.toLocaleString('es')} chars</span>
    <span class="doc-item-time">${ts()}</span>`;
  list.prepend(item);
}

/* ── FILE UPLOAD ────────────────────────────────────────────── */
function onDragOver(e) {
  e.preventDefault();
  $('uploadDrop').classList.add('drag-over');
}

function onDragLeave() {
  $('uploadDrop').classList.remove('drag-over');
}

function onDrop(e) {
  e.preventDefault();
  $('uploadDrop').classList.remove('drag-over');
  const file = e.dataTransfer.files[0];
  if (file) validateAndReadFile(file);
}

function onFileSelect(e) {
  const file = e.target.files[0];
  if (file) validateAndReadFile(file);
}

const ALLOWED_TYPES = ['text/plain', 'text/markdown', 'application/pdf'];
const ALLOWED_EXTS  = ['.txt', '.md', '.pdf'];
const MAX_FILE_SIZE = 5 * 1024 * 1024; // 5 MB

function validateAndReadFile(file) {
  const ext = '.' + file.name.split('.').pop().toLowerCase();
  if (!ALLOWED_EXTS.includes(ext)) {
    showFeedback(`Tipo de archivo no permitido: ${ext}. Usa .txt, .md o .pdf.`, 'error');
    return;
  }
  if (file.size > MAX_FILE_SIZE) {
    showFeedback(`El archivo es demasiado grande (${(file.size/1024/1024).toFixed(1)} MB). Límite: 5 MB.`, 'error');
    return;
  }
  if (file.size === 0) {
    showFeedback('El archivo está vacío.', 'error');
    return;
  }
  readFile(file);
}

function readFile(file) {
  const reader = new FileReader();
  reader.onload = ev => {
    const content = ev.target.result;
    $('docContent').value = content;
    $('docId').value = file.name.replace(/\.[^.]+$/, '').replace(/\s+/g, '_').slice(0, 100);
    onContentInput($('docContent'));
    showFeedback(`Archivo "${esc(file.name)}" cargado (${(file.size/1024).toFixed(1)} KB). Haz clic en "Indexar documento".`, 'success');
  };
  reader.onerror = () => showFeedback('Error al leer el archivo.', 'error');
  reader.readAsText(file);
}

/* ── SEARCH ─────────────────────────────────────────────────── */
async function handleSearch() {
  if (state.isSearching) return;

  const input = $('chatQuery');
  const query = input?.value?.trim();

  // Validate query
  const queryErr = VALIDATION.query(query);
  if (queryErr) {
    // Show subtle inline hint instead of blocking
    input.classList.add('input-shake');
    setTimeout(() => input.classList.remove('input-shake'), 500);
    return;
  }

  // Warn if search service is down
  if (state.serviceStatus.search === false) {
    appendSystemMessage('⚠ El servicio de búsqueda no está disponible. Verifica la pestaña System.');
    return;
  }

  const topK       = parseInt($('topK')?.value || '3', 10);
  const enableEval = $('enableEval')?.checked ?? false;

  input.value = '';
  autoResize(input);

  if (!state.activeChatId) {
    const chat = { id: Date.now(), title: query.slice(0, 40), messages: [] };
    state.chats.unshift(chat);
    state.activeChatId = chat.id;
    renderHistory();
  }

  showChatArea();
  const userTime = ts();
  renderUserBubble(query, userTime);
  saveMessage('user', query, userTime);

  const chat = state.chats.find(c => c.id === state.activeChatId);
  if (chat && chat.messages.length <= 1) {
    chat.title = query.slice(0, 40) + (query.length > 40 ? '…' : '');
    renderHistory();
  }

  state.isSearching = true;
  $('searchBtn').disabled = true;
  $('streamStatus').classList.remove('hidden');

  const loaderId = appendTypingIndicator();
  const t0 = Date.now();

  try {
    const res = await fetch(`${CONFIG.GATEWAY_URL}/search`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json', 'X-API-Key': CONFIG.API_KEY },
      body:    JSON.stringify({ query, top_k: topK, evaluate: enableEval }),
      signal:  AbortSignal.timeout(120_000),
    });

    if (res.status === 401) throw new Error('API Key inválida. Verifica la configuración.');
    if (res.status === 429) throw new Error('Demasiadas solicitudes. Espera un momento.');
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.detail || `Error del servidor (${res.status})`);
    }

    removeEl(loaderId);
    const aiTime = ts();
    const { contentEl } = appendAIBubble(aiTime);

    const reader  = res.body.getReader();
    const decoder = new TextDecoder('utf-8');
    let   text    = '';

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      text += decoder.decode(value, { stream: true });
      contentEl.textContent = text;
      scrollToBottom();
    }

    // If response was empty, show hint
    if (!text.trim()) {
      text = 'No encontré información relevante en los documentos indexados. Intenta subir documentos con contenido relacionado a tu pregunta.';
      contentEl.textContent = text;
    }

    saveMessage('ai', text, aiTime);

    const latency = Date.now() - t0;
    state.queryCount++;
    state.totalLatency += latency;
    $('stat-queries').textContent = state.queryCount;
    $('stat-latency').textContent = Math.round(state.totalLatency / state.queryCount);

  } catch (err) {
    removeEl(loaderId);
    const errTime = ts();
    let errMsg = err.message;
    if (err.name === 'TimeoutError') errMsg = 'La respuesta tardó demasiado. Inténtalo de nuevo.';
    renderAIBubble(`Error: ${errMsg}`, errTime, true);
  } finally {
    state.isSearching  = false;
    $('searchBtn').disabled = false;
    $('streamStatus').classList.add('hidden');
  }
}

function saveMessage(role, content, time) {
  const chat = state.chats.find(c => c.id === state.activeChatId);
  if (chat) chat.messages.push({ role, content, time });
}

function appendSystemMessage(msg) {
  const box = $('chatBox');
  if (!box) return;
  showChatArea();
  const el = document.createElement('div');
  el.className = 'message system-msg';
  el.innerHTML = `<div class="msg-bubble warn-bubble">${esc(msg)}</div>`;
  box.appendChild(el);
  scrollToBottom();
}

/* ── DOM HELPERS ────────────────────────────────────────────── */
function renderUserBubble(text, time) {
  const box = $('chatBox');
  const el  = document.createElement('div');
  el.className = 'message user-msg';
  el.innerHTML = `
    <div class="msg-avatar">Tú</div>
    <div class="msg-body">
      <div class="msg-meta">Tú · ${time}</div>
      <div class="msg-bubble">${esc(text)}</div>
    </div>`;
  box.appendChild(el);
  scrollToBottom();
}

function renderAIBubble(text, time, isError = false) {
  const box = $('chatBox');
  const el  = document.createElement('div');
  el.className = 'message ai-msg';
  el.innerHTML = `
    <div class="msg-avatar">VY</div>
    <div class="msg-body">
      <div class="msg-meta">Vectoryn · ${time}</div>
      <div class="msg-bubble ${isError ? 'error-bubble' : ''}" style="white-space:pre-wrap">${esc(text)}</div>
    </div>`;
  box.appendChild(el);
  scrollToBottom();
}

function appendTypingIndicator() {
  const id  = `loader-${Date.now()}`;
  const box = $('chatBox');
  const el  = document.createElement('div');
  el.id = id;
  el.className = 'message ai-msg';
  el.innerHTML = `
    <div class="msg-avatar">VY</div>
    <div class="msg-body">
      <div class="msg-meta">Vectoryn · ${ts()}</div>
      <div class="msg-bubble">
        <div class="typing-dots"><span></span><span></span><span></span></div>
      </div>
    </div>`;
  box.appendChild(el);
  scrollToBottom();
  return id;
}

function appendAIBubble(time) {
  const box       = $('chatBox');
  const wrapper   = document.createElement('div');
  wrapper.className = 'message ai-msg';

  const contentEl = document.createElement('div');
  contentEl.className   = 'msg-bubble';
  contentEl.style.whiteSpace = 'pre-wrap';

  const metaEl = document.createElement('div');
  metaEl.className    = 'msg-meta';
  metaEl.textContent  = `Vectoryn · ${time}`;

  const body = document.createElement('div');
  body.className = 'msg-body';
  body.appendChild(metaEl);
  body.appendChild(contentEl);

  wrapper.innerHTML = `<div class="msg-avatar">VY</div>`;
  wrapper.appendChild(body);
  box.appendChild(wrapper);
  scrollToBottom();
  return { contentEl };
}

function removeEl(id) { $(id)?.remove(); }

/* ── UTILS ──────────────────────────────────────────────────── */
function scrollToBottom() {
  const area = $('chatArea');
  if (area) area.scrollTop = area.scrollHeight;
}

function autoResize(el) {
  el.style.height = 'auto';
  el.style.height = Math.min(el.scrollHeight, 140) + 'px';
}

function handleKey(e) {
  if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); handleSearch(); }
}

function adjustTopK(delta) {
  const input   = $('topK');
  const display = $('topKDisplay');
  let val = parseInt(input?.value || '3', 10) + delta;
  val = Math.max(1, Math.min(10, val));
  if (input)   input.value         = val;
  if (display) display.textContent = val;
}

function esc(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function ts() {
  return new Date().toLocaleTimeString('es', { hour: '2-digit', minute: '2-digit', hour12: false });
}

/* ── INIT ───────────────────────────────────────────────────── */
document.addEventListener('DOMContentLoaded', () => {
  checkHealth();
  setInterval(checkHealth, 30_000);
  $('chatQuery')?.focus();
  newChat();
});
