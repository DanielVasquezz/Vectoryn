// Auto-detects local vs production (Render/Netlify/etc)
const CONFIG = window.VECTORYN_CONFIG || {
  GATEWAY_URL: 'http://localhost:8080',
  API_KEY:     'your_secret_key_here',
};

const state = {
  queryCount:   0,
  ingestCount:  0,
  totalLatency: 0,
  cacheHits:    0,
  isSearching:  false,
  chats:        [],      // [{id, title, messages:[]}]
  activeChatId: null,
};

const $ = id => document.getElementById(id);

/* ── TAB SWITCHING ───────────────────────────────────────────── */
function switchTab(tab, btn) {
  document.querySelectorAll('.nav-tab').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  ['chat', 'upload', 'status'].forEach(t => {
    const el = $(`tab-${t}`);
    if (el) el.classList.toggle('hidden', t !== tab);
  });
}

/* ── HEALTH CHECK ────────────────────────────────────────────── */
async function checkHealth() {
  const ids = ['svc-ingestion', 'svc-search', 'svc-qdrant', 'svc-kafka', 'svc-redis', 'svc-gateway'];
  ids.forEach(id => {
    const el = $(id);
    if (el) el.classList.remove('up', 'down');
  });

  try {
    const res  = await fetch(`${CONFIG.GATEWAY_URL}/health`, { signal: AbortSignal.timeout(4000) });
    const data = await res.json().catch(() => ({}));

    const gw = res.ok;
    setStatus('svc-gateway',   gw);
    setStatus('svc-ingestion', data.ingestion === 'ok');
    setStatus('svc-search',    data.search    === 'ok');
    setStatus('svc-qdrant',    data.ingestion === 'ok');
    setStatus('svc-kafka',     data.ingestion === 'ok');
    setStatus('svc-redis',     data.search    === 'ok');

    const allOk = gw && data.ingestion === 'ok' && data.search === 'ok';
    setBadge(allOk ? 'ok' : 'warn', allOk ? 'Todos los servicios activos' : 'Algunos servicios inactivos');
  } catch {
    ids.forEach(id => setStatus(id, false));
    setBadge('error', 'Sin conexión al gateway');
  }
}

function setStatus(id, up) {
  const el = $(id);
  if (el) el.classList.add(up ? 'up' : 'down');
}

function setBadge(state, text) {
  const el = $('systemBadge');
  if (!el) return;
  el.className = `system-badge ${state}`;
  el.querySelector('span').textContent = text;
}

/* ── CHAT MANAGEMENT ─────────────────────────────────────────── */
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
  handleSearch();
}

/* ── INGESTION ───────────────────────────────────────────────── */
async function handleIngest() {
  const content = $('docContent')?.value?.trim();
  const docId   = $('docId')?.value?.trim();
  const btn     = $('ingestBtn');
  const btnText = $('ingestBtnText');

  if (!content) {
    showFeedback('Escribe o pega el contenido del documento.', 'error');
    return;
  }

  btn.disabled = true;
  btnText.textContent = 'Indexando…';

  const payload = { id: docId || `doc_${Date.now()}`, content };

  try {
    const res = await fetch(`${CONFIG.GATEWAY_URL}/ingest`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json', 'X-API-Key': CONFIG.API_KEY },
      body:    JSON.stringify(payload),
    });

    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.detail || `Error ${res.status}`);
    }

    state.ingestCount++;
    $('stat-ingested').textContent = state.ingestCount;
    addDocToList(payload.id);
    $('docContent').value = '';
    $('docId').value = '';
    onContentInput($('docContent'));
    showFeedback(`✓ Documento indexado. Listo para consultar en ~5 segundos.`, 'success');

  } catch (err) {
    showFeedback(err.message, 'error');
  } finally {
    btn.disabled = false;
    btnText.textContent = 'Indexar documento';
  }
}

function showFeedback(msg, type) {
  const el = $('ingestFeedback');
  if (!el) return;
  el.textContent = msg;
  el.className = `feedback ${type}`;
  setTimeout(() => { el.className = 'feedback hidden'; }, 6000);
}

function onContentInput(el) {
  const cc = $('charCount');
  if (cc) cc.textContent = el.value.length.toLocaleString();
}

function addDocToList(id) {
  const container = $('docsIndexed');
  const list = $('docsList');
  if (!container || !list) return;
  container.style.display = '';

  const item = document.createElement('div');
  item.className = 'doc-item';
  item.innerHTML = `
    <svg class="doc-item-icon" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="20 6 9 17 4 12"/></svg>
    <span class="doc-item-name">${esc(id)}</span>
    <span class="doc-item-time">${ts()}</span>`;
  list.prepend(item);
}

/* ── FILE UPLOAD ─────────────────────────────────────────────── */
function onDragOver(e) {
  e.preventDefault();
  $('uploadDrop').classList.add('drag-over');
}

function onDrop(e) {
  e.preventDefault();
  $('uploadDrop').classList.remove('drag-over');
  const file = e.dataTransfer.files[0];
  if (file) readFile(file);
}

function onFileSelect(e) {
  const file = e.target.files[0];
  if (file) readFile(file);
}

function readFile(file) {
  const reader = new FileReader();
  reader.onload = ev => {
    const content = ev.target.result;
    $('docContent').value = content;
    $('docId').value = file.name.replace(/\.[^.]+$/, '').replace(/\s+/g, '_');
    onContentInput($('docContent'));
    showFeedback(`Archivo "${file.name}" cargado. Haz clic en "Indexar documento".`, 'success');
  };
  reader.readAsText(file);
}

/* ── SEARCH ──────────────────────────────────────────────────── */
async function handleSearch() {
  if (state.isSearching) return;

  const input = $('chatQuery');
  const query = input?.value?.trim();
  if (!query) return;

  const topK       = parseInt($('topK')?.value || '3', 10);
  const enableEval = $('enableEval')?.checked ?? false;

  input.value = '';
  autoResize(input);

  // Asegurar chat activo
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

  // Actualizar título del chat con la primera pregunta
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
    });

    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.detail || `Error ${res.status}`);
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

    contentEl.textContent = text;
    saveMessage('ai', text, aiTime);

    const latency = Date.now() - t0;
    state.queryCount++;
    state.totalLatency += latency;
    $('stat-queries').textContent  = state.queryCount;
    $('stat-latency').textContent  = Math.round(state.totalLatency / state.queryCount);

  } catch (err) {
    removeEl(loaderId);
    const errTime = ts();
    renderAIBubble(`Lo siento, hubo un problema: ${err.message}`, errTime, true);
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

/* ── DOM HELPERS ─────────────────────────────────────────────── */
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
  const box = $('chatBox');
  const wrapper = document.createElement('div');
  wrapper.className = 'message ai-msg';

  const contentEl = document.createElement('div');
  contentEl.className = 'msg-bubble';
  contentEl.style.whiteSpace = 'pre-wrap';

  const metaEl = document.createElement('div');
  metaEl.className = 'msg-meta';
  metaEl.textContent = `Vectoryn · ${time}`;

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

/* ── UTILS ───────────────────────────────────────────────────── */
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

/* ── INIT ────────────────────────────────────────────────────── */
document.addEventListener('DOMContentLoaded', () => {
  checkHealth();
  setInterval(checkHealth, 30_000);
  $('chatQuery')?.focus();
});
