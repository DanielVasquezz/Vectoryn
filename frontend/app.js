/* ═══════════════════════════════════════════════════════
   VECTORYN app.js v2.0
   — No writing blocked while searching
   — Full responsive
   — Theme system
   — Big-tech micro-interactions
═══════════════════════════════════════════════════════ */

const CONFIG = window.VECTORYN_CONFIG || {
  GATEWAY_URL: 'http://localhost:8080',
  API_KEY:     'your_secret_key_here',
};

// ── STATE ──────────────────────────────────────────────
const state = {
  queryCount:    0,
  ingestCount:   0,
  totalLatency:  0,
  isSearching:   false,          // ← never blocks typing, only the send button
  chats:         [],
  activeChatId:  null,
  serviceStatus: { gateway: null, ingestion: null, search: null },
};

const $ = id => document.getElementById(id);

// ── THEME ──────────────────────────────────────────────
function setTheme(theme) {
  document.documentElement.setAttribute('data-theme', theme);
  localStorage.setItem('vx-theme', theme);
  document.querySelectorAll('.swatch').forEach(s => {
    s.classList.toggle('active', s.dataset.theme === theme);
  });
}

function toggleThemePicker() {
  const picker = $('themePicker');
  picker.classList.toggle('hidden');
}

document.addEventListener('click', e => {
  const dock = document.querySelector('.theme-dock');
  if (dock && !dock.contains(e.target)) {
    $('themePicker')?.classList.add('hidden');
  }
});

// ── SIDEBAR (mobile) ───────────────────────────────────
function openSidebar() {
  $('sidebar').classList.add('open');
  const ov = $('overlay');
  ov.classList.remove('hidden');
  requestAnimationFrame(() => ov.classList.add('visible'));
}

function closeSidebar() {
  $('sidebar').classList.remove('open');
  const ov = $('overlay');
  ov.classList.remove('visible');
  setTimeout(() => ov.classList.add('hidden'), 200);
}

// ── TABS ───────────────────────────────────────────────
function switchTab(tab, btn) {
  document.querySelectorAll('.pill').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  ['chat','upload','status'].forEach(t => {
    $(`tab-${t}`)?.classList.toggle('hidden', t !== tab);
  });
}

// ── HEALTH CHECK ──────────────────────────────────────
async function checkHealth() {
  const refreshBtn = document.querySelector('.btn-refresh');
  refreshBtn?.classList.add('spinning');

  try {
    const res  = await fetch(`${CONFIG.GATEWAY_URL}/health`, {
      signal: AbortSignal.timeout(6000),
    });
    const data = await res.json().catch(() => ({}));

    const gwOk  = res.ok;
    const ingOk = data.ingestion === 'ok';
    const srOk  = data.search === 'ok';

    state.serviceStatus = { gateway: gwOk, ingestion: ingOk, search: srOk };

    setSvc('svc-gateway',   gwOk,  gwOk  ? 'ok' : 'error');
    setSvc('svc-ingestion', ingOk, ingOk ? 'ok' : 'down');
    setSvc('svc-search',    srOk,  srOk  ? 'ok' : 'down');
    setSvc('svc-qdrant',    ingOk && srOk, ingOk && srOk ? 'ok' : '—');
    setSvc('svc-kafka',     ingOk, ingOk ? 'ok' : '—');
    setSvc('svc-redis',     srOk,  srOk  ? 'ok' : '—');

    const allOk = gwOk && ingOk && srOk;
    const someOk = gwOk && (ingOk || srOk);

    setBadge(
      allOk ? 'ok' : someOk ? 'warn' : 'error',
      allOk ? 'All services active'
            : someOk ? 'Degraded'
            : 'Disconnected'
    );

    // Mobile dot
    const mobDot = $('mobStatusDot');
    if (mobDot) {
      mobDot.className = 'mob-status-dot ' + (allOk ? 'ok' : someOk ? 'warn' : 'error');
    }

    if (!ingOk && gwOk) $('kafkaWarning')?.classList.remove('hidden');
    else                $('kafkaWarning')?.classList.add('hidden');

  } catch {
    state.serviceStatus = { gateway: false, ingestion: false, search: false };
    ['svc-gateway','svc-ingestion','svc-search','svc-qdrant','svc-kafka','svc-redis']
      .forEach(id => setSvc(id, false, '—'));
    setBadge('error', 'Disconnected');
  } finally {
    setTimeout(() => refreshBtn?.classList.remove('spinning'), 500);
  }
}

function setSvc(id, up, badge) {
  const el = $(id);
  if (!el) return;
  el.classList.remove('up','down');
  el.classList.add(up ? 'up' : 'down');
  const b = $(`${id}-badge`);
  if (b) b.textContent = badge;
}

function setBadge(st, text) {
  const el = $('systemBadge');
  if (!el) return;
  el.className = `sys-badge ${st}`;
  el.querySelector('span').textContent = text;
}

// ── CHAT MANAGEMENT ───────────────────────────────────
function newChat() {
  const chat = { id: Date.now(), title: 'New conversation', messages: [] };
  state.chats.unshift(chat);
  state.activeChatId = chat.id;
  renderHistory();
  showWelcome();
  closeSidebar();
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
    if (m.role === 'user') renderUserBubble(m.content, m.time, false);
    else renderAIBubble(m.content, m.time, false, false);
  });
  scrollToBottom();
  closeSidebar();
}

function renderHistory() {
  const list = $('historyList');
  if (!list) return;
  if (!state.chats.length) {
    list.innerHTML = `<div class="history-empty">
      <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" opacity=".3"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>
      <p>No conversations yet</p><span>Start by asking a question</span>
    </div>`;
    return;
  }
  list.innerHTML = state.chats.map(c => `
    <div class="history-item ${c.id === state.activeChatId ? 'active' : ''}" onclick="loadChat(${c.id})">
      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="flex-shrink:0;opacity:.4"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>
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

// ── INGESTION ─────────────────────────────────────────
async function handleIngest() {
  const contentEl = $('docContent');
  const docIdEl   = $('docId');
  const content   = contentEl?.value?.trim();
  const docId     = docIdEl?.value?.trim();
  const btn       = $('ingestBtn');
  const btnText   = $('ingestBtnText');

  clearFieldErr('docContent');
  clearFieldErr('docId');

  if (!content || content.length < 10) {
    showFieldErr('docContent', 'Content must be at least 10 characters.');
    contentEl?.focus();
    return;
  }
  if (content.length > 2_000_000) {
    showFieldErr('docContent', 'Content exceeds the 2,000,000 character limit.');
    return;
  }
  if (docId && !/^[a-zA-Z0-9_\-\.]+$/.test(docId)) {
    showFieldErr('docId', 'ID can only contain letters, numbers, dashes, dots, and underscores.');
    docIdEl?.focus();
    return;
  }

  btn.disabled = true;
  btn.classList.add('loading');
  btnText.textContent = 'Indexing…';
  showFeedback('', '');

  const payload = { id: docId || `doc_${Date.now()}`, content };

  try {
    const res = await fetch(`${CONFIG.GATEWAY_URL}/ingest`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json', 'X-API-Key': CONFIG.API_KEY },
      body:    JSON.stringify(payload),
      signal:  AbortSignal.timeout(30_000),
    });

    if (res.status === 401) throw new Error('Invalid API Key. Check your configuration.');
    if (res.status === 429) throw new Error('Rate limit reached. Please wait a moment.');
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.detail || `Server error (${res.status})`);
    }

    const data = await res.json();
    state.ingestCount++;
    $('stat-ingested').textContent = state.ingestCount;
    addDocToList(data.doc_id || payload.id, content.length);
    contentEl.value = '';
    docIdEl.value   = '';
    onContentInput(contentEl);

    let msg = `✓ Document indexed (ID: ${data.doc_id || payload.id}).`;
    if (data.anonymized) msg += ' Sensitive data was masked (PII).';
    msg += ' Available in ~5 seconds.';
    showFeedback(msg, 'success');

  } catch (err) {
    const msg = err.name === 'TimeoutError'
      ? 'Request timed out. Server took too long to respond.'
      : err.message;
    showFeedback(msg, 'error');
  } finally {
    btn.disabled = false;
    btn.classList.remove('loading');
    btnText.textContent = 'Index Document';
  }
}

function showFieldErr(id, msg) {
  const f = $(id); if (!f) return;
  f.classList.add('field-error');
  let e = f.parentElement.querySelector('.field-error-msg');
  if (!e) { e = document.createElement('span'); e.className = 'field-error-msg'; f.after(e); }
  e.textContent = msg;
}

function clearFieldErr(id) {
  const f = $(id); if (!f) return;
  f.classList.remove('field-error');
  f.parentElement?.querySelector('.field-error-msg')?.remove();
}

function showFeedback(msg, type) {
  const el = $('ingestFeedback');
  if (!el) return;
  if (!msg) { el.classList.add('hidden'); return; }
  el.textContent = msg;
  el.className = `feedback-banner ${type}`;
  el.classList.remove('hidden');
  if (type === 'success') setTimeout(() => el.classList.add('hidden'), 8000);
}

function onContentInput(el) {
  const cc = $('charCount');
  const len = el.value.length;
  if (cc) {
    cc.textContent = len.toLocaleString() + ' chars';
    cc.classList.toggle('warn', len > 1_500_000);
    cc.classList.toggle('over', len > 2_000_000);
  }
  if (len > 0) clearFieldErr('docContent');
}

function onDocIdInput(el) {
  if (el.value.length > 0) clearFieldErr('docId');
}

function addDocToList(id, charLen) {
  const container = $('docsIndexed');
  const list = $('docsList');
  if (!container || !list) return;
  container.classList.remove('hidden');
  const item = document.createElement('div');
  item.className = 'doc-item';
  item.innerHTML = `
    <svg class="doc-item-icon" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="20 6 9 17 4 12"/></svg>
    <span class="doc-item-name">${esc(id)}</span>
    <span class="doc-item-meta">${charLen.toLocaleString()} chars · ${ts()}</span>`;
  list.prepend(item);
}

// ── FILE UPLOAD ───────────────────────────────────────
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
  if (file) validateFile(file);
}
function onFileSelect(e) {
  const file = e.target.files[0];
  if (file) validateFile(file);
}

function validateFile(file) {
  const ext = '.' + file.name.split('.').pop().toLowerCase();
  if (!['.txt','.md','.pdf'].includes(ext)) {
    showFeedback(`File type not allowed: ${ext}. Use .txt, .md, or .pdf.`, 'error');
    return;
  }
  if (file.size > 5 * 1024 * 1024) {
    showFeedback(`File too large (${(file.size/1024/1024).toFixed(1)} MB). Limit: 5 MB.`, 'error');
    return;
  }
  if (file.size === 0) { showFeedback('File is empty.', 'error'); return; }
  const reader = new FileReader();
  reader.onload = ev => {
    $('docContent').value = ev.target.result;
    $('docId').value = file.name.replace(/\.[^.]+$/, '').replace(/\s+/g, '_').slice(0, 100);
    onContentInput($('docContent'));
    showFeedback(`"${esc(file.name)}" loaded (${(file.size/1024).toFixed(1)} KB). Click Index Document.`, 'success');
  };
  reader.onerror = () => showFeedback('Error reading file.', 'error');
  reader.readAsText(file);
}

// ── SEARCH ────────────────────────────────────────────
async function handleSearch() {
  // ✅ Allow typing during search — only disable the send button
  if (state.isSearching) return;

  const input = $('chatQuery');
  const query = input?.value?.trim();

  if (!query || query.length < 2) {
    input?.classList.add('shake');
    setTimeout(() => input?.classList.remove('shake'), 400);
    return;
  }

  if (state.serviceStatus.search === false) {
    appendSystemMsg('⚠ Search service unavailable. Check the System tab.');
    return;
  }

  const topK       = parseInt($('topK')?.value || '3', 10);
  const enableEval = $('enableEval')?.checked ?? false;

  // Clear input immediately — don't wait
  input.value = '';
  autoResize(input);

  // Create chat if needed
  if (!state.activeChatId) {
    const chat = { id: Date.now(), title: query.slice(0, 42), messages: [] };
    state.chats.unshift(chat);
    state.activeChatId = chat.id;
    renderHistory();
  }

  showChatArea();
  const userTime = ts();
  renderUserBubble(query, userTime, true);
  saveMsg('user', query, userTime);

  const chat = state.chats.find(c => c.id === state.activeChatId);
  if (chat && chat.messages.length <= 1) {
    chat.title = query.slice(0, 42) + (query.length > 42 ? '…' : '');
    renderHistory();
  }

  state.isSearching = true;
  $('searchBtn').disabled = true;
  $('streamStatus').classList.remove('hidden');

  const loaderId = appendTyping();
  const t0 = Date.now();

  try {
    const res = await fetch(`${CONFIG.GATEWAY_URL}/search`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json', 'X-API-Key': CONFIG.API_KEY },
      body:    JSON.stringify({ query, top_k: topK, evaluate: enableEval }),
      signal:  AbortSignal.timeout(120_000),
    });

    if (res.status === 401) throw new Error('Invalid API Key. Check your configuration.');
    if (res.status === 429) throw new Error('Rate limit reached. Please wait.');
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.detail || `Server error (${res.status})`);
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

    if (!text.trim()) {
      text = 'No relevant information found in the indexed documents. Try uploading documents related to your question.';
      contentEl.textContent = text;
    }

    saveMsg('ai', text, aiTime);

    const latency = Date.now() - t0;
    state.queryCount++;
    state.totalLatency += latency;
    $('stat-queries').textContent = state.queryCount;
    $('stat-latency').textContent = Math.round(state.totalLatency / state.queryCount);

  } catch (err) {
    removeEl(loaderId);
    const aiTime = ts();
    const errMsg = err.name === 'TimeoutError'
      ? 'Response took too long. Please try again.'
      : err.message;
    renderAIBubble(`Error: ${errMsg}`, aiTime, true, true);
  } finally {
    state.isSearching  = false;
    $('searchBtn').disabled = false;
    $('streamStatus').classList.add('hidden');
    // ✅ Refocus input after response — user can type next question immediately
    $('chatQuery')?.focus();
  }
}

function saveMsg(role, content, time) {
  const chat = state.chats.find(c => c.id === state.activeChatId);
  if (chat) chat.messages.push({ role, content, time });
}

function appendSystemMsg(msg) {
  showChatArea();
  const box = $('chatBox'); if (!box) return;
  const el = document.createElement('div');
  el.className = 'message system-msg';
  el.innerHTML = `<div class="msg-bubble warn-bubble">${esc(msg)}</div>`;
  box.appendChild(el);
  scrollToBottom();
}

// ── DOM HELPERS ───────────────────────────────────────
function renderUserBubble(text, time, animate = true) {
  const box = $('chatBox'); if (!box) return;
  const el  = document.createElement('div');
  el.className = `message user-msg${animate ? '' : ' no-anim'}`;
  el.innerHTML = `
    <div class="msg-avatar">You</div>
    <div class="msg-body">
      <div class="msg-meta">You · ${time}</div>
      <div class="msg-bubble">${esc(text)}</div>
    </div>`;
  box.appendChild(el);
  scrollToBottom();
}

function renderAIBubble(text, time, isError = false, animate = true) {
  const box = $('chatBox'); if (!box) return;
  const el  = document.createElement('div');
  el.className = `message ai-msg${animate ? '' : ' no-anim'}`;
  el.innerHTML = `
    <div class="msg-avatar">VY</div>
    <div class="msg-body">
      <div class="msg-meta">Vectoryn · ${time}</div>
      <div class="msg-bubble ${isError ? 'error-bubble' : ''}" style="white-space:pre-wrap">${esc(text)}</div>
    </div>`;
  box.appendChild(el);
  scrollToBottom();
}

function appendTyping() {
  const id  = `ld-${Date.now()}`;
  const box = $('chatBox'); if (!box) return id;
  const el  = document.createElement('div');
  el.id = id;
  el.className = 'message ai-msg';
  el.innerHTML = `
    <div class="msg-avatar">VY</div>
    <div class="msg-body">
      <div class="msg-meta">Vectoryn · ${ts()}</div>
      <div class="msg-bubble"><div class="typing-dots"><span></span><span></span><span></span></div></div>
    </div>`;
  box.appendChild(el);
  scrollToBottom();
  return id;
}

function appendAIBubble(time) {
  const box     = $('chatBox');
  const wrapper = document.createElement('div');
  wrapper.className = 'message ai-msg';
  const contentEl   = document.createElement('div');
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

// ── UTILS ─────────────────────────────────────────────
function scrollToBottom() {
  const area = $('chatArea');
  if (area) area.scrollTo({ top: area.scrollHeight, behavior: 'smooth' });
}

function autoResize(el) {
  el.style.height = 'auto';
  el.style.height = Math.min(el.scrollHeight, 150) + 'px';
}

function handleKey(e) {
  if (e.key === 'Enter' && !e.shiftKey) {
    e.preventDefault();
    handleSearch();
  }
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
  return new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', hour12: false });
}

// ── INIT ──────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  // Restore theme
  const saved = localStorage.getItem('vx-theme') || 'dark';
  setTheme(saved);

  // Health check
  checkHealth();
  setInterval(checkHealth, 30_000);

  // Create initial chat
  newChat();

  // Focus input
  $('chatQuery')?.focus();
});

// ── MANAGE DOCUMENTS ──────────────────────────────────────────────────────────

async function loadDocuments() {
  const list = $('manageDocsList');
  if (!list) return;

  list.innerHTML = '<div class="manage-docs-empty"><span>Loading…</span></div>';

  try {
    const res = await fetch(`${CONFIG.GATEWAY_URL}/documents`, {
      headers: { 'X-API-Key': CONFIG.API_KEY },
      signal: AbortSignal.timeout(10_000),
    });

    if (!res.ok) throw new Error(`Error ${res.status}`);
    const { documents } = await res.json();

    if (!documents || documents.length === 0) {
      list.innerHTML = '<div class="manage-docs-empty"><span>No documents indexed yet</span></div>';
      return;
    }

    list.innerHTML = documents.map(doc => `
      <div class="manage-doc-item" id="doc-item-${esc(doc.doc_id)}">
        <div class="manage-doc-name" title="${esc(doc.doc_id)}">${esc(doc.doc_id)}</div>
        <div class="manage-doc-meta">${doc.chunks} chunk${doc.chunks !== 1 ? 's' : ''}</div>
        <button class="btn-delete-doc" onclick="confirmDelete('${esc(doc.doc_id)}', ${doc.chunks})" title="Delete document">
          <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5">
            <polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6"/>
            <path d="M10 11v6M14 11v6"/><path d="M9 6V4a1 1 0 0 1 1-1h4a1 1 0 0 1 1 1v2"/>
          </svg>
        </button>
      </div>
    `).join('');

  } catch (err) {
    list.innerHTML = `<div class="manage-docs-empty"><span>Error: ${esc(err.message)}</span></div>`;
  }
}

function confirmDelete(docId, chunks) {
  // Verificar si el servicio de search está disponible
  if (state.serviceStatus.search === false) {
    _showNoSpaceBanner('El servicio de búsqueda no está disponible. No se puede eliminar ahora.');
    return;
  }

  // Crear overlay de confirmación
  const overlay = document.createElement('div');
  overlay.className = 'delete-confirm-overlay';
  overlay.id = 'deleteConfirmOverlay';
  overlay.innerHTML = `
    <div class="delete-confirm-box">
      <h3>¿Eliminar documento?</h3>
      <p>
        Se eliminarán <strong>${chunks} chunk${chunks !== 1 ? 's' : ''}</strong> del documento
        <strong>"${esc(docId)}"</strong> de la base de datos vectorial.<br><br>
        Esta acción no se puede deshacer.
      </p>
      <div id="deleteErrorMsg"></div>
      <div class="delete-confirm-actions">
        <button class="btn-cancel-delete" onclick="closeDeleteConfirm()">Cancelar</button>
        <button class="btn-confirm-delete" id="btnConfirmDelete" onclick="executeDelete('${esc(docId)}')">
          Eliminar
        </button>
      </div>
    </div>
  `;
  document.body.appendChild(overlay);

  // Cerrar con click fuera
  overlay.addEventListener('click', e => {
    if (e.target === overlay) closeDeleteConfirm();
  });
}

function closeDeleteConfirm() {
  document.getElementById('deleteConfirmOverlay')?.remove();
}

async function executeDelete(docId) {
  const btn = $('btnConfirmDelete');
  if (btn) { btn.disabled = true; btn.textContent = 'Eliminando…'; }

  try {
    const res = await fetch(`${CONFIG.GATEWAY_URL}/document/${encodeURIComponent(docId)}`, {
      method: 'DELETE',
      headers: { 'X-API-Key': CONFIG.API_KEY },
      signal: AbortSignal.timeout(15_000),
    });

    if (res.status === 503) {
      // Servicio no disponible — no tiene espacio / está caído
      closeDeleteConfirm();
      _showNoSpaceBanner('El sistema no puede procesar la eliminación ahora mismo. Intenta de nuevo en unos segundos.');
      return;
    }

    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.detail || `Error ${res.status}`);
    }

    closeDeleteConfirm();

    // Quitar el item de la lista visualmente sin recargar todo
    document.getElementById(`doc-item-${docId}`)?.remove();

    // Si la lista queda vacía, mostrar mensaje
    const list = $('manageDocsList');
    if (list && list.children.length === 0) {
      list.innerHTML = '<div class="manage-docs-empty"><span>No documents indexed yet</span></div>';
    }

    showFeedback(`✓ Documento "${docId}" eliminado correctamente.`, 'success');

  } catch (err) {
    if (btn) { btn.disabled = false; btn.textContent = 'Eliminar'; }
    const errEl = $('deleteErrorMsg');
    if (errEl) {
      errEl.innerHTML = `<div class="no-space-banner">
        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>
        ${esc(err.message)}
      </div>`;
    }
  }
}

function _showNoSpaceBanner(msg) {
  const list = $('manageDocsList');
  if (!list) return;
  // Quitar banner previo si existe
  list.querySelector('.no-space-banner')?.remove();
  const banner = document.createElement('div');
  banner.className = 'no-space-banner';
  banner.innerHTML = `
    <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>
    ${esc(msg)}
  `;
  list.prepend(banner);
  setTimeout(() => banner.remove(), 6000);
}
