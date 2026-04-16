// ─── Authentication Configuration & Guard ────────────────────────────────────────
let SYSTEM_CONFIG = { mode: 'enterprise', auth_required: true };
let AUTH_TOKEN = null;

function readStoredUserData() {
    const sessionUser = sessionStorage.getItem('sql_agent_user');
    if (sessionUser) return sessionUser;

    const legacyUser = localStorage.getItem('sql_agent_user');
    if (legacyUser) {
        sessionStorage.setItem('sql_agent_user', legacyUser);
        localStorage.removeItem('sql_agent_user');
        return legacyUser;
    }
    return '{}';
}

let USER_DATA = JSON.parse(readStoredUserData());
AUTH_TOKEN = sessionStorage.getItem('sql_agent_token');

/**
 * Returns the appropriate storage provider (localStorage or sessionStorage)
 * based on the session's ephemeral requirements.
 */
function getStorageSource() {
    // If we're in Solo mode or have a Hybrid Solo user identity, 
    // strictly use sessionStorage to ensure no data persists after tab close.
    const isSolo = SYSTEM_CONFIG.mode === 'solo' || USER_DATA.role === 'SOLO_USER';
    return isSolo ? sessionStorage : localStorage;
}

async function initializeApp() {
    try {
        const configRes = await fetch(`${API_BASE}/auth/config`);
        SYSTEM_CONFIG = await configRes.json();
        console.log("System Config Loaded:", SYSTEM_CONFIG);

        // Hybrid Solo Support: Auto-init solo session if explicitly requested or if system is in solo mode
        const isHybridSolo = USER_DATA.username === 'solo_user' || USER_DATA.is_hybrid_solo;
        
        if (SYSTEM_CONFIG.mode === 'solo' || isHybridSolo) {
            // Auto-initialize solo session if not already logged in
            if (!AUTH_TOKEN || USER_DATA.role !== 'SOLO_USER') {
                AUTH_TOKEN = 'standalone-token';
                USER_DATA = {
                    username: 'solo_user',
                    role: 'SOLO_USER',
                    role_label: 'Solo User'
                };
                sessionStorage.setItem('sql_agent_user', JSON.stringify(USER_DATA));
                sessionStorage.setItem('sql_agent_token', AUTH_TOKEN);
            }
        }

        const isLoginPage = window.location.pathname.includes('login.html') || window.location.pathname === '/login';

        if (!USER_DATA.username && SYSTEM_CONFIG.mode !== 'solo' && !isLoginPage) {
            window.location.href = '/login';
            return;
        }

        if (USER_DATA.username && isLoginPage) {
            window.location.href = '/workspace';
            return;
        }

        // Initialize UI and State
        init();

    } catch (e) {
        console.error("Failed to initialize app config", e);
        // Fallback to existing logic if config fails
        if (!USER_DATA.username && SYSTEM_CONFIG.mode !== 'solo' && !window.location.pathname.includes('login.html')) {
            window.location.href = '/login';
        } else {
            init();
        }
    }
}

// ─── State Management ────────────────────────────────────────────────────────
const state = {
    connected: false,
    schema: null,
    history: [],
    currentDatabase: null,
    dashboardHydratedDb: null,
    loading: false,
    isIndexing: false,
    queryInFlight: false,
    historyFilters: {
        start: '',
        end: '',
        sort: 'desc'
    }
};

// Selectors
const selectors = {
    queryInput: document.getElementById('query-input'),
    btnRunQuery: document.getElementById('btn-run-query'),
    btnShowConnect: document.getElementById('btn-show-connect'),
    connectModal: document.getElementById('connect-modal'),
    connectForm: document.getElementById('connect-form'),
    btnCloseModal: document.getElementById('btn-close-modal'),
    loadingOverlay: document.getElementById('loading-overlay'),
    chatMessages: document.getElementById('chat-messages'),
    connectedDbName: document.getElementById('connected-db-name'),
    btnDisconnect: document.getElementById('btn-disconnect'),
    btnSignOut: document.getElementById('btn-signout'),
    schemaContent: document.getElementById('schema-content'),
    statusIndicator: document.querySelector('.status-indicator'),
    statusText: document.querySelector('.status-text'),
    connectionInfoEl: document.querySelector('.connection-info'),
    vizModal: document.getElementById('viz-modal'),
    vizContainer: document.getElementById('viz-container'),
    kpiContainer: document.getElementById('kpi-container'),
    btnCloseViz: document.getElementById('btn-close-viz'),
    vizRecommendations: {
        bar: document.getElementById('viz-recommendation-bar'),
        name: document.getElementById('rec-chart-name'),
        confidence: document.getElementById('rec-confidence'),
        reason: document.getElementById('rec-reason'),
        autoOpened: document.getElementById('rec-auto-opened')
    },
    vizOptionButtons: Array.from(document.querySelectorAll('.viz-option-btn')),

    // AI Settings
    btnShowAiSettings: document.getElementById('btn-show-ai-settings'),
    aiSettingsModal: document.getElementById('ai-settings-modal'),
    btnCloseAiModal: document.getElementById('btn-close-ai-modal'),
    btnSaveAiSettings: document.getElementById('btn-save-ai-settings'),
    userLlmApiKeyInput: document.getElementById('user-llm-api-key'),
    userLlmModelInput: document.getElementById('user-llm-model'),
    userLlmBaseUrlInput: document.getElementById('user-llm-base-url'),
    userLlmBaseUrlGroup: document.getElementById('user-llm-base-url-group'),
    aiStatusBox: document.getElementById('ai-status-box'),
    aiStatusText: document.getElementById('ai-status-text'),
    aiRateLimitBox: document.getElementById('ai-rate-limit-box'),
    aiRateLimitMsg: document.getElementById('ai-rate-limit-msg'),
    aiModelRecsList: document.getElementById('ai-model-recs-list'),
    btnClearAiSettings: document.getElementById('btn-clear-ai-settings'),
    btnTestAiSettings: document.getElementById('btn-test-ai-settings'),
    btnStressTestAi: document.getElementById('btn-stress-test-ai'),
    aiStressTestControls: document.getElementById('ai-stress-test-controls'),
    stressTestGrid: document.getElementById('stress-test-grid'),
    stressTestCount: document.getElementById('stress-test-count'),
    stressTestStatusText: document.getElementById('stress-test-status-text'),

    // Model Discovery
    btnDiscoverModels: document.getElementById('btn-discover-models'),
    discoveredModelsContainer: document.getElementById('discovered-models-container'),
    discoveredModelsList: document.getElementById('discovered-models-list'),

    // Navigation & Views
    navDashboard: document.getElementById('nav-dashboard'),
    navHistory: document.getElementById('nav-history'),
    dashboardView: document.getElementById('dashboard-view'),
    historyView: document.getElementById('history-view'),
    historyList: document.getElementById('history-list'),
    historyDbName: document.getElementById('history-db-name'),
    sideAiStatusBadge: document.getElementById('ai-status-badge'),
    sideAiStatusIcon: document.querySelector('#ai-status-badge .ai-status-icon'),
    sideAiStatusText: document.querySelector('#ai-status-badge .ai-status-text'),

    // Bulk History Actions
    bulkHistoryActions: document.getElementById('bulk-history-actions'),
    selectAllHistory: document.getElementById('selectAllHistory'),
    selectedHistoryCount: document.getElementById('selected-history-count'),
    btnBulkDelete: document.getElementById('btn-bulk-delete'),
    btnBulkExportPdf: document.getElementById('btn-bulk-export-pdf'),

    // History Filtering
    histFilterStart: document.getElementById('history-filter-start'),
    histFilterEnd: document.getElementById('history-filter-end'),
    btnApplyHistFilter: document.getElementById('btn-apply-history-filter'),
    btnClearHistFilter: document.getElementById('btn-clear-history-filter'),
    histSort: document.getElementById('history-sort')
};

// Formatter Utils
const formatters = {
    date: (val) => {
        if (!val) return '';
        const date = new Date(val);
        if (isNaN(date)) return val;
        return date.toLocaleDateString('en-US', { month: 'short', day: '2-digit' });
    },
    value: (val) => {
        if (typeof val !== 'number') return val;
        if (val >= 1000000) return (val / 1000000).toFixed(1) + 'M';
        if (val >= 1000) return (val / 1000).toFixed(1) + 'k';
        return val.toLocaleString();
    },
    percent: (val) => {
        if (typeof val !== 'number') return val;
        return (val * 100).toFixed(1) + '%';
    }
};

const chartTheme = {
    colors: ['#5470c6', '#91cc75', '#fac858', '#ee6666', '#73c0de', '#3ba272', '#fc8452', '#9a60b4', '#ea7ccc'],
    font: 'Inter, system-ui, -apple-system, sans-serif'
};

const API_BASE = window.location.origin;

// Session Management
const getSessionId = () => {
    const storage = getStorageSource();
    let sid = sessionStorage.getItem('sql_agent_session_id');
    if (!sid) {
        sid = storage.getItem('sql_agent_session_id');
    }
    if (!sid) {
        sid = 'sess_' + Math.random().toString(36).substr(2, 9) + '_' + Date.now();
    }
    sessionStorage.setItem('sql_agent_session_id', sid);
    if (storage === localStorage) {
        // Only mirror to localStorage if we are in persistent mode
        localStorage.setItem('sql_agent_session_id', sid);
    } else {
        localStorage.removeItem('sql_agent_session_id');
    }
    return sid;
};

const SESSION_ID = getSessionId();
console.log("SQL Agent Frontend Initialized. Session ID:", SESSION_ID, "Mode:", SYSTEM_CONFIG.mode);

function buildAuthHeaders(extraHeaders = {}) {
    const headers = { ...extraHeaders, 'X-Session-ID': SESSION_ID };
    const token = AUTH_TOKEN || USER_DATA?.token;
    if (token) {
        headers['X-Auth-Token'] = token;
    }
    return headers;
}

async function authFetch(url, options = {}) {
    const headers = buildAuthHeaders(options.headers || {});
    return fetch(url, {
        ...options,
        headers,
        credentials: 'same-origin'
    });
}

function escapeHtml(value) {
    return String(value ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function createInfoParagraph(className, text) {
    const safeText = escapeHtml(text);
    return `<p class="${className}">${safeText}</p>`;
}

function getDashboardCacheKey(dbName = state.currentDatabase) {
    if (!dbName) return null;
    const userKey = USER_DATA?.username || 'solo_user';
    return `sql_agent_dashboard_history:${userKey}:${dbName}`;
}

function readDashboardCache(dbName = state.currentDatabase) {
    const cacheKey = getDashboardCacheKey(dbName);
    if (!cacheKey) return [];
    try {
        const storage = getStorageSource();
        const raw = storage.getItem(cacheKey);
        const parsed = JSON.parse(raw || '[]');
        return Array.isArray(parsed) ? parsed : [];
    } catch (e) {
        console.warn("Failed to read dashboard cache", e);
        return [];
    }
}

function writeDashboardCache(messages, dbName = state.currentDatabase) {
    const cacheKey = getDashboardCacheKey(dbName);
    if (!cacheKey) return;
    try {
        const storage = getStorageSource();
        storage.setItem(cacheKey, JSON.stringify(messages));
    } catch (e) {
        console.warn("Failed to write dashboard cache", e);
    }
}

function clearDashboardCache(dbName = state.currentDatabase) {
    const cacheKey = getDashboardCacheKey(dbName);
    if (!cacheKey) return;
    try {
        const storage = getStorageSource();
        storage.removeItem(cacheKey);
    } catch (e) {
        console.warn("Failed to clear dashboard cache", e);
    }
}

function getAiConfigStorageKey() {
    const username = USER_DATA?.username || 'solo_user';
    return `sql_agent_user_ai_config:${username}`;
}

function getStoredAiConfig() {
    const storage = getStorageSource();
    const key = getAiConfigStorageKey();
    try {
        return JSON.parse(storage.getItem(key) || '{}');
    } catch (e) {
        console.warn("Failed to read AI config", e);
        return {};
    }
}

function setStoredAiConfig(config) {
    const storage = getStorageSource();
    const key = getAiConfigStorageKey();
    try {
        storage.setItem(key, JSON.stringify(config));
    } catch (e) {
        console.warn("Failed to store AI config", e);
    }
}

function clearStoredAiConfig() {
    const scopedKey = getAiConfigStorageKey();
    try {
        localStorage.removeItem(scopedKey);
        sessionStorage.removeItem(scopedKey);
        // Cleanup old shared key from previous behavior
        localStorage.removeItem('sql_agent_user_ai_config');
        sessionStorage.removeItem('sql_agent_user_ai_config');
    } catch (e) {
        console.warn("Failed to clear AI config", e);
    }
}

async function handleSignOut() {
    try {
        await authFetch(`${API_BASE}/auth/logout`, { method: 'POST' });
    } catch (e) {
        console.warn("Sign out cleanup request failed", e);
    } finally {
        const storage = getStorageSource();
        clearDashboardCache(state.currentDatabase);
        clearStoredAiConfig();
        
        storage.removeItem('sql_agent_user');
        storage.removeItem('sql_agent_active_tab');
        sessionStorage.removeItem('sql_agent_session_id');
        localStorage.removeItem('sql_agent_session_id');
        
        // Ensure dual-wipe for hybrid cases
        if (storage === sessionStorage) {
             localStorage.removeItem('sql_agent_user');
             localStorage.removeItem('sql_agent_active_tab');
        }

        window.location.href = '/login';
    }
}

function appendDashboardCacheMessage(message, dbName = state.currentDatabase) {
    if (!message || !dbName) return;
    const messages = readDashboardCache(dbName);
    messages.push(message);
    writeDashboardCache(messages.slice(-20), dbName);
}

function upsertDashboardCacheMessage(message, dbName = state.currentDatabase) {
    if (!message || !dbName) return;

    const messages = readDashboardCache(dbName);
    const targetId = message?.data?.id;
    let replaced = false;

    if (targetId) {
        for (let i = messages.length - 1; i >= 0; i--) {
            const existing = messages[i];
            if (existing?.role !== 'assistant') continue;
            if (String(existing?.data?.id || '') !== String(targetId)) continue;

            messages[i] = {
                ...existing,
                ...message,
                data: {
                    ...(existing.data || {}),
                    ...(message.data || {})
                }
            };
            replaced = true;
            break;
        }
    }

    if (!replaced) {
        messages.push(message);
    }

    writeDashboardCache(messages.slice(-20), dbName);
}

function buildDashboardMessagesFromHistory(historyItems) {
    const messages = [];
    (historyItems || []).forEach(item => {
        const question = item.question || item.user;
        const summary = item.summary || item.assistant;
        if (question) {
            messages.push({ role: 'user', text: question });
        }
        messages.push({
            role: 'assistant',
            data: {
                id: item.request_id || item.id,
                question,
                summary,
                sql: item.sql,
                results: item.results,
                visualization: item.visualization,
                error: item.has_error || false
            }
        });
    });
    return messages;
}

function replaceDashboardCacheFromHistory(historyItems, dbName = state.currentDatabase) {
    if (!dbName) return;
    const messages = buildDashboardMessagesFromHistory(historyItems);
    writeDashboardCache(messages, dbName);
}

function isSoloSession() {
    return SYSTEM_CONFIG.mode === 'solo' || USER_DATA.role === 'SOLO_USER';
}

function restoreDashboardFromCache(dbName = state.currentDatabase) {
    const cachedMessages = readDashboardCache(dbName);
    if (!cachedMessages.length) return false;

    selectors.chatMessages.innerHTML = '';
    cachedMessages.forEach(msg => {
        if (msg.role === 'user') {
            renderUserMessage(msg.text, false, false);
        } else if (msg.role === 'assistant') {
            renderBotMessage(msg.data || {}, false, false);
        }
    });
    scrollToBottom();
    return true;
}

function renderDashboardLoadingState(message = null) {
    const defaultMessage = isSoloSession()
        ? 'Preparing your current solo session...'
        : 'Loading previous chats...';
    const subtext = isSoloSession()
        ? 'Please wait while we restore this tab\'s active conversation and database context.'
        : 'Please wait while we restore your recent conversation and database context.';
    selectors.chatMessages.innerHTML = `
        <div class="workspace-loading-state" id="workspace-loading-state">
            <div class="workspace-loading-spinner">
                <div class="loading-spinner-ring">
                    <div class="loading-spinner-inner"></div>
                </div>
            </div>
            <div class="workspace-loading-text">${escapeHtml(message || defaultMessage)}</div>
            <div class="workspace-loading-subtext">${escapeHtml(subtext)}</div>
        </div>
    `;
}

function renderDashboardEmptyState(
    title = null,
    subtext = null
) {
    const defaultTitle = isSoloSession()
        ? 'No chat in this solo session yet.'
        : 'No previous chats found for this database yet.';
    const defaultSubtext = isSoloSession()
        ? 'Ask a question to start a new temporary conversation for this tab.'
        : 'Ask a question to start a new conversation, or wait a moment if history is still syncing from Supabase.';
    selectors.chatMessages.innerHTML = `
        <div class="workspace-loading-state workspace-empty-state" id="workspace-empty-state">
            <div class="workspace-loading-text">${escapeHtml(title || defaultTitle)}</div>
            <div class="workspace-loading-subtext">${escapeHtml(subtext || defaultSubtext)}</div>
        </div>
    `;
}



// ─── Initialize ──────────────────────────────────────────────────────────────
function init() {
    // UI Adaptation based on Mode
    const isSolo = SYSTEM_CONFIG.mode === 'solo' || USER_DATA.role === 'SOLO_USER';
    
    if (isSolo) {
        const adminBtn = document.getElementById('nav-admin');
        if (adminBtn) adminBtn.style.display = 'none';

        // Hide Sign Out for solo users
        if (selectors.btnSignOut) selectors.btnSignOut.style.display = 'none';

        // Hide session-persistent settings that don't apply to solo
        const rbacTab = document.querySelector('[data-view="rbac"]');
        if (rbacTab) rbacTab.style.display = 'none';
    }

    checkStatus();

    // Event Listeners
    selectors.btnShowConnect.addEventListener('click', () => selectors.connectModal.style.display = 'flex');
    selectors.btnCloseModal.addEventListener('click', () => selectors.connectModal.style.display = 'none');
    selectors.btnDisconnect.addEventListener('click', handleDisconnect);

    // Restore active tab
    const storage = getStorageSource();
    const activeTab = storage.getItem('sql_agent_active_tab') || 'dashboard';
    switchMainTab(activeTab);

    selectors.connectForm.addEventListener('submit', handleConnect);
    selectors.btnRunQuery.addEventListener('click', handleQuery);

    selectors.queryInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') handleQuery();
    });

    // Tab Navigation
    if (selectors.navDashboard && selectors.navHistory) {
        selectors.navDashboard.addEventListener('click', () => switchMainTab('dashboard'));
        selectors.navHistory.addEventListener('click', () => switchMainTab('history'));
    }

    selectors.btnCloseViz.addEventListener('click', () => selectors.vizModal.style.display = 'none');

    document.querySelectorAll('.viz-option-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            const chartType = btn.getAttribute('data-type');
            if (state.currentVizData) {
                renderChartOrKPI(chartType, state.currentVizData);
            }
        });
        btn.setAttribute('tabindex', '0');
        btn.addEventListener('keypress', (e) => {
            if (e.key === 'Enter' || e.key === ' ') {
                btn.click();
            }
        });
    });

    // AI Settings Listeners
    if (selectors.btnShowAiSettings) {
        selectors.btnShowAiSettings.addEventListener('click', openAiSettings);
        selectors.btnCloseAiModal.addEventListener('click', () => selectors.aiSettingsModal.style.display = 'none');
        selectors.btnSaveAiSettings.addEventListener('click', saveAiSettings);
        selectors.btnClearAiSettings.addEventListener('click', clearAiSettings);
        selectors.btnTestAiSettings.addEventListener('click', testAiSettings);
        selectors.btnStressTestAi.addEventListener('click', runStressTestAI);
        loadAiSettingsLocally();
    }

    // Wire the static Sign Out button (solo mode → hide it; enterprise → bind handler)
    if (selectors.btnSignOut) {
        if (SYSTEM_CONFIG.mode === 'solo') {
            selectors.btnSignOut.style.display = 'none';
        } else {
            selectors.btnSignOut.addEventListener('click', handleSignOut);
        }
    }

    // Show Admin Panel button if user is SYSTEM_ADMIN or SUPER_ADMIN
    if (USER_DATA.role === 'SYSTEM_ADMIN' || USER_DATA.role === 'SUPER_ADMIN') {
        const adminBtn = document.getElementById('nav-admin');
        if (adminBtn) adminBtn.classList.remove('nav-hidden');
    }

    // Database Engine Change Listener (Auto-port update)
    const engineSelect = document.getElementById('db-engine');
    const portInput = document.getElementById('db-port');
    if (engineSelect && portInput) {
        engineSelect.addEventListener('change', () => {
            const engine = engineSelect.value;
            if (engine === 'postgres') portInput.value = 5432;
            else if (engine === 'mysql') portInput.value = 3306;
        });
    }

    // Start Heartbeat (Check connection status every 30 seconds)
    setInterval(checkStatus, 30000);

    // Initial AI status check if already connected
    pollAIStatus();
}

// Check Connection Status
async function checkStatus() {
    try {
        const res = await authFetch(`${API_BASE}/status`);
        const data = await res.json();

        if (data.connected && data.database) {
            const hasEmptyState = selectors.chatMessages.querySelector('.workspace-empty-state') !== null;
            const shouldHydrateDashboard =
                state.currentDatabase !== data.database ||
                state.dashboardHydratedDb !== data.database ||
                (!hasEmptyState && selectors.chatMessages.querySelectorAll('.message').length === 0);

            updateConnectionUI(true, data.database);
            if (shouldHydrateDashboard && selectors.chatMessages.querySelectorAll('.message').length === 0) {
                renderDashboardLoadingState();
            }
            await loadSchema();
            await loadHistory();
            if (shouldHydrateDashboard) {
                await populateDashboardFromHistory();
            }
        } else if (data.restoring) {
            // Wait and retry briefly
            setTimeout(checkStatus, 2000);
        } else {
            updateConnectionUI(false);
        }
    } catch (e) {
        console.error("Health check failed", e);
        updateConnectionUI(false);
    }
}

// Update UI based on connection
function updateConnectionUI(connected, dbName = '') {
    const dbChanged = connected && state.currentDatabase !== dbName;
    state.connected = connected;
    if (connected) {
        state.currentDatabase = dbName;
        if (dbChanged) {
            state.dashboardHydratedDb = null;
            selectors.chatMessages.innerHTML = '';
        }
        selectors.statusIndicator.className = 'status-indicator connected';
        selectors.statusText.innerText = `Connected: ${dbName}`;
        selectors.connectedDbName.innerText = dbName;
        selectors.historyDbName.innerText = dbName;
        selectors.btnShowConnect.innerText = 'Switch Database';
        // Show green dot in header connection info
        if (selectors.connectionInfoEl) {
            selectors.connectionInfoEl.classList.add('connected');
        }
    } else {
        state.currentDatabase = null;
        state.dashboardHydratedDb = null;
        selectors.statusIndicator.className = 'status-indicator disconnected';
        selectors.statusText.innerText = 'Disconnected';
        selectors.connectedDbName.innerText = 'Not connected';
        selectors.historyDbName.innerText = 'None';
        selectors.btnShowConnect.innerText = 'Connect DB';
        // Hide green dot when disconnected
        if (selectors.connectionInfoEl) {
            selectors.connectionInfoEl.classList.remove('connected');
        }
        // Clear views on disconnect
        if (selectors.sideAiStatusBadge) {
            selectors.sideAiStatusBadge.style.display = 'none';
        }
    }
}

async function handleDisconnect() {
    showConfirmationModal(
        "Disconnect Database",
        "Are you sure you want to disconnect? All session cache for this database will be cleared.",
        async () => {
            try {
                clearDashboardCache(state.currentDatabase);
                await authFetch(`${API_BASE}/disconnect`, { method: 'POST' });
                updateConnectionUI(false);
                selectors.schemaContent.innerHTML = '<p class="empty-msg">Connect to a database to view schema.</p>';
                showNotification("Database disconnected", "info");
            } catch (e) {
                console.error("Disconnect failed", e);
                showNotification("Disconnect failed", "error");
            }
        }
    );
}

// Handle Database Connection
async function handleConnect(e) {
    e.preventDefault();
    const formData = new FormData(selectors.connectForm);
    const payload = Object.fromEntries(formData.entries());
    payload.port = parseInt(payload.port);

    setLoading(true, "Connecting to database...");

    try {
        const res = await authFetch(`${API_BASE}/connect`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });

        if (!res.ok) throw new Error(await res.text());

        const schema = await res.json();
        state.schema = schema;

        // IMMEDIATE UI DISMISSAL: Show dashboard as soon as schema arrives
        updateConnectionUI(true, payload.database);
        selectors.connectModal.style.display = 'none';
        setLoading(false);
        renderDashboardLoadingState(
            isSoloSession()
                ? 'Preparing this solo workspace...'
                : 'Loading chats for this database...'
        );

        renderSchema(schema);

        // Hydrate both history views right after connect so the dashboard
        // never sits blank while waiting for a later status poll.
        await Promise.all([
            loadHistory(),
            populateDashboardFromHistory()
        ]);

        // After connecting, if we have a locally stored AI config, sync it with the server
        const savedConfig = getStoredAiConfig();
        if (savedConfig.api_key) {
            syncAiKeyWithServer(savedConfig.provider, savedConfig.api_key, savedConfig.model, savedConfig.base_url);
        }

        showNotification('Connected successfully!', 'success');

        // Start polling for AI Readiness (Indexing Status)
        pollAIStatus();
    } catch (err) {
        showNotification('Connection error: ' + err.message, 'error');
    } finally {
        setLoading(false);
    }
}

// ─── AI Settings Logic ───────────────────────────────────────────────────────
let currentSelectedProvider = 'groq';
let pendingRateLimitInfo = null;
let rateLimitToastTimer = null;

function selectProvider(name) {
    currentSelectedProvider = name;
    document.querySelectorAll('.provider-card').forEach(c => c.classList.remove('selected'));
    document.getElementById('provider' + name.charAt(0).toUpperCase() + name.slice(1)).classList.add('selected');

    // Reset discovery UI
    if (selectors.discoveredModelsContainer) selectors.discoveredModelsContainer.style.display = 'none';
    if (selectors.discoveredModelsList) selectors.discoveredModelsList.innerHTML = '';

    // Toggle Base URL field
    if (selectors.userLlmBaseUrlGroup) {
        selectors.userLlmBaseUrlGroup.style.display = (name === 'custom') ? 'block' : 'none';
    }

    // Auto-fill default models if empty
    const modelInput = selectors.userLlmModelInput;
    if (!modelInput.value) {
        if (name === 'groq') modelInput.value = 'llama-3.3-70b-versatile';
        if (name === 'openai') modelInput.value = 'gpt-4o';
        if (name === 'gemini') modelInput.value = 'gemini-1.5-pro';
        if (name === 'deepseek') modelInput.value = 'deepseek-chat';
        if (name === 'anthropic') modelInput.value = 'claude-3-5-sonnet-20241022';
    }
}

function openAiSettings() {
    const savedConfig = getStoredAiConfig();
    if (savedConfig.provider) selectProvider(savedConfig.provider);
    if (savedConfig.api_key) selectors.userLlmApiKeyInput.value = savedConfig.api_key;
    if (savedConfig.model) selectors.userLlmModelInput.value = savedConfig.model;
    if (savedConfig.base_url) selectors.userLlmBaseUrlInput.value = savedConfig.base_url;

    updateAiStatusUI(!!savedConfig.api_key);
    hideRateLimitError();

    if (pendingRateLimitInfo) {
        if (pendingRateLimitInfo.provider) {
            selectProvider(pendingRateLimitInfo.provider);
        }
        showRateLimitError(pendingRateLimitInfo.message, pendingRateLimitInfo.recommendations || []);
    }

    selectors.aiSettingsModal.style.display = 'flex';
}

/**
 * Polls the backend to check if the AI semantic index is ready.
 * Only runs while state.isIndexing is true.
 */
async function pollAIStatus() {
    if (!state.connected) return;

    try {
        const res = await authFetch(`${API_BASE}/status`);
        if (!res.ok) return;
        const status = await res.json();

        state.isIndexing = !!status.is_indexing;
        updateAIStatusUI(state.isIndexing);

        if (state.isIndexing) {
            // Keep polling every 2 seconds until indexing is finished
            setTimeout(pollAIStatus, 2000);
        }
    } catch (e) {
        console.error("AI Status poll failed", e);
    }
}

function updateAIStatusUI(isIndexing) {
    if (!state.connected || !selectors.sideAiStatusBadge) return;

    selectors.sideAiStatusBadge.style.display = 'flex';

    if (isIndexing) {
        selectors.sideAiStatusBadge.className = 'ai-status-container status-indexing';
        selectors.sideAiStatusIcon.innerText = '🧠';
        selectors.sideAiStatusText.innerText = 'AI Indexing...';
    } else {
        selectors.sideAiStatusBadge.className = 'ai-status-container status-ready';
        selectors.sideAiStatusIcon.innerText = '✨';
        selectors.sideAiStatusText.innerText = 'AI Ready';
    }
}

function loadAiSettingsLocally() {
    const savedConfig = getStoredAiConfig();
    updateAiStatusUI(!!savedConfig.api_key);
}

async function saveAiSettings() {
    const key = selectors.userLlmApiKeyInput.value.trim();
    const model = selectors.userLlmModelInput.value.trim();
    const baseUrl = selectors.userLlmBaseUrlInput.value.trim();
    const provider = currentSelectedProvider;

    const config = { provider, api_key: key, model, base_url: baseUrl };
    setStoredAiConfig(config);
    await syncAiKeyWithServer(provider, key, model, baseUrl);

    updateAiStatusUI(!!key);
    hideRateLimitError();
    selectors.aiSettingsModal.style.display = 'none';
    showNotification('LLM Configuration applied!', 'success');
}

async function clearAiSettings() {
    showConfirmationModal(
        "Clear AI Configuration",
        "Are you sure you want to clear your custom LLM settings and API key? This will revert the system to the default API profile.",
        async () => {
            selectors.userLlmApiKeyInput.value = '';
            selectors.userLlmModelInput.value = '';
            if (selectors.userLlmBaseUrlInput) selectors.userLlmBaseUrlInput.value = '';
            clearStoredAiConfig();
            updateAiStatusUI(false);
            hideRateLimitError();
            // Clear and hide stress test
            if (selectors.aiStressTestControls) selectors.aiStressTestControls.style.display = 'none';
            if (selectors.stressTestGrid) selectors.stressTestGrid.innerHTML = '';

            // Sync clear with server
            try {
                await authFetch(`${API_BASE}/auth/llm-config`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ active_provider: currentSelectedProvider, providers: {} })
                });
            } catch (e) {
                console.error("Failed clearing AI config on server", e);
            }

            pendingRateLimitInfo = null;
            showNotification('Settings cleared. Using System API.', 'info');
        }
    );
}

async function testAiSettings() {
    setLoading(true, "Testing LLM connection...");
    hideRateLimitError();
    try {
        const res = await authFetch(`${API_BASE}/auth/test-llm`, { method: 'POST' });
        const data = await res.json();
        if (data.status === 'success') {
            showNotification('Connection successful', 'success');
        } else if (data.status === 'rate_limit') {
            showRateLimitError(data.message, data.recommendations);
        } else {
            showNotification('Test Failed: ' + data.message, 'error');
        }
    } catch (e) {
        showNotification('Err: ' + e.message, 'error');
    } finally {
        setLoading(false);
    }
}

async function runStressTestAI() {
    // Show controls if hidden
    if (selectors.aiStressTestControls.style.display === 'none') {
        selectors.aiStressTestControls.style.display = 'block';
        selectors.stressTestStatusText.innerText = 'Choose count and click Stress Test again to start';
        return;
    }

    const count = parseInt(selectors.stressTestCount.value) || 10;
    if (count > 50) {
        showNotification("Max 50 requests allowed for safety.", 'warning');
        return;
    }

    selectors.stressTestGrid.innerHTML = '';
    selectors.stressTestStatusText.innerText = `Starting ${count} parallel requests...`;
    hideRateLimitError();

    // Create dots
    const dots = [];
    for (let i = 0; i < count; i++) {
        const dot = document.createElement('div');
        dot.className = 'stress-dot pending';
        selectors.stressTestGrid.appendChild(dot);
        dots.push(dot);
    }

    let completed = 0;
    let rateLimits = 0;
    let errors = 0;

    const runRequest = async (index) => {
        try {
            const res = await authFetch(`${API_BASE}/auth/test-llm`, { method: 'POST' });
            const data = await res.json();

            dots[index].classList.remove('pending');
            if (data.status === 'success') {
                dots[index].classList.add('success');
            } else if (data.status === 'rate_limit') {
                dots[index].classList.add('rate-limited');
                rateLimits++;
                showRateLimitError(data.message, data.recommendations);
            } else {
                dots[index].classList.add('error');
                errors++;
            }
        } catch (e) {
            dots[index].classList.remove('pending');
            dots[index].classList.add('error');
            errors++;
        } finally {
            completed++;
            updateStressStatusText(completed, count, rateLimits, errors);
        }
    };

    // Run parallel
    await Promise.all(dots.map((_, i) => runRequest(i)));
}

function updateStressStatusText(done, total, limit, err) {
    let text = `Progress: ${done}/${total}`;
    if (limit > 0) text += ` | ⚠️ Rate Limits: ${limit}`;
    if (err > 0) text += ` | ❌ Errors: ${err}`;

    if (done === total) {
        if (limit === 0 && err === 0) {
            text = `✅ Completed: ${total} requests. All successful!`;
            selectors.stressTestStatusText.style.color = '#16a34a';
            selectors.stressTestStatusText.style.fontWeight = '700';
        } else {
            text = `✅ Completed: ${total} requests. ` + (limit > 0 ? `Detected ${limit} rate limits.` : 'Some errors occurred.');
            selectors.stressTestStatusText.style.color = limit > 0 ? '#dc2626' : '#4b5563';
        }
    } else {
        selectors.stressTestStatusText.style.color = 'inherit';
        selectors.stressTestStatusText.style.fontWeight = 'normal';
    }
    selectors.stressTestStatusText.innerText = text;
}

function showRateLimitError(msg, recommendations) {
    selectors.aiRateLimitMsg.innerText = msg;
    selectors.aiModelRecsList.innerHTML = '';
    const normalizedRecommendations = normalizeModelNames(recommendations || []);

    if (normalizedRecommendations.length > 0) {
        normalizedRecommendations.forEach(model => {
            const span = document.createElement('span');
            span.className = 'rec-model-tag';
            span.innerText = model;
            span.onclick = () => {
                selectors.userLlmModelInput.value = model;
                selectors.userLlmModelInput.classList.remove('input-highlight');
                void selectors.userLlmModelInput.offsetWidth; // trigger reflow
                selectors.userLlmModelInput.classList.add('input-highlight');
                hideRateLimitError();
            };
            selectors.aiModelRecsList.appendChild(span);
        });
    } else {
        selectors.aiModelRecsList.innerHTML = '<span style="font-size: 0.8rem; color: #666;">No alternative models suggested.</span>';
    }

    // Mirror the same alternatives into the model discovery panel so user can switch quickly.
    applyAlternativeModels(normalizedRecommendations);
    selectors.aiRateLimitBox.style.display = 'block';
}

function hideRateLimitError() {
    if (selectors.aiRateLimitBox) selectors.aiRateLimitBox.style.display = 'none';
}

function applyAlternativeModels(models) {
    if (!selectors.discoveredModelsContainer || !selectors.discoveredModelsList) return;
    const normalizedModels = normalizeModelNames(models || []);
    if (normalizedModels.length === 0) return;

    selectors.discoveredModelsList.innerHTML = '';
    normalizedModels.forEach(model => {
        const span = document.createElement('span');
        span.className = 'rec-model-tag';
        span.style.cursor = 'pointer';
        span.innerText = model;
        span.onclick = () => {
            selectors.userLlmModelInput.value = model;
            selectors.userLlmModelInput.classList.remove('input-highlight');
            void selectors.userLlmModelInput.offsetWidth;
            selectors.userLlmModelInput.classList.add('input-highlight');
        };
        selectors.discoveredModelsList.appendChild(span);
    });
    selectors.discoveredModelsContainer.style.display = 'block';
}

function normalizeModelName(model) {
    const raw = String(model || '').trim();
    if (!raw) return '';
    if (raw.includes('/')) return raw.split('/').pop();
    return raw;
}

function normalizeModelNames(models) {
    const unique = [];
    const seen = new Set();
    (models || []).forEach(m => {
        const normalized = normalizeModelName(m);
        if (!normalized) return;
        const key = normalized.toLowerCase();
        if (seen.has(key)) return;
        seen.add(key);
        unique.push(normalized);
    });
    return unique;
}

function showRateLimitToast(message, recommendations = [], provider = null) {
    let toast = document.getElementById('llm-rate-limit-toast');
    if (!toast) {
        toast = document.createElement('div');
        toast.id = 'llm-rate-limit-toast';
        toast.className = 'llm-rate-limit-toast';
        document.body.appendChild(toast);
    }

    const friendlyMessage = formatRateLimitToastMessage(message, provider);
    toast.innerHTML = `
        <div class="toast-title">Personal API limit reached</div>
        <div class="toast-message">${escapeHtml(friendlyMessage)}</div>
        <div class="toast-actions">
            <button type="button" class="toast-open-ai">Switch Model</button>
            <button type="button" class="toast-dismiss">Dismiss</button>
        </div>
    `;

    const openBtn = toast.querySelector('.toast-open-ai');
    const dismissBtn = toast.querySelector('.toast-dismiss');
    if (openBtn) {
        openBtn.onclick = () => {
            if (provider) {
                pendingRateLimitInfo = { ...(pendingRateLimitInfo || {}), provider };
            }
            openAiSettings();
        };
    }
    if (dismissBtn) {
        dismissBtn.onclick = () => toast.classList.remove('show');
    }

    toast.classList.add('show');
    if (rateLimitToastTimer) clearTimeout(rateLimitToastTimer);
    rateLimitToastTimer = setTimeout(() => {
        toast.classList.remove('show');
    }, 10000);
}

function formatRateLimitToastMessage(message, provider) {
    const raw = String(message || '');
    const has429 = raw.includes('429') || raw.toLowerCase().includes('too many requests');

    if (has429) {
        const providerName = provider ? provider.toUpperCase() : 'provider';
        return `Your ${providerName} API reached rate limit (429). Please wait a bit or switch to another model.`;
    }
    return raw || 'The selected model is currently rate-limited.';
}

function handleRateLimitSignal(payload, fallbackMessage = '') {
    if (!payload) return;
    const recommendations = payload.recommendations || [];
    const provider = payload.provider || currentSelectedProvider;
    const message = payload.message || fallbackMessage || 'Rate limit reached.';

    pendingRateLimitInfo = { message, recommendations, provider };
    showRateLimitToast(message, recommendations, provider);

    if (selectors.aiSettingsModal && selectors.aiSettingsModal.style.display === 'flex') {
        if (provider) selectProvider(provider);
        showRateLimitError(message, recommendations);
    }
}

async function discoverModels() {
    if (!currentSelectedProvider) return;

    const baseUrl = (selectors.userLlmBaseUrlInput && currentSelectedProvider === 'custom')
        ? selectors.userLlmBaseUrlInput.value
        : '';

    selectors.btnDiscoverModels.disabled = true;
    selectors.btnDiscoverModels.innerText = '⌛...';

    try {
        const url = `${API_BASE}/auth/list-models?provider=${currentSelectedProvider}${baseUrl ? '&base_url=' + encodeURIComponent(baseUrl) : ''}`;
        const res = await authFetch(url);
        const data = await res.json();

        const normalizedModels = normalizeModelNames(data.models || []);
        if (normalizedModels.length > 0) {
            selectors.discoveredModelsList.innerHTML = '';
            normalizedModels.forEach(model => {
                const span = document.createElement('span');
                span.className = 'rec-model-tag'; // reuse same style
                span.style.cursor = 'pointer';
                span.innerText = model;
                span.onclick = () => {
                    selectors.userLlmModelInput.value = model;
                    selectors.userLlmModelInput.classList.remove('input-highlight');
                    void selectors.userLlmModelInput.offsetWidth; // trigger reflow
                    selectors.userLlmModelInput.classList.add('input-highlight');
                };
                selectors.discoveredModelsList.appendChild(span);
            });
            selectors.discoveredModelsContainer.style.display = 'block';
        } else {
            showNotification('No models found for this provider/key.', 'warning');
        }
    } catch (e) {
        console.error("Discovery failed", e);
        showNotification('Discovery failed: ' + e.message, 'error');
    } finally {
        selectors.btnDiscoverModels.disabled = false;
        selectors.btnDiscoverModels.innerText = '✨ Discover';
    }
}

async function syncAiKeyWithServer(provider, key, model, baseUrl = "") {
    try {
        await authFetch(`${API_BASE}/auth/llm-config`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                active_provider: provider,
                providers: { [provider]: { api_key: key, model: model || getDefaultModelForProvider(provider) } }
            })
        });
    } catch (e) {
        console.error("Failed to sync AI key with server", e);
    }
}

function getDefaultModelForProvider(provider) {
    switch (provider) {
        case 'groq': return 'llama-3.3-70b-versatile';
        case 'openai': return 'gpt-4o';
        case 'gemini': return 'gemini-1.5-pro';
        case 'deepseek': return 'deepseek-chat';
        case 'anthropic': return 'claude-3-5-sonnet-20241022';
        default: return 'llama-3.3-70b-versatile';
    }
}

function updateAiStatusUI(hasKey) {
    if (hasKey) {
        selectors.aiStatusBox.style.background = 'rgba(16, 185, 129, 0.1)';
        selectors.aiStatusBox.style.color = '#059669';
        selectors.aiStatusText.innerText = '🛡️ Using Personal API Key';
    } else {
        selectors.aiStatusBox.style.background = 'rgba(59, 130, 246, 0.1)';
        selectors.aiStatusBox.style.color = '#2563eb';
        selectors.aiStatusText.innerText = '🌐 Using System API';
    }
}

// Load current schema
async function loadSchema() {
    try {
        const res = await authFetch(`${API_BASE}/active-schema`);
        if (!res.ok) throw new Error(`Server returned ${res.status}`);

        const schema = await res.json();
        state.schema = schema;
        renderSchema(schema);
    } catch (e) {
        console.error("Failed to load schema:", e);
        selectors.schemaContent.innerHTML = createInfoParagraph('error-msg', `Error: ${e.message}`);
        // If schema fails specifically with 404, we've lost session.
        if (e.message.includes('404')) {
            updateConnectionUI(false);
        }
    }
}

// Render Schema to Sidebar
function renderSchema(schema) {
    if (!schema || !schema.tables || schema.tables.length === 0) {
        selectors.schemaContent.innerHTML = '<p class="empty-msg">No tables accessible.</p>';
        return;
    }
    selectors.schemaContent.innerHTML = schema.tables.map(table => `
        <div class="table-item">
            <span class="table-name">${escapeHtml(table.table)}</span>
            <div class="col-list">
                ${table.columns.map(c => escapeHtml(c.name)).join(', ')}
            </div>
        </div>
    `).join('');
}

// ─── ChatGPT Interface Utilities ─────────────────────────────────────────────
function renderBotThinkingMessage(reqId) {
    const msgDiv = document.createElement('div');
    msgDiv.className = 'message bot thinking-state';
    msgDiv.id = `bot-thinking-${reqId}`;

    msgDiv.innerHTML = `
        <div class="typing-indicator">
            <div class="typing-dots">
                <span></span>
                <span></span>
                <span></span>
            </div>
            <span class="thinking-text" id="thinking-text-${reqId}">Data Agent is thinking...</span>
        </div>
    `;

    clearWorkspaceDefaultStates();
    selectors.chatMessages.appendChild(msgDiv);
    scrollToBottom();
    return msgDiv;
}

/**
 * Typewriter effect for smooth AI text delivery
 * @param {HTMLElement} el - Element to type into
 * @param {string} text - Text to type
 * @param {number} speed - Delay between chars in ms
 */
function typeWriter(el, text, speed = 15, callback = null) {
    if (!el || !text) {
        if (callback) callback();
        return;
    }

    let i = 0;
    el.innerHTML = ''; // Clear previous
    el.classList.add('typing-active');

    function type() {
        if (i < text.length) {
            const container = selectors.chatMessages;
            const isNearBottom = container.scrollHeight - container.scrollTop - container.clientHeight < 80;

            // Handle basic HTML entities or tags if present (simplified)
            if (text.charAt(i) === '<') {
                const end = text.indexOf('>', i);
                if (end !== -1) {
                    el.innerHTML += text.substring(i, end + 1);
                    i = end + 1;
                } else {
                    el.innerHTML += text.charAt(i);
                    i++;
                }
            } else if (text.charAt(i) === '&') {
                const end = text.indexOf(';', i);
                if (end !== -1 && end - i < 10) {
                    el.innerHTML += text.substring(i, end + 1);
                    i = end + 1;
                } else {
                    el.innerHTML += text.charAt(i);
                    i++;
                }
            } else {
                el.innerHTML += text.charAt(i);
                i++;
            }

            if (isNearBottom) {
                scrollToBottom();
            }
            setTimeout(type, speed);
        } else {
            el.classList.remove('typing-active');
            if (callback) callback();
        }
    }

    type();
}

/**
 * Update the status text of a thinking message with a smooth transition
 */
function updateThinkingStatus(reqId, status) {
    const el = document.getElementById(`thinking-text-${reqId}`);
    if (!el) return;

    el.style.opacity = '0';
    setTimeout(() => {
        el.innerText = status;
        el.style.opacity = '1';
    }, 300);
}

// Handle Query Submission
async function handleQuery() {
    const question = selectors.queryInput.value.trim();
    if (!question) return;

    if (!state.connected) {
        showNotification("Please connect to a database first.", 'warning');
        selectors.connectModal.style.display = 'flex';
        return;
    }

    renderUserMessage(question);
    selectors.queryInput.value = '';
    state.queryInFlight = true;

    const reqId = Date.now();
    const thinkingMsg = renderBotThinkingMessage(reqId);

    // Simulated status progression to keep user engaged during long Supabase queries
    const statusSequence = [
        { ms: 1500, text: "Analyzing database schema..." },
        { ms: 3500, text: "Crafting secure SQL query..." },
        { ms: 5500, text: "Retrieving data from Supabase..." },
        { ms: 8000, text: "Formatting results..." }
    ];

    const statusTimers = statusSequence.map(s => setTimeout(() => updateThinkingStatus(reqId, s.text), s.ms));

    try {
        const res = await authFetch(`${API_BASE}/query`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ question })
        });

        // Clear timers once response arrives
        statusTimers.forEach(t => clearTimeout(t));

        if (!res.ok) {
            let errorMessage = "Query failed";
            try {
                const errJson = await res.json();
                errorMessage = errJson.detail || errJson.message || JSON.stringify(errJson);
            } catch (_) {
                const errorText = await res.text();
                errorMessage = errorText || `HTTP ${res.status}`;
            }
            throw new Error(errorMessage);
        }

        const data = await res.json();

        // Remove thinking message before rendering real response
        if (thinkingMsg) thinkingMsg.remove();

        renderBotMessage(data);

        if (data.llm_rate_limit) {
            handleRateLimitSignal(data.llm_rate_limit, data.summary);
        } else if (data.error && String(data.error).toLowerCase().includes('rate limit')) {
            handleRateLimitSignal({ message: data.summary || data.error, recommendations: [] }, data.summary);
        }

        // If the query returned a "not connected" error, sync UI
        if (data.error && data.error.includes("No active database connection")) {
            console.warn("Backend reported no connection during query. Syncing UI.");
            updateConnectionUI(false);
            selectors.schemaContent.innerHTML = '<p class="empty-msg">Disconnected. Please re-connect.</p>';
        }
    } catch (err) {
        if (thinkingMsg) thinkingMsg.remove();
        renderBotMessage({ summary: `Error: ${err.message}`, error: true });
    } finally {
        state.queryInFlight = false;
    }
}

function clearWorkspaceDefaultStates() {
    const emptyState = document.getElementById('workspace-empty-state');
    if (emptyState) emptyState.remove();

    const loadingState = document.getElementById('workspace-loading-state');
    if (loadingState) loadingState.remove();
}

function renderUserMessage(text, scroll = true, persist = true) {
    clearWorkspaceDefaultStates();
    const msgDiv = document.createElement('div');
    msgDiv.className = 'message user';
    msgDiv.innerHTML = `<div class="bubble"><p>${escapeHtml(text)}</p></div>`;
    selectors.chatMessages.appendChild(msgDiv);
    if (persist) {
        appendDashboardCacheMessage({ role: 'user', text });
    }
    if (scroll) scrollToBottom();
}

function scrollToBottom() { selectors.chatMessages.scrollTop = selectors.chatMessages.scrollHeight; }

const VISUALIZATION_MAX_ROWS = 10;

function getVisualizationResults(results) {
    if (!results || !Array.isArray(results.rows)) return results;
    if (results.rows.length <= VISUALIZATION_MAX_ROWS) return results;
    return {
        ...results,
        rows: results.rows.slice(0, VISUALIZATION_MAX_ROWS)
    };
}

function shouldAutoOpenVisualization(data, persist) {
    if (!persist || !data || data.error) return false;
    if (!data.results || !data.results.rows || data.results.rows.length === 0) return false;
    if (!data.visualization) return false;

    const recommendedType = data.visualization.recommended_chart;
    const confidence = Number(data.visualization.confidence || 0);
    if (!recommendedType || recommendedType === 'table') return false;

    return confidence >= 80;
}

function openVisualizationModal(data, fallbackType = 'bar', options = {}) {
    const { autoOpened = false } = options;
    if (!data || !data.results || !data.results.rows || data.results.rows.length === 0) return;

    state.currentVizData = getVisualizationResults(data.results);
    let recommendedType = fallbackType;
    if (data.visualization) {
        recommendedType = data.visualization.recommended_chart || fallbackType;
        selectors.vizRecommendations.name.innerText = recommendedType.toUpperCase();
        selectors.vizRecommendations.confidence.innerText = `${data.visualization.confidence}% Match`;
        selectors.vizRecommendations.reason.innerText = data.visualization.reason;
        selectors.vizRecommendations.bar.style.display = 'flex';
    } else {
        selectors.vizRecommendations.bar.style.display = 'none';
    }

    if (selectors.vizRecommendations.autoOpened) {
        selectors.vizRecommendations.autoOpened.style.display = autoOpened ? 'inline-flex' : 'none';
    }

    selectors.vizModal.style.display = 'flex';
    renderChartOrKPI(recommendedType, state.currentVizData, data.visualization);
    applyVisualizationOptions(data.visualization, state.currentVizData);
}

function slugifyFilename(text, fallback = 'query_results') {
    const normalized = String(text || '')
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '_')
        .replace(/^_+|_+$/g, '');
    return normalized || fallback;
}

function exportResultsToExcel(results, baseName = 'query_results') {
    if (!results || !Array.isArray(results.columns) || !Array.isArray(results.rows) || !results.columns.length) return;

    const headerHtml = '<tr>' + results.columns.map(col => `<th>${escapeHtml(col)}</th>`).join('') + '</tr>';
    const rowsHtml = results.rows.map(row =>
        '<tr>' + results.columns.map(col => `<td>${escapeHtml(row && row[col] !== undefined ? row[col] : '')}</td>`).join('') + '</tr>'
    ).join('');

    const workbookHtml = `
        <html xmlns:o="urn:schemas-microsoft-com:office:office"
              xmlns:x="urn:schemas-microsoft-com:office:excel"
              xmlns="http://www.w3.org/TR/REC-html40">
        <head>
            <meta charset="utf-8" />
            <style>
                table { border-collapse: collapse; width: 100%; }
                th, td { border: 1px solid #dbe2ea; padding: 8px 10px; text-align: left; }
                th { background: #f4f7fb; font-weight: 700; }
            </style>
        </head>
        <body>
            <table>
                <thead>${headerHtml}</thead>
                <tbody>${rowsHtml}</tbody>
            </table>
        </body>
        </html>
    `;

    const blob = new Blob([workbookHtml], { type: 'application/vnd.ms-excel;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `${slugifyFilename(baseName)}.xls`;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
}

async function exportQueryResults(itemId, results, baseName = 'query_results') {
    if (!state.connected) {
        showNotification("Please connect to a database first.", 'warning');
        return;
    }

    if (!itemId) {
        exportResultsToExcel(results, baseName);
        return;
    }

    try {
        const encodedItemId = encodeURIComponent(String(itemId));
        const url = `${API_BASE}/history/export/excel?item_ids=${encodedItemId}&x_session_id=${SESSION_ID}`;
        const res = await authFetch(url);

        if (!res.ok) throw new Error(await res.text());

        const blob = await res.blob();
        const downloadUrl = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = downloadUrl;
        a.download = `${slugifyFilename(baseName)}.xlsx`;
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(downloadUrl);
        a.remove();
    } catch (e) {
        console.error("Full Excel export failed", e);
        showNotification("Full export failed. Downloading the preview rows instead.", 'warning');
        exportResultsToExcel(results, baseName);
    }
}

function getExcelButtonLabel(results) {
    const returnedRows = Array.isArray(results?.rows) ? results.rows.length : 0;
    const totalCount = results?.total_count || returnedRows;
    const hiddenRows = Math.max(0, totalCount - TABLE_PREVIEW_ROWS);

    if (hiddenRows > 0) {
        const moreCount = Math.max(0, totalCount - returnedRows);
        if (moreCount > 0) {
            return `Download Full Excel (${moreCount} more result${moreCount === 1 ? '' : 's'})`;
        }
    }
    return 'Excel Sheet';
}

function renderBotMessage(data, scroll = true, persist = true) {
    if (!window.__sqlAgentBotMessageCounter) {
        window.__sqlAgentBotMessageCounter = 0;
    }
    const reqId = data.id || `local_bot_${++window.__sqlAgentBotMessageCounter}`;
    let msgDiv = document.getElementById(`bot-message-${reqId}`);
    const isNew = !msgDiv;

    if (isNew) {
        clearWorkspaceDefaultStates();
        msgDiv = document.createElement('div');
        msgDiv.className = 'message bot';
        msgDiv.id = `bot-message-${reqId}`;
    }

    const isKPI = data.visualization?.recommended_chart === 'kpi';
    const hasRows = !!(data.results && Array.isArray(data.results.rows) && data.results.rows.length > 0);
    let html = '';

    if (isKPI && data.results?.rows?.length === 1) {
        const row = data.results.rows[0];
        const col = data.results.columns[0];
        const val = row[col];
        const displayVal = typeof val === 'number' ? formatters.value(val) : val;

        html = `
            <div class="kpi-metric-card">
                <div class="kpi-metric-title">${escapeHtml(col)}</div>
                <div class="kpi-metric-value">${escapeHtml(displayVal)}</div>
                <div class="kpi-metric-subtitle">Direct Insight from Data</div>
            </div>
            <div class="bot-details" style="margin-top: 8px; padding: 12px;">
                <button class="sql-toggle-btn"><span>▶</span> View Generated SQL</button>
                <div class="sql-content"><pre><code>${escapeHtml(data.sql || "-- No SQL generated")}</code></pre></div>
            </div>
        `;
    } else {
        let summaryLoading = '<span class="generating-pulse">Running deeper analysis...</span>';

        if (data.error && !data.summary) {
            summaryLoading = `<span style="color: #d32f2f;">${escapeHtml(String(data.error))}</span>`;
        }

        const explanationLoading = '<span style="color: #888; font-style: italic;">SQL explanation skipped to keep responses fast. Inspect the generated SQL below.</span>';

        const summaryHtml = data.summary ? formatAIResponse(data.summary) : summaryLoading;
        const summaryStatus = data.summary ? 'ready' : (data.error ? 'failed' : 'loading');

        // Only trigger typewriter for truly fresh query arrivals that have summaries
        const initialSummary = (persist && isNew && data.summary) ? '' : summaryHtml;
        html = `<div class="bubble" id="llm-summary-${reqId}" data-status="${summaryStatus}">${initialSummary}</div>`;

        // Ensure SQL is still viewable even if there's an error
        if (data.error) {
            html += `
                <div class="bot-details" style="margin-top: 8px;">
                    <button class="sql-toggle-btn"><span>▶</span> View Generated SQL</button>
                    <div class="sql-content">
                        <pre><code>${escapeHtml(data.sql || "-- No SQL generated")}</code></pre>
                    </div>
                </div>
            `;
        } else if (hasRows) {
            html += `<div class="bot-result-panel">`;
            if (hasRows) {
                html += `
                    <div class="bot-actions">
                        <button class="viz-results-btn"><span>📊</span> Visualize Results</button>
                        <button class="excel-results-btn"><span>📄</span> ${getExcelButtonLabel(data.results)}</button>
                    </div>
                `;
            }

            const tableHtml = createTableHTML(data.results?.columns, data.results?.rows, data.results?.total_count);
            if (tableHtml) {
                html += `<div class="table-wrapper bot-table">${tableHtml}</div>`;
            }

            const explanationHtml = data.explanation ? formatAIResponse(data.explanation) : explanationLoading;
            const explanationStatus = data.explanation ? 'ready' : 'skipped';
            const initialExplanation = (persist && data.explanation) ? '' : explanationHtml;

            html += `
                <div class="bot-details">
                    <button class="sql-toggle-btn"><span>▶</span> View Generated SQL & Explanation</button>
                    <div class="sql-content">
                        <div class="llm-explanation" id="llm-explanation-${reqId}" data-status="${explanationStatus}" style="margin-bottom: 12px; padding-bottom: 12px; border-bottom: 1px solid #eaeaea;">
                            ${initialExplanation}
                        </div>
                        <pre><code>${escapeHtml(data.sql || "-- No SQL generated")}</code></pre>
                    </div>
                </div>
            `;
        }
    }

    if (!isKPI && !data.error && hasRows && html.includes('bot-result-panel')) {
        html += `</div>`;
    }

    msgDiv.innerHTML = html;
    if (isNew) {
        selectors.chatMessages.appendChild(msgDiv);
    }

    // Trigger typewriter for fresh summaries/explanations only once
    if (persist && isNew) {
        if (data.summary) {
            const sumEl = msgDiv.querySelector(`#llm-summary-${data.id || reqId}`);
            if (sumEl) typeWriter(sumEl, formatAIResponse(data.summary), 12);
        }
        if (data.explanation) {
            const explEl = msgDiv.querySelector(`#llm-explanation-${data.id || reqId}`);
            if (explEl) typeWriter(explEl, formatAIResponse(data.explanation), 8);
        }
    }

    if (persist) {
        upsertDashboardCacheMessage({
            role: 'assistant',
            data: {
                id: data.id || reqId,
                question: data.question,
                summary: data.summary,
                sql: data.sql,
                results: data.results,
                visualization: data.visualization,
                error: data.error
            }
        });
    }

    const toggleBtn = msgDiv.querySelector('.sql-toggle-btn');
    if (toggleBtn) {
        toggleBtn.onclick = function () {
            const content = msgDiv.querySelector('.sql-content');
            const isShown = content.classList.toggle('show');
            this.querySelector('span').innerText = isShown ? '▼' : '▶';
        };
    }

    const vizBtn = msgDiv.querySelector('.viz-results-btn');
    if (vizBtn) {
        vizBtn.onclick = () => {
            if (data.results && data.results.rows && data.results.rows.length > 0) {
                openVisualizationModal(data, 'bar');
            } else {
                showNotification("No data to visualize.", 'warning');
            }
        };
    }

    const excelBtn = msgDiv.querySelector('.excel-results-btn');
    if (excelBtn) {
        excelBtn.onclick = async () => {
            await exportQueryResults(data.id, data.results, data.question || 'query_results');
        };
    }

    // Ensure visualization renders properly initially if it exists
    if (shouldAutoOpenVisualization(data, persist) && !data._vizOpened) {
        data._vizOpened = true;
        setTimeout(() => openVisualizationModal(data, 'bar', { autoOpened: true }), 120);
    }
    if (scroll) scrollToBottom();

    // Begin async polling only for the enrichments that are hydrated in the background.
    // We should poll even if hasRows is false, because the AI still generates a summary explaining the empty results.
    if (data.id && !data.error && (!data.summary || (hasRows && !data.visualization))) {
        pollForInsights(data.id, data);
    }
}

async function pollForInsights(reqId, originalData, attempts = 0) {
    const MAX_ATTEMPTS = 45; // ~70 seconds
    const pollIdStr = String(reqId);
    
    if (attempts === 0) console.log(`[Polling] Started for reqId: ${pollIdStr}`);

    if (attempts > MAX_ATTEMPTS) {
        console.warn(`[Polling] Max attempts reached for ${pollIdStr}. Giving up.`);
        const sumEl = document.getElementById(`llm-summary-${reqId}`);
        if (sumEl && sumEl.getAttribute('data-status') === 'loading') {
            sumEl.innerHTML = '<span style="color: #888; font-style: italic;">Analysis unavailable.</span>';
            sumEl.setAttribute('data-status', 'failed');
        }
        return;
    }

    try {
        // Cache busting is critical for background hydration polling
        const historyUrl = `${API_BASE}/history?limit=15&sort=desc&_cache_bust=${Date.now()}`;
        const res = await authFetch(historyUrl);
        
        if (res.ok) {
            const history = await res.json();
            
            // Comprehensive match: check request_id and primary id
            const item = history.find(h => {
                const hReqId = h.request_id ? String(h.request_id) : null;
                const hId = h.id ? String(h.id) : null;
                return hReqId === pollIdStr || hId === pollIdStr;
            });

            if (item) {
                let hydrationCompleted = false;

                // 1. Hydrate Summary
                if (item.summary) {
                    const sumEl = document.getElementById(`llm-summary-${reqId}`);
                    // Update if loading OR if it's the very first attempt (handling fast-track results)
                    if (sumEl && (sumEl.getAttribute('data-status') === 'loading' || attempts === 0)) {
                        console.log(`[Polling] Hydrating summary for ${pollIdStr}`);
                        sumEl.setAttribute('data-status', 'ready');
                        typeWriter(sumEl, formatAIResponse(item.summary), 12);
                        originalData.summary = item.summary;
                        
                        upsertDashboardCacheMessage({
                            role: 'assistant',
                            data: { ...originalData, summary: item.summary }
                        });
                        hydrationCompleted = true;
                    }
                }

                // 2. Hydrate Visualization
                if (item.visualization) {
                    originalData.visualization = item.visualization;
                    if (shouldAutoOpenVisualization(originalData, true) && !originalData._vizOpened) {
                        console.log(`[Polling] Auto-opening viz for ${pollIdStr}`);
                        originalData._vizOpened = true;
                        openVisualizationModal(originalData, 'bar', { autoOpened: true });
                    }
                }

                // If summary is ready, we are done
                if (item.summary) {
                    console.log(`[Polling] Success for ${pollIdStr}`);
                    return;
                }
            }
        }
    } catch (e) {
        console.error(`[Polling] Error for ${pollIdStr}:`, e);
    }

    // Exponential backoff or constant interval
    const nextInterval = attempts < 5 ? 1200 : 2000;
    setTimeout(() => pollForInsights(reqId, originalData, attempts + 1), nextInterval);
}


function applyVisualizationOptions(visualization, results) {
    const allowed = new Set();
    const isMultiRow = results && results.rows && results.rows.length > 1;

    // If we have alternatives from backend, show them
    if (visualization && visualization.alternatives) {
        visualization.alternatives.forEach(alt => {
            if (alt.chart !== 'table') allowed.add(alt.chart);
        });
    }

    // Always ensure at least Bar is available if multi-row
    if (isMultiRow) {
        allowed.add('bar');
        allowed.add('line');
        allowed.add('pie');
    }

    // Explicitly add recommended if not table
    if (visualization && visualization.recommended_chart !== 'table') {
        allowed.add(visualization.recommended_chart);
    }

    selectors.vizOptionButtons.forEach(btn => {
        const type = btn.getAttribute('data-type');
        btn.style.display = allowed.has(type) ? 'inline-flex' : 'none';

        // Highlight current type
        const currentType = (visualization && visualization.recommended_chart === 'table' && isMultiRow) ? 'bar' : (visualization?.recommended_chart || 'bar');
        if (type === currentType) {
            btn.classList.add('active');
        } else {
            btn.classList.remove('active');
        }
    });
}

function renderChartOrKPI(type, results, config = null) {
    // Defensive check for 'table' type which ECharts cannot render
    if (type === 'table') {
        const isSingle = results.rows?.length === 1 && results.columns?.length === 1;
        const isMultiRow = results.rows?.length > 1;

        if (isSingle) {
            type = 'kpi'; // Redirect single-value tables to KPI card
        } else if (isMultiRow) {
            // If it's a multi-row table recommendation, fallback to bar for visualization
            type = 'bar';
        } else {
            // Hide both containers if it's really not chartable
            selectors.vizContainer.style.display = 'none';
            selectors.kpiContainer.style.display = 'none';
            return;
        }
    }

    selectors.vizContainer.style.display = type === 'kpi' ? 'none' : 'block';
    selectors.kpiContainer.style.display = type === 'kpi' ? 'flex' : 'none';

    if (type === 'kpi') {
        const row = results.rows[0];
        const val = row[results.columns[0]];
        const kpiTitleEl = document.getElementById('kpi-title');
        const kpiValueEl = document.getElementById('kpi-value');

        if (kpiTitleEl) kpiTitleEl.innerText = results.columns[0];
        if (kpiValueEl) kpiValueEl.innerText = typeof val === 'number' ? formatters.value(val) : val;
    } else {
        renderEChart(type, results, config);
    }
}

function renderEChart(type, results, config) {
    if (!window.echarts) return;
    const existing = echarts.getInstanceByDom(selectors.vizContainer);
    if (existing) existing.dispose();
    const chart = echarts.init(selectors.vizContainer);
    const cols = results.columns;
    const rows = results.rows;
    const xCol = (config && config.x_axis) ? config.x_axis : cols[0];
    const yCol = (config && config.y_axis) ? config.y_axis : null;
    const numericCols = cols.filter(c => rows.some(r => typeof r[c] === 'number'));
    const yCols = (config && config.all_metrics && config.all_metrics.length)
        ? config.all_metrics
        : numericCols.filter(c => c !== xCol);
    const resolvedYCols = yCols.length ? yCols : numericCols.slice(0, 1);
    const xAxisType = (config && config.x_axis_type) ? config.x_axis_type : 'category';
    const chartType = (config && config.series_type) ? config.series_type : type;
    const axisData = rows.map(r => r[xCol]);
    const isCartesian = ['line', 'bar', 'area', 'scatter', 'histogram', 'candlestick'].includes(chartType);
    const allowBrush = ['line', 'bar', 'scatter', 'area'].includes(chartType);
    const allowMagicType = ['line', 'bar', 'area'].includes(chartType);

    const truncatedAxisLabel = (value) => {
        const text = String(value ?? '');
        return text.length > 18 ? `${text.slice(0, 18)}...` : text;
    };

    const buildToolbox = () => {
        const feature = {
            restore: { title: 'Reset view' },
            saveAsImage: { title: 'Save image' },
            dataView: { title: 'View data', readOnly: true }
        };

        if (isCartesian) {
            feature.dataZoom = { title: { zoom: 'Zoom', back: 'Reset zoom' } };
        }

        if (allowMagicType) {
            feature.magicType = { type: ['line', 'bar'] };
        }

        if (allowBrush) {
            feature.brush = {
                type: ['rect', 'polygon', 'clear'],
                title: {
                    rect: 'Box select',
                    polygon: 'Lasso select',
                    clear: 'Clear selection'
                }
            };
        }

        return {
            right: 10,
            top: 4,
            itemSize: 16,
            feature
        };
    };

    const buildDataZoom = () => {
        if (!isCartesian || axisData.length <= 12) return [];
        return [
            { type: 'inside', start: 0, end: Math.min(100, Math.max(22, (12 / axisData.length) * 100)) },
            { type: 'slider', height: 18, bottom: 6, start: 0, end: Math.min(100, Math.max(22, (12 / axisData.length) * 100)) }
        ];
    };

    let option;

    if (chartType === 'pie') {
        const valueCol = yCol || resolvedYCols[0];
        if (!valueCol) return;
        option = {
            color: chartTheme.colors,
            tooltip: { trigger: 'item', formatter: '{b}<br/>{c} ({d}%)' },
            legend: { bottom: '2%' },
            toolbox: buildToolbox(),
            series: [{
                name: valueCol || 'Value',
                type: 'pie',
                radius: ['35%', '68%'],
                center: ['50%', '48%'],
                itemStyle: { borderRadius: 8, borderColor: '#fff', borderWidth: 2 },
                label: { formatter: '{b}\n{d}%' },
                data: rows.map(r => ({
                    name: r[xCol],
                    value: r[valueCol]
                }))
            }]
        };
    } else if (chartType === 'scatter') {
        const xNumeric = xCol;
        const yNumeric = yCol || resolvedYCols[0];
        if (!xNumeric || !yNumeric) return;
        option = {
            color: chartTheme.colors,
            tooltip: {
                trigger: 'item',
                formatter: (params) => {
                    const point = params.data || [];
                    return `${xNumeric}: ${formatters.value(point[0])}<br/>${yNumeric}: ${formatters.value(point[1])}`;
                }
            },
            toolbox: buildToolbox(),
            brush: allowBrush ? { toolbox: ['rect', 'polygon', 'clear'], xAxisIndex: 'all', yAxisIndex: 'all' } : undefined,
            grid: { bottom: '10%', containLabel: true },
            xAxis: { type: 'value', name: xNumeric, axisLabel: { formatter: (v) => formatters.value(v) } },
            yAxis: { type: 'value', name: yNumeric, axisLabel: { formatter: (v) => formatters.value(v) } },
            series: [{
                type: 'scatter',
                symbolSize: 12,
                data: rows.map(r => [r[xNumeric], r[yNumeric]])
            }]
        };
    } else if (chartType === 'histogram') {
        const valueCol = xCol;
        const values = rows.map(r => Number(r[valueCol])).filter(v => Number.isFinite(v));
        if (!values.length) return;
        const bucketCount = Math.min(10, Math.max(5, Math.round(Math.sqrt(values.length || 1))));
        const min = Math.min(...values);
        const max = Math.max(...values);
        const width = bucketCount > 0 && max !== min ? (max - min) / bucketCount : 1;
        const bins = Array.from({ length: bucketCount }, (_, idx) => {
            const start = min + idx * width;
            const end = idx === bucketCount - 1 ? max : start + width;
            return { start, end, count: 0 };
        });

        values.forEach(v => {
            let index = width === 1 && max === min ? 0 : Math.min(bucketCount - 1, Math.floor((v - min) / width));
            if (index < 0) index = 0;
            bins[index].count += 1;
        });

        option = {
            color: chartTheme.colors,
            tooltip: { trigger: 'axis' },
            toolbox: buildToolbox(),
            dataZoom: buildDataZoom(),
            grid: { bottom: '18%', containLabel: true },
            xAxis: {
                type: 'category',
                name: valueCol,
                data: bins.map(bin => `${formatters.value(bin.start)}-${formatters.value(bin.end)}`),
                axisLabel: { formatter: truncatedAxisLabel }
            },
            yAxis: { type: 'value', name: 'Count' },
            series: [{
                type: 'bar',
                barMaxWidth: 48,
                data: bins.map(bin => bin.count)
            }]
        };
    } else if (chartType === 'candlestick' && config && config.ohlc_cols) {
        const ohlc = config.ohlc_cols;
        option = {
            color: ['#16a34a', '#dc2626'],
            tooltip: { trigger: 'axis' },
            toolbox: buildToolbox(),
            dataZoom: buildDataZoom(),
            axisPointer: { link: [{ xAxisIndex: 'all' }] },
            grid: { bottom: '18%', containLabel: true },
            xAxis: { type: 'category', data: axisData, axisLabel: { rotate: 30, formatter: truncatedAxisLabel } },
            yAxis: { type: 'value', scale: true },
            series: [{
                type: 'candlestick',
                data: rows.map(r => [
                    r[ohlc.open],
                    r[ohlc.close],
                    r[ohlc.low],
                    r[ohlc.high]
                ])
            }]
        };
    } else {
        option = {
            color: chartTheme.colors,
            tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
            legend: { top: '5%' },
            toolbox: buildToolbox(),
            brush: allowBrush ? { toolbox: ['rect', 'polygon', 'clear'], xAxisIndex: 'all', yAxisIndex: 'all' } : undefined,
            dataZoom: buildDataZoom(),
            grid: { bottom: axisData.length > 12 ? '18%' : '10%', containLabel: true },
            xAxis: {
                type: xAxisType === 'date' ? 'category' : 'category',
                data: axisData,
                axisLabel: { rotate: 30, formatter: truncatedAxisLabel }
            },
            yAxis: { type: 'value', axisLabel: { formatter: (v) => formatters.value(v) } },
            series: resolvedYCols.map(y => ({
                name: y,
                type: chartType === 'area' ? 'line' : chartType,
                areaStyle: chartType === 'area' ? {} : null,
                data: rows.map(r => r[y]),
                smooth: chartType === 'line' || chartType === 'area'
            }))
        };
    }

    chart.setOption(option);
    window.addEventListener('resize', () => chart.resize(), { once: true });
}

function formatAIResponse(text) {
    if (!text) return "No insight.";
    let formatted = escapeHtml(text).replace(/\*\*(.*?)\*\*/g, '<b>$1</b>');
    return formatted.split('\n').filter(p => p.trim()).map(p => `<p>${p}</p>`).join('');
}

const TABLE_PREVIEW_ROWS = 10;
let tablePreviewIdCounter = 0;

window.toggleTablePreview = function (button, hiddenId, hasLocalData) {
    const hiddenRows = document.querySelectorAll(`.${hiddenId}`);
    if (!hiddenRows.length && !hasLocalData) {
        showNotification("Refine your search or export to Excel to see all results.", "info");
        return;
    }

    const isExpanded = hiddenRows[0] ? !hiddenRows[0].classList.contains('show') : false;
    hiddenRows.forEach(row => row.classList.toggle('show', isExpanded));
    const remaining = Number(button.getAttribute('data-remaining') || '0');

    button.innerText = isExpanded
        ? 'Show less'
        : `See ${remaining} more row${remaining === 1 ? '' : 's'}`;
};

function createTableHTML(cols, rows, totalCount) {
    if (!cols || !cols.length) return ''; // Hide empty tables or wait for data

    const safeRows = Array.isArray(rows) ? rows : [];
    const previewRows = safeRows.slice(0, TABLE_PREVIEW_ROWS);

    const renderRows = (rowSet, rowClass = '') => rowSet
        .map(r => `<tr${rowClass ? ` class="${rowClass}"` : ''}>` + cols.map(c => `<td>${escapeHtml(r ? (r[c] !== undefined ? r[c] : '') : '')}</td>`).join('') + '</tr>')
        .join('');

    let html = '<table class="results-table"><thead><tr>' + cols.map(c => `<th>${escapeHtml(c)}</th>`).join('') + '</tr></thead><tbody>';
    html += renderRows(previewRows);
    html += '</tbody></table>';

    return html;
}

function setLoading(isLoading, text) {
    selectors.loadingOverlay.style.display = isLoading ? 'flex' : 'none';
    document.getElementById('loading-text').innerText = text;
}

// ─── Main View Navigation ────────────────────────────────────────────────────
function switchMainTab(tabId) {
    const storage = getStorageSource();
    storage.setItem('sql_agent_active_tab', tabId);
    if (tabId === 'dashboard') {
        selectors.navDashboard.classList.add('active');
        selectors.navHistory.classList.remove('active');
        selectors.dashboardView.style.display = 'flex';
        selectors.historyView.style.display = 'none';

    } else if (tabId === 'history') {
        selectors.navHistory.classList.add('active');
        selectors.navDashboard.classList.remove('active');
        selectors.historyView.style.display = 'flex';
        selectors.dashboardView.style.display = 'none';

        // Ensure bulk toolbar is shown if history exists
        if (state.history.length > 0 && selectors.bulkHistoryActions) {
            selectors.bulkHistoryActions.style.display = 'flex';
            updateBulkActionState();
        }
    }
}

// ─── History Management ──────────────────────────────────────────────────────
async function populateDashboardFromHistory() {
    try {
        if (!state.currentDatabase) return;
        if (state.queryInFlight) return;

        const hasExistingMessages = selectors.chatMessages.children.length > 0;
        const alreadyHydrated = state.dashboardHydratedDb === state.currentDatabase;
        if (hasExistingMessages && alreadyHydrated) return;

        if (!hasExistingMessages) {
            renderDashboardLoadingState();
        }

        // Fetch most recent 10 messages for dashboard (desc gets latest reversed to asc)
        const res = await authFetch(`${API_BASE}/history?limit=10&sort=desc`);
        if (!res.ok) {
            if (restoreDashboardFromCache(state.currentDatabase)) {
                state.dashboardHydratedDb = state.currentDatabase;
            }
            return;
        }

        const history = await res.json();
        if (history.length === 0) {
            if (restoreDashboardFromCache(state.currentDatabase)) {
                state.dashboardHydratedDb = state.currentDatabase;
            } else {
                renderDashboardEmptyState();
            }
            state.dashboardHydratedDb = state.currentDatabase;
            return;
        }

        // Chronic order for chat view (newest at bottom)
        history.reverse();

        const restoredMessages = buildDashboardMessagesFromHistory(history);
        const cachedMessages = readDashboardCache(state.currentDatabase);
        const currentVisibleMessages = selectors.chatMessages.querySelectorAll('.message').length;
        const bestKnownMessageCount = Math.max(cachedMessages.length, currentVisibleMessages);

        if (hasExistingMessages && currentVisibleMessages > 0) {
            state.dashboardHydratedDb = state.currentDatabase;
            return;
        }

        // Do not wipe the current chat with a partial backend history response.
        if (bestKnownMessageCount > 0 && restoredMessages.length < bestKnownMessageCount) {
            console.warn(
                `[Dashboard Restore] Skipping partial history hydrate for ${state.currentDatabase}. ` +
                `Restored ${restoredMessages.length} messages, but ${bestKnownMessageCount} are already visible/cached.`
            );
            if (!hasExistingMessages) {
                if (!restoreDashboardFromCache(state.currentDatabase)) {
                    renderDashboardEmptyState(
                        'No chats available to restore yet.',
                        'Supabase did not return recent messages for this database. Ask a question to begin a new chat.'
                    );
                }
            }
            state.dashboardHydratedDb = state.currentDatabase;
            return;
        }

        // Clear existing (except greeting if needed, but usually we want a clean restore)
        selectors.chatMessages.innerHTML = '';
        writeDashboardCache(restoredMessages, state.currentDatabase);

        history.forEach(item => {
            const question = item.question || item.user;
            const response = {
                id: item.request_id || item.id,
                question,
                summary: item.summary || item.assistant,
                sql: item.sql,
                results: item.results,
                visualization: item.visualization,
                error: item.has_error || false
            };

            renderUserMessage(question, false, false); // false = Don't scroll yet
            renderBotMessage(response, false, false);
        });

        state.dashboardHydratedDb = state.currentDatabase;
        scrollToBottom();
    } catch (e) {
        console.error("Failed to populate dashboard from history", e);
        if (!restoreDashboardFromCache(state.currentDatabase)) {
            renderDashboardEmptyState(
                'We could not load previous chats right now.',
                'Your database connection is active, but chat history did not come back from the server.'
            );
        }
    }
}

async function loadHistory() {
    try {
        let url = `${API_BASE}/history?sort=${state.historyFilters.sort}`;
        if (state.historyFilters.start) url += `&start_date=${state.historyFilters.start}`;
        if (state.historyFilters.end) url += `&end_date=${state.historyFilters.end + 'T23:59:59'}`;

        const res = await authFetch(url);
        if (!res.ok) return;

        const history = await res.json();
        state.history = history;

        // Render into the dedicated history list
        if (!selectors.historyList) return;

        selectors.historyList.innerHTML = ''; // Clear previous

        if (history.length === 0) {
            selectors.historyList.innerHTML = '<p class="empty-msg" style="text-align: center; color: #888; padding: 40px 0;">No history available for this database.</p>';
            if (selectors.bulkHistoryActions) selectors.bulkHistoryActions.style.display = 'none';
        } else {
            // Show bulk actions toolbar
            if (selectors.bulkHistoryActions) {
                selectors.bulkHistoryActions.style.display = 'flex';
                selectors.selectAllHistory.checked = false;
                updateBulkActionState();
            }

            history.forEach(item => {
                const question = item.question || item.user;
                const summary = item.summary || item.assistant;
                const itemId = item.id;

                const blockDiv = document.createElement('div');
                blockDiv.className = 'history-block';
                blockDiv.style.cssText = 'background: #fafafa; border: 1px solid #eee; border-radius: 8px; margin-bottom: 24px; padding: 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.02); position: relative; display: flex; gap: 16px;';

                // Add Checkbox column
                let checkboxHtml = '';
                if (itemId) {
                    checkboxHtml = `
                    <div style="padding-top: 4px;">
                        <input type="checkbox" class="history-item-checkbox" data-id="${itemId}" style="cursor: pointer; width: 16px; height: 16px;" onchange="updateBulkActionState()">
                    </div>`;
                }

                // Main content wrapper
                let contentHtml = `<div style="flex: 1; position: relative;">`;

                // Action Buttons container in the top right
                let actionsHtml = '';
                if (itemId) {
                    actionsHtml = `
                    <div class="history-item-actions" style="position: absolute; top: -4px; right: 0; display: flex; gap: 8px;">
                        <button onclick="exportHistoryItem(${itemId}, 'excel')" class="btn-secondary" style="padding: 4px 8px; font-size: 0.7rem;">📊 Excel</button>
                        <button onclick="exportHistoryItem(${itemId}, 'pdf')" class="btn-secondary" style="padding: 4px 8px; font-size: 0.7rem;">📄 PDF</button>
                        <button onclick="deleteHistoryItem(${itemId})" class="btn-secondary" style="padding: 4px 8px; font-size: 0.7rem; color: #d32f2f; border-color: rgba(211,47,47,0.2); background: rgba(211,47,47,0.05);">🗑️ Delete</button>
                    </div>`;
                }

                let html = `
                    ${actionsHtml}
                    <div style="font-size: 0.75rem; color: #888; margin-bottom: 12px; font-weight: 500; text-transform: uppercase;">${escapeHtml(formatters.date(item.timestamp || Date.now()))}</div>
                `;

                if (question) {
                    html += `<div class="history-question" style="font-weight: 600; color: #333; margin-bottom: 12px; font-size: 1.05rem;">Q: ${escapeHtml(question)}</div>`;
                }

                if (summary) {
                    html += `<div class="history-summary" style="color: #555; line-height: 1.5; margin-bottom: 16px;">${formatAIResponse(summary)}</div>`;

                    if (item.sql || (item.results && item.results.columns)) {
                        html += `
                            <div class="bot-details" style="margin-top: 16px; border-top: 1px solid #eaeaea; padding-top: 16px;">
                                <button class="sql-toggle-btn" onclick="this.nextElementSibling.classList.toggle('show'); this.querySelector('span').innerText = this.nextElementSibling.classList.contains('show') ? '▼' : '▶';" style="color: var(--brand);"><span>▶</span> View Generated SQL</button>
                                <div class="sql-content" style="background: #f4f4f4; border-radius: 6px; margin: 8px 0;"><pre style="padding: 12px; font-size: 0.85rem; color: #333;"><code>${escapeHtml(item.sql || "-- No SQL generated")}</code></pre></div>
                        `;

                        if (item.results && item.results.columns) {
                            html += `
                                <button class="viz-results-btn" style="color: var(--brand); margin-top: 8px;"><span>📊</span> Visualize Results</button>
                                <button class="excel-results-btn" style="margin-top: 8px;"><span>📄</span> ${getExcelButtonLabel(item.results)}</button>
                                <div class="table-wrapper" style="margin-top: 12px;">${createTableHTML(item.results.columns, item.results.rows)}</div>
                            `;
                        }
                        html += `</div>`;
                    }
                }

                contentHtml += html + `</div>`;
                blockDiv.innerHTML = checkboxHtml + contentHtml;
                selectors.historyList.appendChild(blockDiv);

                // Bind viz button for this history block
                const vizBtn = blockDiv.querySelector('.viz-results-btn');
                if (vizBtn) {
                    vizBtn.onclick = () => {
                        if (item.results && item.results.rows && item.results.rows.length > 0) {
                            openVisualizationModal({
                                results: item.results,
                                visualization: item.visualization
                            }, 'bar');
                        } else {
                            showNotification("No data to visualize.", 'warning');
                        }
                    };
                }
                const excelBtn = blockDiv.querySelector('.excel-results-btn');
                if (excelBtn) {
                    excelBtn.onclick = async () => {
                        await exportQueryResults(item.id || item.request_id, item.results, question || 'history_results');
                    };
                }
            });
        }

    } catch (e) {
        console.error("Failed to load history", e);
    }
}

async function handleClearHistory() {
    if (!state.connected) return showNotification("Please connect to a database first.", 'warning');

    showConfirmationModal(
        "Clear Chat History",
        "Are you sure you want to PERMANENTLY clear your chat history for this database? This action cannot be undone.",
        async () => {
            try {
                await authFetch(`${API_BASE}/history/clear`, { method: 'DELETE' });

                // Only clear the history view list, leave dashboard chat alone
                if (selectors.historyList) {
                    selectors.historyList.innerHTML = '<p class="empty-msg" style="text-align: center; color: #888; padding: 40px 0;">History cleared.</p>';
                }

                state.history = [];
                clearDashboardCache(state.currentDatabase);
                showNotification("History cleared successfully", "success");
            } catch (e) {
                console.error("Clear history failed", e);
                showNotification("Failed to clear history", "error");
            }
        }
    );
}

async function exportHistory(format) {
    const url = `${API_BASE}/history/export/${format}?x_session_id=${SESSION_ID}`;
    // Use a hidden anchor to trigger download
    const a = document.createElement('a');
    a.href = url;
    // For authenticated requests, simple anchor doesn't send headers. 
    // But our export endpoints can take session ID in query if needed, or we fetch as blob.

    try {
        const res = await authFetch(url);
        if (!res.ok) throw new Error("Export failed");
        const blob = await res.blob();
        const downloadUrl = window.URL.createObjectURL(blob);
        a.href = downloadUrl;
        a.download = `chat_history_${new Date().getTime()}.${format === 'excel' ? 'xlsx' : 'pdf'}`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
    } catch (e) {
        showNotification("Export failed: " + e.message, 'error');
    }
}

async function exportHistoryItem(itemId, format) {
    if (!state.connected) {
        showNotification("Please connect to a database first.", 'warning');
        return;
    }

    try {
        const url = `${API_BASE}/history/export/${format}?item_ids=${itemId}&x_session_id=${SESSION_ID}`;
        const res = await authFetch(url);

        if (!res.ok) throw new Error(await res.text());

        const blob = await res.blob();
        const downloadUrl = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = downloadUrl;
        const ext = format === 'excel' ? 'xlsx' : 'pdf';
        a.download = `chat_history_item_${itemId}.${ext}`;
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(downloadUrl);
        a.remove();
    } catch (e) {
        console.error("Export failed", e);
        showNotification("Failed to export history item: " + e.message, 'error');
    }
}

async function deleteHistoryItem(itemId) {
    showConfirmationModal(
        "Delete History Item",
        "Are you sure you want to delete this specific history item? This will remove all associated insights and code.",
        async () => {
            try {
                const res = await authFetch(`${API_BASE}/history?item_ids=${itemId}`, { method: 'DELETE' });
                if (!res.ok) throw new Error(await res.text());

                showNotification("History item deleted", "success");
                await loadHistory();
            } catch (e) {
                console.error("Failed to delete history item", e);
                showNotification("Delete failed: " + e.message, 'error');
            }
        }
    );
}

// ─── Bulk History Management ──────────────────────────────────────────────────
function getSelectedHistoryIds() {
    const checkboxes = document.querySelectorAll('.history-item-checkbox:checked');
    return Array.from(checkboxes).map(cb => cb.getAttribute('data-id'));
}

window.updateBulkActionState = function () {
    const selectedIds = getSelectedHistoryIds();
    const count = selectedIds.length;

    if (selectors.selectedHistoryCount) {
        selectors.selectedHistoryCount.innerText = `(${count} selected)`;
    }

    const disabled = count === 0;
    if (selectors.btnBulkExportPdf) selectors.btnBulkExportPdf.disabled = disabled;
    if (selectors.btnBulkDelete) selectors.btnBulkDelete.disabled = disabled;
};

if (selectors.selectAllHistory) {
    selectors.selectAllHistory.addEventListener('change', (e) => {
        const isChecked = e.target.checked;
        document.querySelectorAll('.history-item-checkbox').forEach(cb => {
            cb.checked = isChecked;
        });
        updateBulkActionState();
    });
}


if (selectors.btnBulkExportPdf) {
    selectors.btnBulkExportPdf.addEventListener('click', () => {
        const ids = getSelectedHistoryIds();
        if (ids.length) exportBulkHistory('pdf', ids);
    });
}

if (selectors.btnBulkDelete) {
    selectors.btnBulkDelete.addEventListener('click', async () => {
        const ids = getSelectedHistoryIds();
        if (!ids.length) return;

        showConfirmationModal(
            "Delete Multiple Items",
            `Are you sure you want to delete ${ids.length} history items? This action cannot be undone and will permanently remove all associated results.`,
            async () => {
                try {
                    const res = await authFetch(`${API_BASE}/history?item_ids=${ids.join(',')}`, { method: 'DELETE' });
                    if (!res.ok) throw new Error(await res.text());

                    showNotification(`${ids.length} items deleted successfully`, "success");
                    await loadHistory();
                    if (selectors.selectAllHistory) selectors.selectAllHistory.checked = false;
                    updateBulkActionState();
                } catch (e) {
                    console.error("Bulk delete failed", e);
                    showNotification("Delete failed: " + e.message, 'error');
                }
            }
        );
    });
}

async function exportBulkHistory(format, itemIds) {
    if (!state.connected) return;

    try {
        const url = `${API_BASE}/history/export/${format}?item_ids=${itemIds.join(',')}&x_session_id=${SESSION_ID}`;
        const res = await authFetch(url);

        if (!res.ok) throw new Error(await res.text());

        const blob = await res.blob();
        const downloadUrl = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = downloadUrl;
        a.download = `bulk_history_export.${format === 'excel' ? 'xlsx' : 'pdf'}`;
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(downloadUrl);
        a.remove();

        // Uncheck all after export
        if (selectors.selectAllHistory) {
            selectors.selectAllHistory.checked = false;
            document.querySelectorAll('.history-item-checkbox').forEach(cb => cb.checked = false);
            updateBulkActionState();
        }
    } catch (e) {
        console.error("Bulk export failed", e);
        showNotification("Failed to export items: " + e.message, 'error');
    }
}

// ─── Event Bindings for Filtering ─────────────────────────────────────────────
if (selectors.btnApplyHistFilter) {
    selectors.btnApplyHistFilter.onclick = () => {
        state.historyFilters.start = selectors.histFilterStart.value;
        state.historyFilters.end = selectors.histFilterEnd.value;
        state.historyFilters.sort = selectors.histSort.value;
        loadHistory();
    };
}
if (selectors.btnClearHistFilter) {
    selectors.btnClearHistFilter.onclick = () => {
        selectors.histFilterStart.value = '';
        selectors.histFilterEnd.value = '';
        selectors.histSort.value = 'desc';
        state.historyFilters = { start: '', end: '', sort: 'desc' };
        loadHistory();
    };
}
if (selectors.histSort) {
    selectors.histSort.onchange = () => {
        state.historyFilters.sort = selectors.histSort.value;
        loadHistory();
    };
}

// Start App
initializeApp();

/**
 * Premium Global Toast Notification System
 * type: 'success' | 'error' | 'warning' | 'info'
 */
function showNotification(message, type = 'info') {
    const container = document.getElementById('toast-container');
    if (!container) return console.warn('Toast container not found');

    const toast = document.createElement('div');
    toast.className = `toast ${type}`;

    let icon = '🔔';
    if (type === 'success') icon = '✅';
    if (type === 'error') icon = '❌';
    if (type === 'warning') icon = '⚠️';

    toast.innerHTML = `
        <div class="toast-icon">${icon}</div>
        <div class="toast-content">
            <div class="toast-message">${message}</div>
        </div>
    `;

    container.appendChild(toast);

    // Auto-remove after 4 seconds
    setTimeout(() => {
        toast.classList.add('fade-out');
        setTimeout(() => toast.remove(), 400);
    }, 4000);
}

/**
 * Premium Global Confirmation Modal
 * @param {string} title - The modal title
 * @param {string} message - The confirmation message
 * @param {function} onConfirm - Callback for confirm action
 */
function showConfirmationModal(title, message, onConfirm) {
    const overlay = document.getElementById('confirm-modal-overlay');
    const titleEl = document.getElementById('confirm-modal-title');
    const msgEl = document.getElementById('confirm-modal-message');
    const confirmBtn = document.getElementById('confirm-modal-confirm');
    const cancelBtn = document.getElementById('confirm-modal-cancel');

    if (!overlay || !titleEl || !msgEl || !confirmBtn || !cancelBtn) {
        console.warn('Confirm modal components not found');
        return;
    }

    titleEl.innerText = title;
    msgEl.innerText = message;
    overlay.classList.add('open');

    // Clean up previous listeners
    const newConfirmBtn = confirmBtn.cloneNode(true);
    const newCancelBtn = cancelBtn.cloneNode(true);
    confirmBtn.parentNode.replaceChild(newConfirmBtn, confirmBtn);
    cancelBtn.parentNode.replaceChild(newCancelBtn, cancelBtn);

    const close = () => overlay.classList.remove('open');

    newConfirmBtn.onclick = () => {
        close();
        if (onConfirm) onConfirm();
    };

    newCancelBtn.onclick = close;
    overlay.onclick = (e) => { if (e.target === overlay) close(); };
}
