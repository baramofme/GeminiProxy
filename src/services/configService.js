// dbService.js

const { runDb, getDb, allDb } = require('./dbQueryService.js');
const dbModule = require('../db');

// --- Async Operation Queue ---
let dbOperationQueue = Promise.resolve();
function serializeDb(callback) {
    dbOperationQueue = dbOperationQueue.then(async () => {
        try { return await callback(); }
        catch (error) {
            console.error('[DB] Serialized operation error:', error);
            throw error;
        }
    });
    return dbOperationQueue;
}

// --- Transaction helpers ---
async function startTx() { await runDb('BEGIN TRANSACTION'); }
async function commitTx() { await runDb('COMMIT'); }
async function rollbackTx() { await runDb('ROLLBACK'); }

// --- General Helpers ---
function toDbString(value) {
    if (value === undefined || value === null) return null;
    if (typeof value === 'object') return JSON.stringify(value);
    return String(value);
}
function parseDbValue(row) {
    if (!row) return null;
    try { return JSON.parse(row.value); }
    catch { return row.value; }
}

// --- Settings management ---
async function getSetting(key, defaultValue = null) {
    const row = await getDb('SELECT value FROM settings WHERE key = ?', [key]);
    return row ? parseDbValue(row) : defaultValue;
}

async function setSetting(key, value, skipSync = false, useTransaction = false) {
    if (!useTransaction) await startTx();
    try {
        await runDb('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', [key, toDbString(value)]);
        if (!useTransaction) {
            await commitTx();
            if (!skipSync) {
                try { await dbModule.syncToGitHub(); }
                catch (e) { console.error('[DB] GitHub sync error (setSetting):', e); }
            }
        }
    } catch (error) {
        if (!useTransaction) await rollbackTx();
        throw error;
    }
}

// --- Models config ---
async function getModelsConfig() {
    const rows = await allDb('SELECT * FROM models_config');
    return Object.fromEntries(rows.map(row => [
        row.model_id, {
            category: row.category,
            dailyQuota: row.daily_quota ?? undefined,
            individualQuota: row.individual_quota ?? undefined
        }
    ]));
}

function validateModelQuotas(category, daily, individual) {
    if (category === 'Custom') {
        if (daily !== null && (!Number.isInteger(daily) || daily < 0)) throw new Error("Custom model dailyQuota must be a non-negative integer or null.");
    } else if (['Pro', 'Flash'].includes(category)) {
        if (individual !== null && (!Number.isInteger(individual) || individual < 0)) throw new Error("Pro/Flash model individualQuota must be a non-negative integer or null.");
    }
}

async function setModelConfig(modelId, category, dailyQuota, individualQuota) {
    const daily = dailyQuota == null ? null : Number(dailyQuota);
    const indiv = individualQuota == null ? null : Number(individualQuota);
    validateModelQuotas(category, daily, indiv);
    await serializeDb(async () => {
        await startTx();
        try {
            await runDb(
                `INSERT OR REPLACE INTO models_config (model_id, category, daily_quota, individual_quota)
         VALUES (?, ?, ?, ?)`,
                [modelId, category, daily, indiv]
            );
            await commitTx();
            try { await dbModule.syncToGitHub(); }
            catch (e) { console.error('[DB] GitHub sync error (setModelConfig):', e); }
        } catch (error) { await rollbackTx(); throw error; }
    });
}

async function deleteModelConfig(modelId) {
    await serializeDb(async () => {
        await startTx();
        try {
            const result = await runDb('DELETE FROM models_config WHERE model_id = ?', [modelId]);
            if (result.changes === 0) { await rollbackTx(); throw new Error(`Model '${modelId}' not found.`); }
            await commitTx();
            try { await dbModule.syncToGitHub(); }
            catch (e) { console.error('[DB] GitHub sync error (deleteModelConfig):', e); }
        } catch (error) { await rollbackTx(); throw error; }
    });
}

// --- Category Quotas ---
const defaultQuotas = { proQuota: 50, flashQuota: 1500 };

async function getCategoryQuotas() {
    const quotas = await getSetting('category_quotas', defaultQuotas);
    return {
        proQuota: typeof quotas?.proQuota === 'number' ? quotas.proQuota : defaultQuotas.proQuota,
        flashQuota: typeof quotas?.flashQuota === 'number' ? quotas.flashQuota : defaultQuotas.flashQuota
    };
}
async function setCategoryQuotas(proQuota, flashQuota) {
    if (![proQuota, flashQuota].every((n) => typeof n === 'number' && n >= 0)) throw new Error("Quotas must be non-negative numbers.");
    await serializeDb(async () => {
        await startTx();
        try {
            await runDb('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', [
                'category_quotas', JSON.stringify({ proQuota: Math.floor(proQuota), flashQuota: Math.floor(flashQuota) })
            ]);
            await commitTx();
            try { await dbModule.syncToGitHub(); }
            catch (e) { console.error('[DB] GitHub sync error (setCategoryQuotas):', e); }
        } catch (error) { await rollbackTx(); throw error; }
    });
}

// --- Worker Key management ---
async function getAllWorkerKeys() {
    const rows = await allDb('SELECT api_key, description, safety_enabled, created_at FROM worker_keys ORDER BY created_at DESC');
    return rows.map(row => ({
        key: row.api_key,
        description: row.description || '',
        safetyEnabled: !!row.safety_enabled,
        createdAt: row.created_at
    }));
}

async function getWorkerKeySafetySetting(apiKey) {
    const row = await getDb('SELECT safety_enabled FROM worker_keys WHERE api_key = ?', [apiKey]);
    return row ? !!row.safety_enabled : true;
}

async function addWorkerKey(apiKey, description = '') {
    await serializeDb(async () => {
        await startTx();
        try {
            await runDb(
                `INSERT INTO worker_keys (api_key, description, safety_enabled, created_at)
         VALUES (?, ?, ?, CURRENT_TIMESTAMP)`,
                [apiKey, description, 1]
            );
            await commitTx();
            try { await dbModule.syncToGitHub(); }
            catch (e) { console.error('[DB] GitHub sync error (addWorkerKey):', e); }
        } catch (err) {
            await rollbackTx();
            if (err.code === 'SQLITE_CONSTRAINT') throw new Error(`Worker key '${apiKey}' already exists.`);
            throw err;
        }
    });
}

async function updateWorkerKeySafety(apiKey, safetyEnabled) {
    await serializeDb(async () => {
        await startTx();
        try {
            const result = await runDb('UPDATE worker_keys SET safety_enabled = ? WHERE api_key = ?', [safetyEnabled ? 1 : 0, apiKey]);
            if (result.changes === 0) { await rollbackTx(); throw new Error(`Worker key '${apiKey}' not found.`); }
            await commitTx();
            try { await dbModule.syncToGitHub(); }
            catch (e) { console.error('[DB] GitHub sync error (updateWorkerKeySafety):', e); }
        } catch (error) { await rollbackTx(); throw error; }
    });
}

async function deleteWorkerKey(apiKey) {
    await serializeDb(async () => {
        await startTx();
        try {
            const result = await runDb('DELETE FROM worker_keys WHERE api_key = ?', [apiKey]);
            if (result.changes === 0) { await rollbackTx(); throw new Error(`Worker key '${apiKey}' not found.`); }
            await commitTx();
            try { await dbModule.syncToGitHub(); }
            catch (e) { console.error('[DB] GitHub sync error (deleteWorkerKey):', e); }
        } catch (error) { await rollbackTx(); throw error; }
    });
}

// --- GitHub configuration ---
async function getGitHubConfig() {
    return await getSetting('github_config', { repo: '', token: '', dbPath: './database.db', encryptKey: null });
}
async function setGitHubConfig(repo, token, dbPath = './database.db', encryptKey = null) {
    await serializeDb(async () => {
        await startTx();
        try {
            await runDb('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', [
                'github_config', JSON.stringify({ repo, token, dbPath, encryptKey })
            ]);
            await commitTx();
            try { await dbModule.syncToGitHub(); }
            catch (e) { console.error('[DB] GitHub sync error (setGitHubConfig):', e); }
        } catch (error) { await rollbackTx(); throw error; }
    });
}

// --- Exports ---
module.exports = {
    // Settings
    getSetting, setSetting,
    // GitHub
    getGitHubConfig, setGitHubConfig,
    // Models
    getModelsConfig, setModelConfig, deleteModelConfig,
    // Category Quotas
    getCategoryQuotas, setCategoryQuotas,
    // Worker Keys
    getAllWorkerKeys, getWorkerKeySafetySetting, addWorkerKey, updateWorkerKeySafety, deleteWorkerKey,
    // DB helpers
    runDb, getDb, allDb, serializeDb,
};
