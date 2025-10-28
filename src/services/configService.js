const runDb = require('./dbQueryService.js').runDb;
const getDb = require('./dbQueryService.js').getDb;
const allDb = require('./dbQueryService.js').allDb;

// dbModule은 외부에서 가져와야 하므로, 이 코드가 작동하려면 필요합니다.
const dbModule = require('../db');

// Simple queue for serializing database operations
let dbOperationQueue = Promise.resolve();

/**
 * Executes a series of database operations sequentially.
 * @param {Function} callback A function that performs async operations.
 * @returns {Promise<any>} Returns the result of the callback function.
 */
const serializeDb = (callback) => {
    // Chain the operation to the queue
    dbOperationQueue = dbOperationQueue.then(async () => {
        try {
            return await callback();
        } catch (error) {
            // Log error but don't break the queue
            console.error('Error in serialized database operation:', error);
            throw error;
        }
    });

    return dbOperationQueue;
};

// --- Settings Management (Generic Key-Value) ---

/**
 * Gets a specific setting value from the 'settings' table.
 * @param {string} key The setting key.
 * @param {any} [defaultValue=null] Value to return if key not found.
 * @returns {Promise<any>} The setting value (parsed if JSON) or defaultValue.
 */
async function getSetting(key, defaultValue = null) {
    const row = await getDb('SELECT value FROM settings WHERE key = ?', [key]);
    if (!row) {
        return defaultValue;
    }
    try {
        // Attempt to parse as JSON, fallback to raw value
        return JSON.parse(row.value);
    } catch (e) {
        return row.value; // Return as string if not valid JSON
    }
}

/**
 * Sets a specific setting value in the 'settings' table.
 * Automatically stringifies objects/arrays.
 * @param {string} key The setting key.
 * @param {any} value The value to set.
 * @param {boolean} [skipSync=false] Skip sync to GitHub if true.
 * @param {boolean} [useTransaction=false] Whether this call is part of an existing transaction.
 * @returns {Promise<void>}
 */
async function setSetting(key, value, skipSync = false, useTransaction = false) {
    // Convert value to string for storage
    const valueToStore = (typeof value === 'object' && value !== null)
        ? JSON.stringify(value)
        : String(value); // Ensure it's a string if not object/array
    
    // If not part of an existing transaction, start a new one
    if (!useTransaction) {
        await runDb('BEGIN TRANSACTION');
    }
    
    try {
        // Update or insert the setting
        await runDb('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', [key, valueToStore]);
        
        // If we started a transaction, commit it
        if (!useTransaction) {
            await runDb('COMMIT');
            
            // Sync updates to GitHub (unless skipped)
            if (!skipSync) {
                await dbModule.syncToGitHub();
            }
        }
    } catch (error) {
        // If we started a transaction and an error occurred, roll it back
        if (!useTransaction) {
            await runDb('ROLLBACK');
        }
        // Re-throw the error to be handled by the caller
        throw error;
    }
}


// --- Model Configuration ---

/**
 * Gets the entire models configuration object.
 * @returns {Promise<Record<string, {category: string, dailyQuota?: number, individualQuota?: number}>>}
 */
async function getModelsConfig() {
    const rows = await allDb('SELECT * FROM models_config');
    const config = {};
    rows.forEach(row => {
        config[row.model_id] = {
            category: row.category,
            // Return null or undefined from DB as undefined
            dailyQuota: row.daily_quota ?? undefined,
            individualQuota: row.individual_quota ?? undefined
        };
    });
    return config;
}

/**
 * Adds or updates a model configuration.
 * @param {string} modelId
 * @param {'Pro' | 'Flash' | 'Custom'} category
 * @param {number | null | undefined} dailyQuota Use null/undefined for no limit.
 * @param {number | null | undefined} individualQuota Use null/undefined for no limit.
 * @returns {Promise<void>}
 */
async function setModelConfig(modelId, category, dailyQuota, individualQuota) {
    // Ensure null is stored in DB if quota is undefined or explicitly null
    const dailyQuotaDb = (dailyQuota === undefined || dailyQuota === null) ? null : Number(dailyQuota);
    const individualQuotaDb = (individualQuota === undefined || individualQuota === null) ? null : Number(individualQuota);

    if ((category === 'Custom' && dailyQuotaDb !== null && !Number.isInteger(dailyQuotaDb)) || dailyQuotaDb < 0) {
        throw new Error("Custom model dailyQuota must be a non-negative integer or null.");
    }
    if (((category === 'Pro' || category === 'Flash') && individualQuotaDb !== null && !Number.isInteger(individualQuotaDb)) || individualQuotaDb < 0) {
        throw new Error("Pro/Flash model individualQuota must be a non-negative integer or null.");
    }

    // Use serializeDb to ensure atomic operations and avoid concurrency issues
    await serializeDb(async () => {
        await runDb('BEGIN TRANSACTION');

        try {
            const sql = `
                INSERT OR REPLACE INTO models_config
                (model_id, category, daily_quota, individual_quota)
                VALUES (?, ?, ?, ?)
            `;

            await runDb(sql, [modelId, category, dailyQuotaDb, individualQuotaDb]);

            // Commit the transaction
            await runDb('COMMIT');

            // Sync updates to GitHub (outside transaction)
            await dbModule.syncToGitHub();
        } catch (error) {
            // Rollback on error
            await runDb('ROLLBACK');
            throw error;
        }
    });
}

/**
 * Deletes a model configuration.
 * @param {string} modelId
 * @returns {Promise<void>}
 */
async function deleteModelConfig(modelId) {
    // Use serializeDb to ensure atomic operations and avoid concurrency issues
    await serializeDb(async () => {
        await runDb('BEGIN TRANSACTION');

        try {
            const result = await runDb('DELETE FROM models_config WHERE model_id = ?', [modelId]);

            if (result.changes === 0) {
                await runDb('ROLLBACK');
                throw new Error(`Model '${modelId}' not found for deletion.`);
            }

            await runDb('COMMIT');

            // Sync updates to GitHub (outside transaction)
            await dbModule.syncToGitHub();
        } catch (error) {
            await runDb('ROLLBACK');
            throw error;
        }
    });
}


// --- Category Quotas ---

/**
 * Gets the category quotas (Pro/Flash).
 * @returns {Promise<{proQuota: number, flashQuota: number}>}
 */
async function getCategoryQuotas() {
    // Retrieve from settings table, providing defaults
    const quotas = await getSetting('category_quotas', { proQuota: 50, flashQuota: 1500 });
    // Ensure the retrieved value has the expected format
     return {
        proQuota: typeof quotas?.proQuota === 'number' ? quotas.proQuota : 50,
        flashQuota: typeof quotas?.flashQuota === 'number' ? quotas.flashQuota : 1500,
    };
}

/**
 * Sets the category quotas.
 * @param {number} proQuota
 * @param {number} flashQuota
 * @returns {Promise<void>}
 */
async function setCategoryQuotas(proQuota, flashQuota) {
    if (typeof proQuota !== 'number' || typeof flashQuota !== 'number' || proQuota < 0 || flashQuota < 0) {
        throw new Error("Quotas must be non-negative numbers.");
    }

    // Use serializeDb to ensure atomic operations and avoid concurrency issues
    await serializeDb(async () => {
        await runDb('BEGIN TRANSACTION');

        try {
            // Save directly with SQL to avoid nested transactions
            const quotasObj = {
                proQuota: Math.floor(proQuota),
                flashQuota: Math.floor(flashQuota)
            };

            await runDb('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)',
                ['category_quotas', JSON.stringify(quotasObj)]);

            // Commit the transaction
            await runDb('COMMIT');

            // Sync updates to GitHub outside transaction
            await dbModule.syncToGitHub();
        } catch (error) {
            // Rollback on error
            await runDb('ROLLBACK');
            throw error;
        }
    });
}


// --- Worker Keys ---

/**
 * Gets all worker keys with their descriptions and safety settings.
 * @returns {Promise<Array<{key: string, description: string, safetyEnabled: boolean, createdAt: string}>>}
 */
async function getAllWorkerKeys() {
    const rows = await allDb('SELECT api_key, description, safety_enabled, created_at FROM worker_keys ORDER BY created_at DESC');
    return rows.map(row => ({
        key: row.api_key,
        description: row.description || '',
        safetyEnabled: row.safety_enabled === 1, // Convert DB integer to boolean
        createdAt: row.created_at
    }));
}

/**
 * Gets safety setting for a specific worker key.
 * @param {string} apiKey The worker API key.
 * @returns {Promise<boolean>} True if safety is enabled, false otherwise (defaults to true if key not found, though middleware should prevent this).
 */
async function getWorkerKeySafetySetting(apiKey) {
     const row = await getDb('SELECT safety_enabled FROM worker_keys WHERE api_key = ?', [apiKey]);
     // Default to true if key doesn't exist (shouldn't happen if middleware is used) or if value is null/undefined
     return row ? row.safety_enabled === 1 : true;
}


/**
 * Adds a new worker key.
 * @param {string} apiKey
 * @param {string} [description='']
 * @returns {Promise<void>}
 */
async function addWorkerKey(apiKey, description = '') {
    // Use serializeDb to ensure atomic operations and avoid concurrency issues
    await serializeDb(async () => {
        await runDb('BEGIN TRANSACTION');

        try {
            const sql = `
                INSERT INTO worker_keys (api_key, description, safety_enabled, created_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            `;

            await runDb(sql, [apiKey, description, 1]); // Default safety_enabled to true (1)

            // Commit transaction
            await runDb('COMMIT');

            // Sync updates to GitHub (outside transaction)
            await dbModule.syncToGitHub();
        } catch (err) {
            // Rollback on error
            await runDb('ROLLBACK');

            if (err.code === 'SQLITE_CONSTRAINT') { // Handle potential unique constraint violation
                throw new Error(`Worker key '${apiKey}' already exists.`);
            }
            throw err; // Re-throw other errors
        }
    });
}

/**
 * Updates a worker key's safety setting.
 * @param {string} apiKey
 * @param {boolean} safetyEnabled
 * @returns {Promise<void>}
 */
async function updateWorkerKeySafety(apiKey, safetyEnabled) {
    // Use serializeDb to ensure atomic operations and avoid concurrency issues
    await serializeDb(async () => {
        await runDb('BEGIN TRANSACTION');

        try {
            const sql = `UPDATE worker_keys SET safety_enabled = ? WHERE api_key = ?`;
            const result = await runDb(sql, [safetyEnabled ? 1 : 0, apiKey]);

            if (result.changes === 0) {
                await runDb('ROLLBACK');
                throw new Error(`Worker key '${apiKey}' not found for updating safety settings.`);
            }

            await runDb('COMMIT');

            // Sync updates to GitHub (outside transaction)
            await dbModule.syncToGitHub();
        } catch (error) {
            await runDb('ROLLBACK');
            throw error;
        }
    });
}


/**
 * Deletes a worker key.
 * @param {string} apiKey
 * @returns {Promise<void>}
 */
async function deleteWorkerKey(apiKey) {
    // Use serializeDb to ensure atomic operations and avoid concurrency issues
    await serializeDb(async () => {
        await runDb('BEGIN TRANSACTION');

        try {
            const result = await runDb('DELETE FROM worker_keys WHERE api_key = ?', [apiKey]);

            if (result.changes === 0) {
                await runDb('ROLLBACK');
                throw new Error(`Worker key '${apiKey}' not found for deletion.`);
            }

            await runDb('COMMIT');

            // Sync updates to GitHub (outside transaction)
            await dbModule.syncToGitHub();
        } catch (error) {
            await runDb('ROLLBACK');
            throw error;
        }
    });
}


// --- GitHub Configuration ---

/**
 * Gets the GitHub repository configuration.
 * @returns {Promise<{repo: string, token: string, dbPath: string, encryptKey: string|null}>}
 */
async function getGitHubConfig() {
    return await getSetting('github_config', { repo: '', token: '', dbPath: './database.db', encryptKey: null });
}

/**
 * Sets the GitHub repository configuration.
 * @param {string} repo The GitHub repository in format "username/repo-name"
 * @param {string} token GitHub personal access token
 * @param {string} [dbPath='./database.db'] Path to the database file
 * @param {string|null} [encryptKey=null] Optional encryption key for database file
 * @returns {Promise<void>}
 */
async function setGitHubConfig(repo, token, dbPath = './database.db', encryptKey = null) {
    // Use serializeDb to ensure atomic operations and avoid concurrency issues
    await serializeDb(async () => {
        await runDb('BEGIN TRANSACTION');

        try {
            // Save directly with SQL to avoid nested transactions
            const configObj = { repo, token, dbPath, encryptKey };

            await runDb('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)',
                ['github_config', JSON.stringify(configObj)]);

            // Commit the transaction
            await runDb('COMMIT');

            // Sync updates to GitHub outside transaction
            await dbModule.syncToGitHub();
        } catch (error) {
            // Rollback on error
            await runDb('ROLLBACK');
            throw error;
        }
    });
}


module.exports = {
    // Settings
    getSetting,
    setSetting,
    // GitHub
    getGitHubConfig,
    setGitHubConfig,
    // Models
    getModelsConfig,
    setModelConfig,
    deleteModelConfig,
    // Category Quotas
    getCategoryQuotas,
    setCategoryQuotas,
    // Worker Keys
    getAllWorkerKeys,
    getWorkerKeySafetySetting,
    addWorkerKey,
    updateWorkerKeySafety,
    deleteWorkerKey,
    // DB helpers (optional export if needed elsewhere)
    runDb,
    getDb,
    allDb,
    serializeDb,
};
