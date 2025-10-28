const dbModule = require('../db');

// --- Helper Functions for DB Interaction ---

/**
 * Helper function to get database instance
 * @returns {object} Database instance
 */
const getDbInstance = () => {
    const db = dbModule.db;
    if (!db) {
        throw new Error('Database not initialized');
    }
    return db;
};

/**
 * Helper function to run a single SQL query with parameters.
 * Returns a Promise.
 * @param {string} sql The SQL query string.
 * @param {Array} params Query parameters.
 * @returns {Promise<object>} Promise resolving with { lastID, changes } or rejecting with error.
 */
const runDb = (sql, params = []) => {
    return new Promise((resolve, reject) => {
        const db = getDbInstance();
        db.run(sql, params, function (err) { // Use function() to access this context
            if (err) {
                console.error('Database run error:', err.message, 'SQL:', sql, 'Params:', params);
                reject(err);
            } else {
                resolve({ lastID: this.lastID, changes: this.changes });
            }
        });
    });
};

/**
 * Helper function to get a single row from the database.
 * Returns a Promise.
 * @param {string} sql The SQL query string.
 * @param {Array} params Query parameters.
 * @returns {Promise<object|null>} Promise resolving with the row or null, or rejecting with error.
 */
const getDb = (sql, params = []) => {
    return new Promise((resolve, reject) => {
        const db = getDbInstance();
        db.get(sql, params, (err, row) => {
            if (err) {
                console.error('Database get error:', err.message, 'SQL:', sql, 'Params:', params);
                reject(err);
            } else {
                resolve(row);
            }
        });
    });
};

/**
 * Helper function to get all rows from the database.
 * Returns a Promise.
 * @param {string} sql The SQL query string.
 * @param {Array} params Query parameters.
 * @returns {Promise<Array>} Promise resolving with an array of rows or rejecting with error.
 */
const allDb = (sql, params = []) => {
    return new Promise((resolve, reject) => {
        const db = getDbInstance();
        db.all(sql, params, (err, rows) => {
            if (err) {
                console.error('Database all error:', err.message, 'SQL:', sql, 'Params:', params);
                reject(err);
            } else {
                resolve(rows);
            }
        });
    });
};

module.exports = {
    runDb,
    getDb,
    allDb,
};
