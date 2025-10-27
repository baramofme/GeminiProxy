const fetch = require('node-fetch');
const { Readable, Transform } = require('stream'); // Import Transform
const fs = require('fs').promises; // Async fs for temp file operations
const os = require('os');
const path = require('path');
const { v4: uuidv4 } = require('uuid');
const { GoogleGenAI } = require('@google/genai');
const configService = require('./configService');
const transformUtils = require('../utils/transform');

// List of Vertex AI supported models (prefix [v] indicates it's a Vertex API model)
const VERTEX_SUPPORTED_MODELS = [
    "[v]gemini-2.5-flash",
    "[v]gemini-2.5-pro"
];

// Default region
const DEFAULT_REGION = 'us-central1';

// Temporary credentials file path
let tempCredentialsPath = null;

// --- Database-only Configuration ---
let VERTEX_JSON_STRING = null; // Store database loaded value

// --- Initialize Credentials on Load ---
let isVertexInitialized = false;
let isUsingExpressMode = false; // Track if we're using Express Mode

/**
 * Initializes Vertex credentials (loads JSON, creates temp file, sets env var) on service start.
 */
async function initializeVertexCredentials() {
    if (isVertexInitialized) return; // Already initialized

    // First try to load configuration from database (priority)
    let databaseConfig = null;
    let databaseError = false;
    try {
        databaseConfig = await configService.getSetting('vertex_config', null);
    } catch (error) {
        console.warn("Failed to load Vertex config from database:", error);
        databaseError = true;
    }

    // If database is not available, disable Vertex AI
    if (databaseError) {
        console.info("Database not available for Vertex configuration, Vertex AI disabled");
        isVertexInitialized = true; // Mark as initialized (but disabled)
        return;
    }

    // Check database configuration first
    if (databaseConfig && (databaseConfig.expressApiKey || databaseConfig.vertexJson)) {
        console.info("Using Vertex AI configuration from database");

        if (databaseConfig.expressApiKey) {
            // Use Express Mode from database
            console.info("Using Vertex AI Express Mode with API key from database");
            isUsingExpressMode = true;
            isVertexInitialized = true;
            return;
        } else if (databaseConfig.vertexJson) {
            // Use Service Account from database
            try {
                JSON.parse(databaseConfig.vertexJson); // Validate JSON
                VERTEX_JSON_STRING = databaseConfig.vertexJson;
                console.info("Using VERTEX credentials from database");

                const createdPath = await createServiceAccountFile(VERTEX_JSON_STRING);
                if (!createdPath) {
                    throw new Error("createServiceAccountFile returned null or undefined.");
                }
                tempCredentialsPath = createdPath;
                process.env.GOOGLE_APPLICATION_CREDENTIALS = tempCredentialsPath;
                console.log(`Vertex AI credentials created and set from database: GOOGLE_APPLICATION_CREDENTIALS=${tempCredentialsPath}`);
                isVertexInitialized = true;
                return;
            } catch (error) {
                console.error("Failed to initialize Vertex AI credentials from database:", error);
                console.info("Database Vertex configuration is invalid, Vertex AI disabled");
                isVertexInitialized = true; // Mark as initialized (but disabled)
                return;
            }
        }
    } else if (databaseConfig === null) {
        // Database configuration is explicitly null (not found)
        // According to user requirements: when database has no vertex config, should not enable vertex
        console.info("No Vertex AI configuration found in database, Vertex AI disabled");
        isVertexInitialized = true; // Mark as initialized (but disabled)
        return;
    } else {
        // Database configuration exists but is invalid
        console.warn("Invalid Vertex AI configuration in database, Vertex AI disabled");
        isVertexInitialized = true; // Mark as initialized (but disabled)
        return;
    }

    // This should not be reached - all cases should return above
    console.error("Unexpected code path in Vertex AI initialization");
    isVertexInitialized = true; // Mark as initialized (but disabled)
    return;
}

// Don't initialize immediately when module loads - will be initialized after DB is ready
// initializeVertexCredentials();


/**
 * Creates a temporary service account file for Vertex AI authentication.
 * @param {string} vertexJsonString - JSON string containing the service account credentials.
 * @returns {Promise<string|null>} The path to the temporary file, or null on failure.
 */
async function createServiceAccountFile(vertexJsonString) {
    try {
        const serviceAccountInfo = JSON.parse(vertexJsonString);

        // Basic validation
        const requiredKeys = ["type", "project_id", "private_key_id", "private_key", "client_email", "client_id"];
        if (!requiredKeys.every(key => key in serviceAccountInfo)) {
            console.error("Invalid JSON format for Vertex database configuration. Missing required keys.");
            return null;
        }
        if (serviceAccountInfo.type !== "service_account") {
            console.error("Invalid JSON format for Vertex database configuration. 'type' must be 'service_account'.");
            return null;
        }

        const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'vertexai-nodejs-'));
        const tempFilePath = path.join(tempDir, 'service-account.json');
        await fs.writeFile(tempFilePath, JSON.stringify(serviceAccountInfo, null, 2), 'utf-8');
        // console.info(`Successfully parsed 'VERTEX' JSON and created temporary credentials file: ${tempFilePath}`); // Removed log
        return tempFilePath;
    } catch (e) {
        console.error(`Failed to create service account file from Vertex database configuration JSON: ${e}`, e);
        return null;
    }
}

/**
 * Maps OpenAI roles to Vertex AI roles.
 * @param {string} openaiRole - The role from the OpenAI request ('system', 'user', 'assistant', 'tool').
 * @returns {string} The corresponding Vertex AI role ('user', 'model', 'function').
 */
function mapOpenaiRoleToVertex(openaiRole) {
    const roleMap = {
        system: 'user', // Treat system messages as user messages for compatibility
        user: 'user',
        assistant: 'model',
        tool: 'function' // Tool results map to 'function' role in Vertex
    };
    return roleMap[openaiRole.toLowerCase()] || 'user'; // Default to user
}

/**
 * Parses a data URI (e.g., for base64 encoded images).
 * @param {string} uri - The data URI string.
 * @returns {{mimeType: string, data: Buffer}|null} Parsed mime type and data buffer, or null if invalid.
 */
function parseImageDataUri(uri) {
    if (!uri || !uri.startsWith('data:')) {
        return null;
    }
    try {
        const commaIndex = uri.indexOf(',');
        if (commaIndex === -1) return null;

        const header = uri.substring(5, commaIndex); // Remove 'data:' prefix
        const encodedData = uri.substring(commaIndex + 1);
        const parts = header.split(';');
        const mimeType = parts[0];

        if (parts.includes('base64')) {
            const data = Buffer.from(encodedData, 'base64');
            return { mimeType, data };
        } else {
            // Handle other encodings (e.g., URL encoding)
            console.warn(`Unsupported data URI encoding (non-base64): ${parts.slice(1).join(';')}`); // Keep warn log in English
            return { mimeType, data: Buffer.from(decodeURIComponent(encodedData)) }; // Attempt URL decoding
        }
    } catch (e) {
        console.error(`Error parsing data URI: ${e}`, e); // Keep error log in English
        return null;
    }
}

/**
 * Asynchronously converts OpenAI message content parts to Vertex AI Parts, handling text and images.
 * Downloads images from HTTPS URLs if necessary.
 * @param {Array<object>} openAIContentParts - Array of OpenAI content parts (text or image_url).
 * @returns {Promise<Array<object>>} A promise resolving to an array of Vertex AI Part objects.
 */
async function convertOpenaiPartsToVertexParts(openAIContentParts) {
    const vertexParts = [];
    for (const part of openAIContentParts) {
        if (part.type === 'text') {
            vertexParts.push({ text: part.text });
        } else if (part.type === 'image_url' && part.image_url) {
            const imageUrl = part.image_url.url;
            if (imageUrl.startsWith('data:')) {
                const parsed = parseImageDataUri(imageUrl);
                if (parsed) {
                    vertexParts.push({
                        inlineData: {
                            mimeType: parsed.mimeType,
                            data: parsed.data.toString('base64') // Vertex SDK expects base64 string
                        }
                    });
                } else {
                    console.warn(`Could not parse data URI: ${imageUrl.substring(0, 50)}...`); // Keep warn log in English
                    vertexParts.push({ text: `[Failed to parse image data URI]` });
                }
            } else if (imageUrl.startsWith('gs://')) {
                // Handle Google Cloud Storage URIs
                const mime = require('mime-types'); // Lazy require mime-types
                vertexParts.push({
                    fileData: {
                        mimeType: mime.lookup(imageUrl) || 'application/octet-stream',
                        fileUri: imageUrl
                    }
                });
            } else if (imageUrl.startsWith('http://') || imageUrl.startsWith('https://')) {
                // Attempt to download image from URL
                try {
                    const response = await fetch(imageUrl, { timeout: 10000 }); // 10s timeout
                    if (!response.ok) {
                        throw new Error(`HTTP error! status: ${response.status}`);
                    }
                    const imageBuffer = await response.buffer();
                    const contentType = response.headers.get('content-type') || 'application/octet-stream';
                    vertexParts.push({
                        inlineData: {
                            mimeType: contentType,
                            data: imageBuffer.toString('base64')
                        }
                    });
                } catch (e) {
                    console.error(`Failed to download image from ${imageUrl}: ${e}`); // Keep error log in English
                    vertexParts.push({ text: `[Failed to load image at ${imageUrl}]` });
                }
            } else {
                console.warn(`Unsupported image URL format: ${imageUrl}`); // Keep warn log in English
                vertexParts.push({ text: `[Unsupported image format at ${imageUrl}]` });
            }
        }
    }
    return vertexParts;
}

/**
 * Converts OpenAI message list to Vertex AI Content array.
 * @param {Array<object>} messages - Array of OpenAI message objects.
 * @returns {Promise<Array<object>>} A promise resolving to an array of Vertex AI Content objects.
 */
async function convertOpenaiMessagesToVertex(messages) {
    const vertexContents = [];
    // Map tool_call_id -> function name from prior assistant tool_calls for resolving subsequent tool messages
    const toolCallIdToName = new Map();
    
    // Process all messages, including system messages, mapping to appropriate Vertex roles
    for (const msg of messages) {
        const vertexRole = mapOpenaiRoleToVertex(msg.role);
        let parts = [];

        if (vertexRole === 'function') { // Handle tool/function results
            if (msg.tool_call_id && msg.content) {
                // Resolve function name: explicit msg.name or by prior assistant tool_call mapping
                const resolvedName = msg.name || toolCallIdToName.get(msg.tool_call_id);
                if (resolvedName) {
                    let responseContent = {};
                    try {
                        // Attempt to parse the string content into an object
                        responseContent = typeof msg.content === 'string' ? JSON.parse(msg.content) : msg.content;
                    } catch (e) {
                        console.warn(`Tool result content for ${resolvedName} (${msg.tool_call_id}) is not valid JSON, sending as string: ${msg.content}`); // Keep warn log in English
                        // Send as simple text if not parsable JSON
                        parts.push({ text: `[Tool Result for ${resolvedName}: ${msg.content}]` });
                        continue; // Skip adding as functionResponse if invalid
                    }
                    // Ensure Vertex functionResponse.response is always an object (Struct)
                    if (responseContent === null || Array.isArray(responseContent) || typeof responseContent !== 'object') {
                        responseContent = { content: responseContent };
                    }
                    parts.push({
                        functionResponse: {
                            name: resolvedName,
                            response: responseContent // Vertex SDK expects the actual object
                        }
                    });
                } else {
                    console.warn(`Tool message received without function name and unresolved tool_call_id mapping: ${JSON.stringify(msg)}`); // Keep warn log in English
                    parts.push({ text: `[Tool Result for ${msg.tool_call_id}: ${typeof msg.content === 'string' ? msg.content : JSON.stringify(msg.content)}]` });
                }
            } else {
                console.warn(`Tool message missing tool_call_id or content: ${JSON.stringify(msg)}`); // Keep warn log in English
                parts.push({ text: msg.content || '[Empty Tool Message]' });
            }
        } else if (vertexRole === 'model') { // Handle assistant messages (including potential tool calls)
            if (msg.tool_calls && msg.tool_calls.length > 0) {
                // If assistant message contains tool calls, represent them as FunctionCallParts
                for (const toolCall of msg.tool_calls) {
                    if (toolCall.type === 'function' && toolCall.function) {
                        let args = {};
                        try {
                            // Arguments from OpenAI are a JSON string, Vertex expects an object
                            args = JSON.parse(toolCall.function.arguments || '{}');
                        } catch (e) {
                            console.error(`Failed to parse tool call arguments for ${toolCall.function.name}: ${e}`); // Keep error log in English
                            args = { _error: "Failed to parse arguments", raw_arguments: toolCall.function.arguments };
                        }
                        // Track id->name mapping for subsequent tool message
                        if (toolCall.id && toolCall.function?.name) {
                            toolCallIdToName.set(toolCall.id, toolCall.function.name);
                        }
                        parts.push({
                            functionCall: {
                                name: toolCall.function.name,
                                args: args // Pass the parsed object
                            }
                        });
                    }
                }
                // If there's also text content along with tool calls, add it as a separate text part
                if (msg.content && typeof msg.content === 'string') {
                    parts.push({ text: msg.content });
                } else if (Array.isArray(msg.content)) {
                    // Handle multi-part assistant messages (rare but possible)
                    const textParts = msg.content.filter(p => p.type === 'text').map(p => p.text).join('\n');
                    if (textParts) {
                        parts.push({ text: textParts });
                    }
                    // Note: Image parts from assistant messages are generally not expected/handled here.
                }
            } else {
                // Normal assistant message (text or potentially multimodal)
                if (typeof msg.content === 'string') {
                    parts.push({ text: msg.content });
                } else if (Array.isArray(msg.content)) {
                    parts = parts.concat(await convertOpenaiPartsToVertexParts(msg.content));
                }
            }
        } else { // Handle 'user' messages (can be text or multimodal)
            if (typeof msg.content === 'string') {
                parts.push({ text: msg.content });
            } else if (Array.isArray(msg.content)) {
                parts = parts.concat(await convertOpenaiPartsToVertexParts(msg.content));
            }
        }

        if (parts.length > 0) {
            // Ensure role mapping is correct before pushing
            const finalVertexRole = mapOpenaiRoleToVertex(msg.role);
            vertexContents.push({ role: finalVertexRole, parts });
        } else {
            console.warn(`Message resulted in empty parts, skipping: ${JSON.stringify(msg)}`); // Keep warn log in English
        }
    }

    return vertexContents;
}

/**
 * Converts OpenAI tool definitions to Vertex AI Tool format.
 * @param {Array<object>|null} tools - Array of OpenAI tool objects.
 * @returns {Array<object>|null} Array of Vertex AI Tool objects or null.
 */
function convertOpenaiToolsToVertex(tools) {
    if (!tools || tools.length === 0) {
        return null;
    }

    const functionDeclarations = [];
    for (const tool of tools) {
        if (tool.type === 'function' && tool.function) {
            const func = tool.function;
            // Sanitize parameters schema for Vertex compatibility
            let params = func.parameters ? JSON.parse(JSON.stringify(func.parameters)) : { type: 'object', properties: {} };
            try {
                if (params) {
                    params = transformUtils.sanitizeToolParameters(params);
                }
            } catch (e) {
                console.warn(`Failed to sanitize tool parameters for ${func.name}: ${e?.message || e}`);
            }
            // Defensive: ensure root parameters has explicit type when object-like
            if (params && !params.type && (params.properties || params.required || ('additionalProperties' in params))) {
                params.type = 'object';
            }
            functionDeclarations.push({
                name: func.name,
                description: func.description || '',
                parameters: params
            });
        } else {
            console.warn(`Unsupported tool type encountered: ${tool.type}`); // Keep warn log in English
        }
    }

    if (functionDeclarations.length > 0) {
        // Vertex SDK expects a Tool object containing the declarations
        return [{ functionDeclarations }];
    }

    return null;
}

/**
 * Maps Vertex AI finish reasons to OpenAI finish reasons.
 * @param {string|null} reason - Vertex AI finish reason string.
 * @returns {string|null} OpenAI finish reason string or null.
 */
function convertVertexFinishReasonToOpenai(reason) {
    if (!reason) return null;
    const mapping = {
        'STOP': 'stop',
        'MAX_TOKENS': 'length',
        'SAFETY': 'content_filter',
        'RECITATION': 'content_filter', // Often related to safety/policy
        'TOOL_CALL': 'tool_calls',
        'FUNCTION_CALL': 'tool_calls', // Older naming
        'FINISH_REASON_UNSPECIFIED': null,
        'OTHER': null
    };
    return mapping[reason.toUpperCase()] || null; // Default to null if unknown
}

/**
 * Converts a Vertex FunctionCallPart or FunctionCall object into an OpenAI tool_calls object.
 * @param {object} functionCall - The Vertex functionCall object.
 * @param {number} [index=0] - Optional index for multiple tool calls.
 * @returns {object|null} OpenAI tool_calls object structure, or null if input is invalid.
 */
function convertVertexToolCallToOpenai(functionCall, index = 0) {
    if (!functionCall || !functionCall.name) {
        console.error("Invalid functionCall object received from Vertex", functionCall); // Keep error log in English
        return null;
    }
    return {
        id: `call_${uuidv4()}`, // Generate a unique ID for the call
        type: 'function',
        function: {
            name: functionCall.name,
            // OpenAI expects arguments as a JSON string
            arguments: JSON.stringify(functionCall.args || {})
        },
        index: index
    };
}

/**
 * Creates Vertex AI safety settings.
 * @param {string} [blockLevel='OFF'] - The threshold level.
 * @returns {Array<object>} Array of Vertex safety setting objects.
 */
function createSafetySettings(blockLevel = 'OFF') {
    const HarmCategory = {
        HARM_CATEGORY_UNSPECIFIED: "HARM_CATEGORY_UNSPECIFIED",
        HARM_CATEGORY_HATE_SPEECH: "HARM_CATEGORY_HATE_SPEECH",
        HARM_CATEGORY_DANGEROUS_CONTENT: "HARM_CATEGORY_DANGEROUS_CONTENT",
        HARM_CATEGORY_HARASSMENT: "HARM_CATEGORY_HARASSMENT",
        HARM_CATEGORY_SEXUALLY_EXPLICIT: "HARM_CATEGORY_SEXUALLY_EXPLICIT"
    };

    const categories = [
        HarmCategory.HARM_CATEGORY_HATE_SPEECH,
        HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
        HarmCategory.HARM_CATEGORY_HARASSMENT,
        HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT
    ];
    return categories.map(category => ({
        category: category,
        threshold: blockLevel // Use the string representation
    }));
}

/**
 * Handles chat completion requests for the Vertex API.
 */
async function proxyVertexChatCompletions(openAIRequestBody, workerApiKey, stream, keepAliveCallback = null) {
    console.log("Using Vertex AI proxy service"); // Keep log in English

    // Whether to use KEEPALIVE in streaming mode - get from database only
    const keepAliveEnabled = String(await configService.getSetting('keepalive', '0')) === '1';
    const requestedModelId = openAIRequestBody?.model;

    // Validate request
    if (!requestedModelId) {
        return { error: { message: "Missing 'model' field in request body" }, status: 400 };
    }
    if (!openAIRequestBody.messages || !Array.isArray(openAIRequestBody.messages)) {
        return { error: { message: "Missing or invalid 'messages' field in request body" }, status: 400 };
    }

    // Remove [v] prefix from model name to get the actual Vertex model ID
    let vertexModelId = requestedModelId;
    if (vertexModelId.startsWith('[v]')) {
        vertexModelId = vertexModelId.substring(3);
    }

    // Check safety setting
    let isSafetyEnabled;
    try {
        isSafetyEnabled = await configService.getWorkerKeySafetySetting(workerApiKey);
    } catch (error) {
        console.error("Error getting worker key safety setting:", error); // Keep error log in English
        isSafetyEnabled = true; // Default to enabled safety settings
    }

    // Check initialization status
    if (!isVertexInitialized) {
        return {
            error: {
                message: "Vertex AI service not initialized."
            },
            status: 500
        };
    }

    let ai;
    
    try {
        // Initialize client based on authentication mode
        if (isUsingExpressMode) {
            // Express Mode with API Key - get from database only
            let expressApiKey = null;

            try {
                const databaseConfig = await configService.getSetting('vertex_config', null);
                if (databaseConfig && databaseConfig.expressApiKey) {
                    expressApiKey = databaseConfig.expressApiKey;
                    console.log("Using Express API Key from database");
                } else {
                    throw new Error("Express API Key not found in database configuration");
                }
            } catch (error) {
                console.error("Failed to load Express API Key from database:", error);
                throw new Error("EXPRESS_API_KEY is not available in database configuration.");
            }

            if (!expressApiKey) {
                throw new Error("EXPRESS_API_KEY is not available in database configuration.");
            }

            ai = new GoogleGenAI({
                vertexai: true,
                apiKey: expressApiKey
            });

            console.log("Vertex AI Client initialized with Express Mode API Key"); // Keep log in English
        } else {
            // Standard mode with service account
            if (!tempCredentialsPath) {
                return {
                    error: {
                        message: "Temporary credentials path not set for service account authentication."
                    },
                    status: 500
                };
            }
            
            // Read service account file to get project_id
            const keyFileContent = await fs.readFile(tempCredentialsPath, 'utf-8');
            const keyFileData = JSON.parse(keyFileContent);
            const project_id = keyFileData.project_id;
            
            if (!project_id) {
                throw new Error("No project_id found in service account JSON");
            }

            // Initialize GoogleGenAI client with Vertex AI service account configuration
            let region = DEFAULT_REGION;
            ai = new GoogleGenAI({
                vertexai: true,
                project: project_id,
                location: region
            });
            
            console.log(`Vertex AI Client initialized for project '${project_id}' in region '${region}'.`); // Keep log in English
        }

        // Convert OpenAI format to Vertex format
        const vertexContents = await convertOpenaiMessagesToVertex(openAIRequestBody.messages);
        const vertexTools = convertOpenaiToolsToVertex(openAIRequestBody.tools);
        
        // Set safety level
        const safetySettings = createSafetySettings(isSafetyEnabled ? 'BLOCK_MEDIUM_AND_ABOVE' : 'OFF');

        // Configure generation parameters
        const generationConfig = {
            maxOutputTokens: openAIRequestBody.max_tokens,
            temperature: openAIRequestBody.temperature,
            topP: openAIRequestBody.top_p,
            topK: openAIRequestBody.top_k,
            stopSequences: typeof openAIRequestBody.stop === 'string' ? [openAIRequestBody.stop] : openAIRequestBody.stop
        };
        
        // Remove undefined keys
        Object.keys(generationConfig).forEach(key => 
            generationConfig[key] === undefined && delete generationConfig[key]
        );

        // Tool configuration
        let toolConfig = null;
        if (vertexTools) {
            let mode = 'AUTO'; // Default
            let allowedFunctionNames = [];

            if (openAIRequestBody.tool_choice) {
                if (typeof openAIRequestBody.tool_choice === 'string') {
                    if (openAIRequestBody.tool_choice === 'none') {
                        mode = 'NONE';
                    } else if (openAIRequestBody.tool_choice === 'auto') {
                        mode = 'AUTO';
                    } else {
                        mode = 'ANY'; // Assume non-standard string is a function name
                        allowedFunctionNames.push(openAIRequestBody.tool_choice);
                    }
                } else if (typeof openAIRequestBody.tool_choice === 'object' && openAIRequestBody.tool_choice.type === 'function') {
                    const funcName = openAIRequestBody.tool_choice.function?.name;
                    if (funcName) {
                        mode = 'ANY'; // ANY requires specifying function name(s)
                        allowedFunctionNames.push(funcName);
                    }
                }
            }
            toolConfig = {
                functionCallingConfig: {
                    mode: mode,
                    allowedFunctionNames: allowedFunctionNames.length > 0 ? allowedFunctionNames : undefined
                }
            };
        }

        // Build the request payload with all parameters
        const requestPayload = {
            model: vertexModelId,
            contents: vertexContents,
            generationConfig: generationConfig,
            safetySettings: safetySettings,
            tools: vertexTools,
            toolConfig: toolConfig
        };
        // Remove keys with null or undefined values from the payload
        Object.keys(requestPayload).forEach(key => (requestPayload[key] == null) && delete requestPayload[key]);


        // Determine if KEEPALIVE mode should be used:
        // 1. KEEPALIVE environment variable is set to 1
        // 2. Client requested streaming
        // 3. Safety settings are disabled
        const useKeepAlive = keepAliveEnabled && stream && !isSafetyEnabled;
        
        // Determine the actual stream mode based on whether KEEPALIVE is used
        const actualStreamMode = useKeepAlive ? false : stream;
        
        // Log KEEPALIVE mode
        if (useKeepAlive) {
            console.log(`Using KEEPALIVE mode: Client requested streaming but sending non-streaming request to Vertex (safety settings disabled)`); // Keep log in English
        }

        // Handle response
        if (stream) {
            if (useKeepAlive) {
                // KEEPALIVE mode: Asynchronous handling with internal retry logic
                if (keepAliveCallback) {
                    const keepAliveRunner = async () => {
                        console.log('KEEPALIVE (Vertex): Starting heartbeat and async request process.');
                        keepAliveCallback.startHeartbeat();

                        let lastKeepAliveError = null;
                        const MAX_RETRIES = parseInt(await configService.getSetting('max_retry', '1')) || 1;

                        for (let kAttempt = 1; kAttempt <= MAX_RETRIES; kAttempt++) {
                            try {
                                console.log(`KEEPALIVE (Vertex) Attempt ${kAttempt}: Sending request.`);
                                const response = await ai.models.generateContent(requestPayload);

                                // Check for valid response
                                if (!response || !response.candidates || response.candidates.length === 0) {
                                     const promptFeedback = response?.promptFeedback;
                                     if (promptFeedback?.blockReason) {
                                         const blockMessage = promptFeedback.blockReasonMessage || `Blocked due to ${promptFeedback.blockReason}`;
                                         throw new Error(JSON.stringify({
                                             error: {
                                                 message: `Request blocked by Vertex AI safety filters: ${blockMessage}`,
                                                 type: "vertex_ai_safety_filter",
                                                 code: "content_filter"
                                             }
                                         }));
                                     }
                                     throw new Error("No valid candidates received from Vertex AI.");
                                }

                                // Success case
                                console.log(`KEEPALIVE (Vertex): Request successful on attempt ${kAttempt}. Stopping heartbeat.`);
                                keepAliveCallback.stopHeartbeat();
                                keepAliveCallback.sendFinalResponse(response);
                                return; // Exit runner on success

                            } catch (error) {
                                console.error(`KEEPALIVE (Vertex) Attempt ${kAttempt} failed:`, error.message);
                                try {
                                   // Try to parse the error message as it might be a JSON string
                                   lastKeepAliveError = JSON.parse(error.message).error || { message: error.message };
                                } catch (e) {
                                   // If parsing fails, use the raw message
                                   lastKeepAliveError = { message: error.message };
                                }
                                
                                if (kAttempt < MAX_RETRIES) {
                                    console.warn(`KEEPALIVE (Vertex): Retrying...`);
                                }
                            }
                        }

                        // If loop finishes, all retries have failed
                        console.error(`KEEPALIVE (Vertex): All ${MAX_RETRIES} attempts failed. Sending last error.`);
                        keepAliveCallback.stopHeartbeat();
                        keepAliveCallback.sendError(lastKeepAliveError || { message: "All Vertex keepalive attempts failed." });
                    };

                    keepAliveRunner(); // Run the async function

                    // Return immediately to the client
                    return {
                        isKeepAlive: true,
                        selectedKeyId: 'vertex-ai',
                        modelCategory: 'Vertex',
                        requestedModelId: requestedModelId
                    };
                } else {
                     console.error('KEEPALIVE: No callback available for Vertex KEEPALIVE mode');
                     return {
                         error: {
                             message: 'KEEPALIVE callback not available for Vertex',
                             type: 'vertex_keepalive_setup_error'
                         },
                         status: 500
                     };
                }
            } else {
                // Standard streaming mode
                try {
                    // Use the new API for streaming
                    const streamResult = await ai.models.generateContentStream(requestPayload);
                    
                    let toolCallIndex = 0; // Keep track across chunks

                    // Create a Transform stream to process the stream from Vertex SDK
                    const vertexTransformer = new Transform({
                        objectMode: true, // Process objects from Vertex SDK
                        async transform(item, encoding, callback) {
                            try {
                                if (!item || !item.candidates || item.candidates.length === 0) {
                                    return callback(); // Skip empty items
                                }

                                const candidate = item.candidates[0];
                                const finishReasonVertex = candidate?.finishReason;
                                const finishReasonOpenai = convertVertexFinishReasonToOpenai(finishReasonVertex);

                                let deltaContent = null;
                                let deltaToolCalls = [];

                                if (candidate.content && candidate.content.parts) {
                                    for (const part of candidate.content.parts) {
                                        if (part.text) {
                                            deltaContent = part.text;
                                        } else if (part.functionCall) {
                                            const openaiToolCall = convertVertexToolCallToOpenai(part.functionCall, toolCallIndex++);
                                            if (openaiToolCall) {
                                                deltaToolCalls.push(openaiToolCall);
                                            }
                                        }
                                    }
                                }

                                // Create chunk only if there's content, tool calls, or a finish reason
                                if (deltaContent !== null || deltaToolCalls.length > 0 || finishReasonOpenai) {
                                    const choiceDelta = {
                                        role: 'assistant',
                                        content: deltaContent,
                                        tool_calls: deltaToolCalls.length > 0 ? deltaToolCalls : undefined
                                    };
                                    const streamChoice = {
                                        index: 0,
                                        delta: choiceDelta,
                                        finish_reason: finishReasonOpenai,
                                        logprobs: null
                                    };
                                    const responseChunk = {
                                        id: `chatcmpl-stream-${uuidv4()}`,
                                        object: 'chat.completion.chunk',
                                        created: Math.floor(Date.now() / 1000),
                                        model: requestedModelId,
                                        choices: [streamChoice],
                                        usage: null
                                    };
                                    // Push the transformed JSON string downstream
                                    this.push(JSON.stringify(responseChunk));
                                }
                                
                                // Prepare to end if there's a finish reason
                                // Note: No need to explicitly end the stream here, let the source stream end naturally
                                callback();
                            } catch (err) {
                                callback(err); // Propagate errors
                            }
                        },

                        flush(callback) {
                            // Getting final aggregated usage data is difficult here as we are a transform stream
                            // Ignore sending aggregated data for now
                            
                            // Send the [DONE] message
                            this.push(JSON.stringify({ done: true }));
                            callback();
                        }
                    });

                    // Pipe the Vertex SDK stream through our transformer
                    // The streamResult might be structured differently based on the API mode
                    // In standard mode: streamResult.stream is AsyncIterable<GenerateContentResponse>
                    // In Express Mode: streamResult itself might be the iterable
                    let sdkStream;
                    
                    if (streamResult.stream) {
                        // Standard mode structure with .stream property
                        sdkStream = Readable.from(streamResult.stream);
                    } else if (streamResult[Symbol.asyncIterator] || streamResult[Symbol.iterator]) {
                        // Express Mode might return the iterator directly
                        sdkStream = Readable.from(streamResult);
                    } else {
                        throw new Error("Unexpected response format from Vertex AI streaming API");
                    }
                    
                    const outputStream = sdkStream.pipe(vertexTransformer);

                    return {
                        response: { body: outputStream }, // Return the transform stream directly
                        selectedKeyId: 'vertex-ai',
                        modelCategory: 'Vertex'
                    };

                } catch (error) {
                    console.error(`Error during Vertex AI stream generation: ${error}`, error); // Keep error log in English
                    return {
                        error: {
                            message: `Vertex AI stream generation failed: ${error.message}`,
                            type: 'vertex_ai_error'
                        },
                        status: 500
                    };
                }
            }
        } else {
            // Non-streaming response
            try {
                // Use the new API for non-streaming
                const response = await ai.models.generateContent(requestPayload);

                if (!response || !response.candidates || response.candidates.length === 0) {
                    // Check if blocked by safety filter
                    const promptFeedback = response?.promptFeedback;
                    if (promptFeedback?.blockReason) {
                        const blockMessage = promptFeedback.blockReasonMessage || `Blocked due to ${promptFeedback.blockReason}`;
                        console.warn(`Request blocked by safety filters: ${blockMessage}`); // Keep warn log in English
                        return {
                            error: {
                                message: `Request blocked by Vertex AI safety filters: ${blockMessage}`,
                                type: "vertex_ai_safety_filter",
                                code: "content_filter"
                            },
                            status: 400
                        };
                    }
                    throw new Error("No valid candidates received from Vertex AI.");
                }

                const candidate = response.candidates[0];
                const finishReasonVertex = candidate.finishReason;
                const finishReasonOpenai = convertVertexFinishReasonToOpenai(finishReasonVertex);

                let responseContent = null;
                let responseToolCalls = [];

                if (candidate.content && candidate.content.parts) {
                    const textParts = [];
                    for (const part of candidate.content.parts) {
                        if (part.text) {
                            textParts.push(part.text);
                        } else if (part.functionCall) {
                            const openaiToolCall = convertVertexToolCallToOpenai(part.functionCall);
                            if (openaiToolCall) {
                                responseToolCalls.push(openaiToolCall);
                            }
                        }
                    }
                    if (textParts.length > 0) {
                        responseContent = textParts.join(''); // Concatenate text parts
                    }
                }

                // Handle response blocked by safety filter
                if (finishReasonOpenai === 'content_filter' && !responseContent && responseToolCalls.length === 0) {
                    const safetyRatings = candidate.safetyRatings || [];
                    const blockMessages = safetyRatings.filter(r => r.blocked).map(r => `${r.category}: ${r.severity || 'Blocked'}`);
                    const message = `Response blocked by Vertex AI safety filters. Reasons: ${blockMessages.join(' ') || finishReasonVertex}`;
                    console.warn(message); // Keep warn log in English
                    return {
                        error: {
                            message: message,
                            type: "vertex_ai_safety_filter",
                            code: "content_filter"
                        },
                        status: 400
                    };
                }

                // Build OpenAI format response message
                const message = {
                    role: 'assistant',
                    content: responseContent, // Can be null if only tool calls
                    tool_calls: responseToolCalls.length > 0 ? responseToolCalls : undefined
                };

                const choice = {
                    index: 0,
                    message: message,
                    finish_reason: finishReasonOpenai,
                    logprobs: null // Not supported
                };

                // Extract usage statistics
                const usage = {
                    prompt_tokens: response.usageMetadata?.promptTokenCount || 0,
                    completion_tokens: response.usageMetadata?.candidatesTokenCount || 0,
                    total_tokens: response.usageMetadata?.totalTokenCount || (response.usageMetadata?.promptTokenCount || 0) + (response.usageMetadata?.candidatesTokenCount || 0) // Calculate if not present
                };

                // Create the full OpenAI format response
                const openaiResponse = {
                    id: `chatcmpl-${uuidv4()}`,
                    object: 'chat.completion',
                    created: Math.floor(Date.now() / 1000),
                    model: requestedModelId,
                    choices: [choice],
                    usage: usage,
                    system_fingerprint: null // Not provided by Vertex
                };

                // Format the response as a JSON object for the 'json' method
                return {
                    response: {
                        json: () => Promise.resolve(openaiResponse), // Return the object directly
                        ok: true,
                        status: 200
                    },
                    selectedKeyId: 'vertex-ai',
                    modelCategory: 'Vertex'
                };

            } catch (error) {
                console.error(`Error during Vertex AI non-stream generation: ${error}`, error); // Keep error log in English
                return {
                    error: {
                        message: `Vertex AI non-stream generation failed: ${error.message}`,
                        type: 'vertex_ai_error'
                    },
                    status: 500
                };
            }
        }
    } catch (error) {
        console.error(`Error in Vertex AI proxy: ${error}`, error); // Keep error log in English
        // Clean up temporary file
        if (tempCredentialsPath) {
            try {
                const dirPath = path.dirname(tempCredentialsPath);
                await fs.rm(dirPath, { recursive: true, force: true });
                console.info(`Cleaned up temporary credentials directory: ${dirPath}`); 
            } catch (e) {
                console.warn(`Failed to delete temporary credentials directory: ${e}`); // Keep warn log in English
            }
        }
        return {
            error: {
                message: `Internal Vertex AI Proxy Error: ${error.message}`,
                type: 'vertex_internal_error'
            },
            status: 500
        };
    }
}

/**
 * Gets the list of Vertex supported models.
 * @returns {Array<string>} Array of supported model IDs.
 */
function getVertexSupportedModels() {
    // Return supported models if either authentication method is available
    return (isUsingExpressMode || VERTEX_JSON_STRING) ? VERTEX_SUPPORTED_MODELS : [];
}

/**
 * Checks if the Vertex feature is enabled (based on database configuration only).
 * @returns {boolean} True if Vertex AI is enabled, false otherwise.
 */
function isVertexEnabled() {
    // Check if we have service account JSON or Express API Key from database
    // This is a synchronous check based on initialization state
    return !!VERTEX_JSON_STRING || isUsingExpressMode;
}

/**
 * Reinitializes Vertex credentials with database configuration.
 * This function is called when the configuration is updated via the admin panel.
 */
async function reinitializeWithDatabaseConfig() {
    console.log("Reinitializing Vertex AI with database configuration...");

    // Reset initialization state
    isVertexInitialized = false;
    isUsingExpressMode = false;
    VERTEX_JSON_STRING = null;

    // Clean up existing credentials file if it exists
    if (tempCredentialsPath) {
        try {
            const fs = require('fs').promises;
            await fs.unlink(tempCredentialsPath);
            console.log("Cleaned up previous credentials file");
        } catch (error) {
            console.warn("Failed to clean up previous credentials file:", error);
        }
        tempCredentialsPath = null;
    }

    // Clear environment variable
    delete process.env.GOOGLE_APPLICATION_CREDENTIALS;

    // Reinitialize with new configuration
    await initializeVertexCredentials();

    console.log("Vertex AI reinitialization completed");
}

module.exports = {
    proxyVertexChatCompletions,
    getVertexSupportedModels,
    isVertexEnabled, // Export check function
    reinitializeWithDatabaseConfig, // Export reinitialization function
    initializeVertexCredentials // Export initialization function for delayed init
};
