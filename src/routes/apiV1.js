// src/routes/apiV1.js

const express = require('express');
const { Readable, Transform } = require('stream'); // For handling streams and transforming
const requireWorkerAuth = require('../middleware/workerAuth');
const geminiProxyService = require('../services/geminiProxyService');
const configService = require('../services/configService'); // For /v1/models
const transformUtils = require('../utils/transform');

// Import vertexProxyService, which now includes manual loading logic
const vertexProxyService = require('../services/vertexProxyService');

const router = express.Router();

// Apply worker authentication middleware to all /v1 routes
router.use(requireWorkerAuth);

// --- /v1/models ---
router.get('/models', async (req, res, next) => {
    try {
        const modelsConfig = await configService.getModelsConfig();
        let modelsData = Object.keys(modelsConfig).map(modelId => ({
            id: modelId,
            object: "model",
            created: Math.floor(Date.now() / 1000), // Placeholder timestamp
            owned_by: "google", // Assuming all configured models are Google's
            // Add other relevant properties if available/needed
        }));

        // Check if web search is enabled
        const webSearchEnabled = String(await configService.getSetting('web_search', '0')) === '1';

        // Add search versions for gemini-2.0+ series models only if web search is enabled
        let searchModels = [];
        if (webSearchEnabled) {
            searchModels = Object.keys(modelsConfig)
                .filter(modelId =>
                    // Match gemini-2.0, gemini-2.5, gemini-3.0, etc. series models
                    /^gemini-[2-9]\.\d/.test(modelId) &&
                    // Exclude models that are already search versions
                    !modelId.endsWith('-search')
                )
                .map(modelId => ({
                    id: `${modelId}-search`,
                    object: "model",
                    created: Math.floor(Date.now() / 1000),
                    owned_by: "google",
                }));
        }

        // Add non-thinking versions for gemini-2.5-flash-preview models
        const nonThinkingModels = Object.keys(modelsConfig)
            .filter(modelId =>
                // Currently only gemini-2.5-flash-preview supports thinkingBudget
                modelId.includes('gemini-2.5-flash-preview') &&
                // Exclude models that are already non-thinking versions
                !modelId.endsWith(':non-thinking')
            )
            .map(modelId => ({
                id: `${modelId}:non-thinking`,
                object: "model",
                created: Math.floor(Date.now() / 1000),
                owned_by: "google",
            }));

        // Merge regular, search and non-thinking model lists
        modelsData = [...modelsData, ...searchModels, ...nonThinkingModels];

        // If Vertex feature is enabled (via manual loading), add Vertex AI supported models
        if (vertexProxyService.isVertexEnabled()) {
            const vertexModels = vertexProxyService.getVertexSupportedModels().map(modelId => ({
                id: modelId,  // Model ID including [v] prefix
                object: "model",
                created: Math.floor(Date.now() / 1000),
                owned_by: "google",
            }));

            // Add Vertex models to the list
            modelsData = [...modelsData, ...vertexModels];
        }

        res.json({ object: "list", data: modelsData });
    } catch (error) {
        console.error("Error handling /v1/models:", error);
        next(error); // Pass to global error handler
    }
});

// --- /v1/chat/completions ---
router.post('/chat/completions', async (req, res, next) => {

    // console.log('[request body]', JSON.stringify(req.body));

    // 재귀적으로 JSON Schema 필드를 변환 (제거 및 $ref 인라인화)
    function transformJsonSchemaFields(schemaObj, parentDefs = {}) {
        if (Array.isArray(schemaObj)) {
            return schemaObj.map(item => transformJsonSchemaFields(item, parentDefs));
        } else if (schemaObj && typeof schemaObj === 'object') {
            // 현재 스키마 객체 내의 $defs를 부모 $defs와 병합하여 모든 정의에 접근 가능하게 함
            const currentLevelDefs = { ...parentDefs, ...(schemaObj.$defs || {}) };

            const transformedObj = {};

            // $ref를 먼저 처리하여 인라인화
            if (schemaObj.$ref) {
                const refPath = schemaObj.$ref;
                if (refPath.startsWith('#/$defs/')) {
                    const defName = refPath.substring('#/$defs/'.length);
                    if (currentLevelDefs[defName]) {
                        // $ref가 가리키는 정의를 찾아서 인라인화하고 재귀적으로 처리
                        // 여기서 $ref가 가리키는 정의가 현재 객체를 대체하므로, 해당 정의를 반환
                        return transformJsonSchemaFields(currentLevelDefs[defName], currentLevelDefs);
                    } else {
                        console.warn(`[WARNING] Local $ref '${refPath}' in schema could not be resolved. This reference will be removed.`);
                        // 해결할 수 없는 $ref는 무시하고 빈 객체 반환 (혹은 오류 처리)
                        return {};
                    }
                } else {
                    console.warn(`[WARNING] Non-local or unsupported $ref '${refPath}' in schema. This reference will be removed.`);
                    // 지원하지 않는 $ref는 무시
                    return {};
                }
            }

            // $ref가 없거나 처리된 후, 다른 필드들을 처리
            for (const key of Object.keys(schemaObj)) {
                // $schema 필드는 메타데이터이므로 항상 제거
                if (key === '$schema') {
                    continue;
                }
                // $defs 필드는 참조 해상도에 사용되었으므로 최종 출력에서는 제거
                if (key === '$defs') {
                    continue;
                }
                // $ref는 위에서 이미 처리했으므로 여기서는 건너뛰기 (이미 대체되었거나 무시됨)
                if (key === '$ref') {
                    continue;
                }

                // 다른 모든 필드는 재귀적으로 변환
                transformedObj[key] = transformJsonSchemaFields(schemaObj[key], currentLevelDefs);
            }
            return transformedObj;
        }
        return schemaObj;
    }

    // tools 처리 뿐만 아니라, function_declarations 내부에도 무조건 적용해야 함!
    if (req.body?.tools) {
        req.body.tools = req.body.tools.map(tool => {
            // function_declarations 내 parameters를 재귀적으로 처리!
            if (tool.function_declarations) {
                tool.function_declarations = tool.function_declarations.map(fnDecl => {
                    if (fnDecl.parameters) {
                        fnDecl.parameters = transformJsonSchemaFields(fnDecl.parameters);
                    }
                    return fnDecl;
                });
            }
            // 기존 function.parameters가 있으면 여기도 처리
            if (tool.function && tool.function.parameters) {
                tool.function.parameters = transformJsonSchemaFields(tool.function.parameters);
            }
            return tool;
        });
    }

    // 사용자 지침에 따라 req.body에서 잠재적인 safety_settings 또는 response_schema 필드를 제거
    // 비록 현재 req.body에 명시적으로 보이지 않지만, 예방적인 차원에서 추가합니다.
    if (req.body?.safety_settings) {
        delete req.body.safety_settings;
        console.log('[request body after clean] safety_settings field removed based on user guidance');
    }
    if (req.body?.response_schema) {
        delete req.body.response_schema;
        console.log('[request body after clean] response_schema field removed based on user guidance');
    }

    console.log('[request body after clean]', JSON.stringify(req.body));

    const openAIRequestBody = req.body;
    const workerApiKey = req.workerApiKey; // Attached by requireWorkerAuth middleware
    const stream = openAIRequestBody?.stream ?? false;
    const requestedModelId = openAIRequestBody?.model; // Keep track for transformations

    try {
        // --- Model Validation Step ---
        // Get all available models to validate against the request
        const modelsConfig = await configService.getModelsConfig();
        let enabledModels = Object.keys(modelsConfig);

        // Add search versions if web search is enabled
        const webSearchEnabled = String(await configService.getSetting('web_search', '0')) === '1';
        if (webSearchEnabled) {
            const searchModels = Object.keys(modelsConfig)
                .filter(modelId => /^gemini-[2-9]\.\d/.test(modelId) && !modelId.endsWith('-search'))
                .map(modelId => `${modelId}-search`);
            enabledModels = [...enabledModels, ...searchModels];
        }

        // Add non-thinking versions
        const nonThinkingModels = Object.keys(modelsConfig)
            .filter(modelId => modelId.includes('gemini-2.5-flash-preview') && !modelId.endsWith(':non-thinking'))
            .map(modelId => `${modelId}:non-thinking`);
        enabledModels = [...enabledModels, ...nonThinkingModels];

        // Add Vertex models if the feature is enabled
        if (vertexProxyService.isVertexEnabled()) {
            const vertexModels = vertexProxyService.getVertexSupportedModels();
            enabledModels = [...enabledModels, ...vertexModels];
        }

        // Validate that the requested model is in the enabled list
        if (!requestedModelId || !enabledModels.includes(requestedModelId)) {
            return res.status(400).json({
                error: {
                    message: `Model not found or not enabled: ${requestedModelId}. Please check the /v1/models endpoint for available models.`,
                    type: 'invalid_request_error',
                    param: 'model'
                }
            });
        }
        // --- End Model Validation ---

        // Check if this is a non-thinking model request
        const isNonThinking = requestedModelId?.endsWith(':non-thinking');
        // Remove the suffix for actual model lookup, but keep original for response
        const actualModelId = isNonThinking ? requestedModelId.replace(':non-thinking', '') : requestedModelId;

        // Set thinkingBudget to 0 for non-thinking models
        const thinkingBudget = isNonThinking ? 0 : undefined;

        // If model was modified, update the request body with the actual model ID
        if (isNonThinking) {
            openAIRequestBody.model = actualModelId;
        }

        let result;

        // KEEPALIVE mode setup - prepare heartbeat callback if needed
        let keepAliveCallback = null;
        const keepAliveEnabled = String(await configService.getSetting('keepalive', '0')) === '1';
        const isSafetyEnabled = await configService.getWorkerKeySafetySetting(workerApiKey);
        const useKeepAlive = keepAliveEnabled && stream && !isSafetyEnabled;

        // Debug logging for KEEPALIVE mode
        console.log(`KEEPALIVE Debug - keepAliveEnabled: ${keepAliveEnabled}, stream: ${stream}, isSafetyEnabled: ${isSafetyEnabled}, useKeepAlive: ${useKeepAlive}`);

        if (useKeepAlive) {
            // Set up KEEPALIVE heartbeat management
            const { Readable } = require('stream');
            const keepAliveSseStream = new Readable({ read() {} });
            let keepAliveTimerId = null;
            let isConnectionClosed = false;

            // Function to safely clean up resources
            const cleanup = () => {
                if (keepAliveTimerId) {
                    clearInterval(keepAliveTimerId);
                    keepAliveTimerId = null;
                }
                isConnectionClosed = true;
            };

            // Monitor client connection status
            res.on('close', () => {
                console.log('KEEPALIVE: Client connection closed');
                cleanup();
            });

            res.on('error', (err) => {
                console.error('KEEPALIVE: Client connection error:', err);
                cleanup();
            });

            // Handle stream errors
            keepAliveSseStream.on('error', (err) => {
                console.error('KEEPALIVE: Stream error:', err);
                cleanup();
            });

            keepAliveSseStream.on('end', () => {
                console.log('KEEPALIVE: Stream ended');
                cleanup();
            });

            keepAliveSseStream.on('finish', () => {
                console.log('KEEPALIVE: Stream finished');
                cleanup();
            });

            const sendKeepAliveSseChunk = () => {
                // Check multiple connection states
                if (isConnectionClosed || res.writableEnded || res.destroyed || !res.writable) {
                    cleanup();
                    return;
                }

                try {
                    const keepAliveSseData = {
                        id: "keepalive",
                        object: "chat.completion.chunk",
                        created: Math.floor(Date.now() / 1000),
                        model: requestedModelId,
                        choices: [{ index: 0, delta: {}, finish_reason: null }]
                    };
                    keepAliveSseStream.push(`data: ${JSON.stringify(keepAliveSseData)}\n\n`);
                } catch (err) {
                    console.error('KEEPALIVE: Error sending heartbeat:', err);
                    cleanup();
                }
            };

            // Set streaming headers for KEEPALIVE mode
            res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
            res.setHeader('Cache-Control', 'no-cache');
            res.setHeader('Connection', 'keep-alive');
            res.setHeader('X-Proxied-By', 'gemini-proxy-panel-node');

            // Pipe stream to response after setting up error handlers
            const pipeStream = keepAliveSseStream.pipe(res);

            // Handle pipe errors
            pipeStream.on('error', (err) => {
                console.error('KEEPALIVE: Pipe error:', err);
                cleanup();
            });

            // Create callback object for geminiProxyService
            keepAliveCallback = {
                startHeartbeat: () => {
                    console.log('KEEPALIVE: Starting heartbeat (3 second intervals)');
                    keepAliveTimerId = setInterval(sendKeepAliveSseChunk, 3000); // 3 second intervals
                    sendKeepAliveSseChunk(); // Send first one immediately
                },
                stopHeartbeat: () => {
                    console.log('KEEPALIVE: Stopping heartbeat');
                    cleanup();
                },
                sendFinalResponse: (responseData) => {
                    try {
                        // Double-check connection status
                        if (res.writableEnded || res.destroyed || !res.writable) {
                            console.warn("KEEPALIVE: Response stream ended before data could be sent.");
                            return;
                        }

                        const openAIResponse = JSON.parse(transformUtils.transformGeminiResponseToOpenAI(
                            responseData,
                            requestedModelId
                        ));
                        const content = openAIResponse.choices[0].message.content || "";
                        const completeChunk = {
                            id: `chatcmpl-${Date.now()}-${Math.random().toString(36).substring(2, 8)}`,
                            object: "chat.completion.chunk",
                            created: Math.floor(Date.now() / 1000),
                            model: requestedModelId,
                            choices: [{
                                index: 0,
                                delta: { role: "assistant", content: content },
                                finish_reason: openAIResponse.choices[0].finish_reason || "stop"
                            }]
                        };

                        keepAliveSseStream.push(`data: ${JSON.stringify(completeChunk)}\n\n`);
                        keepAliveSseStream.push('data: [DONE]\n\n');
                        keepAliveSseStream.push(null); // End the stream
                    } catch (error) {
                        console.error("Error processing KEEPALIVE final response:", error);
                        const errorPayload = {
                            error: {
                                message: error.message || 'Failed to process KEEPALIVE response',
                                type: error.type || 'keepalive_proxy_error',
                                code: error.code,
                                status: error.status
                            }
                        };
                        keepAliveSseStream.push(`data: ${JSON.stringify(errorPayload)}\n\n`);
                        keepAliveSseStream.push('data: [DONE]\n\n');
                        keepAliveSseStream.push(null);
                    }
                },
                sendError: (errorData) => {
                    try {
                        // Double-check connection status
                        if (res.writableEnded || res.destroyed || !res.writable) {
                            console.warn("KEEPALIVE: Response stream ended before error could be sent.");
                            return;
                        }

                        const errorPayload = {
                            error: {
                                message: errorData.message || 'Upstream API error',
                                type: errorData.type || 'upstream_error',
                                code: errorData.code
                            }
                        };

                        keepAliveSseStream.push(`data: ${JSON.stringify(errorPayload)}\n\n`);
                        keepAliveSseStream.push('data: [DONE]\n\n');
                        keepAliveSseStream.push(null); // End the stream
                    } catch (error) {
                        console.error("Error sending KEEPALIVE error response:", error);
                        // Try to end the stream gracefully
                        try {
                            keepAliveSseStream.push(null);
                        } catch (e) {
                            console.error("Failed to end stream after error:", e);
                        }
                    }
                }
            };
        }

        // Check if it's a Vertex model (with [v] prefix) and confirm Vertex feature is enabled
        if (requestedModelId && requestedModelId.startsWith('[v]') && vertexProxyService.isVertexEnabled()) {
            // Use Vertex proxy service to handle the request
            console.log(`Using Vertex AI to process model: ${requestedModelId}`);
            result = await vertexProxyService.proxyVertexChatCompletions(
                openAIRequestBody,
                workerApiKey,
                stream,
                keepAliveCallback
            );
        } else {
            // Use Gemini proxy service to handle the request with optional thinkingBudget
            result = await geminiProxyService.proxyChatCompletions(
                openAIRequestBody,
                workerApiKey,
                stream,
                thinkingBudget,
                keepAliveCallback
            );
        }

        // Check if the service returned an error
        if (result.error) {
            // In KEEPALIVE mode, send error through the heartbeat stream
            if (useKeepAlive && keepAliveCallback) {
                console.log('KEEPALIVE: Sending error response through heartbeat stream');
                try {
                    // Stop heartbeat first
                    keepAliveCallback.stopHeartbeat();

                    // Send error through the stream
                    const errorPayload = {
                        error: {
                            message: result.error.message || 'Upstream API error',
                            type: result.error.type || 'upstream_error',
                            code: result.error.code,
                            status: result.status || 500
                        }
                    };

                    // Use the existing stream to send error
                    const { Readable } = require('stream');
                    const errorStream = new Readable({ read() {} });
                    errorStream.pipe(res);
                    errorStream.push(`data: ${JSON.stringify(errorPayload)}\n\n`);
                    errorStream.push('data: [DONE]\n\n');
                    errorStream.push(null);
                    return;
                } catch (streamError) {
                    console.error('KEEPALIVE: Failed to send error through stream:', streamError);
                    // Fallback: if stream fails, we can't do much more since headers are already sent
                    return;
                }
            } else {
                // Normal mode: set headers and send JSON error
                res.setHeader('Content-Type', 'application/json');
                return res.status(result.status || 500).json({ error: result.error });
            }
        }

        // Destructure the successful result
        const { response: geminiResponse, selectedKeyId, modelCategory } = result;

        console.log('[response]', JSON.stringify(geminiResponse.body));

        // --- Handle Response ---

        // Check if this is a KEEPALIVE special response first
        if (result.isKeepAlive) {
            console.log(`KEEPALIVE mode activated for model ${requestedModelId} - response will be handled asynchronously`);
            // In the new KEEPALIVE mode, the response is handled completely asynchronously
            // The heartbeat is already started and the response will be sent when ready
            // We just return here as everything is handled in the background
            return; // Exit early for KEEPALIVE mode
        }

        // Set common headers (only for non-KEEPALIVE mode)
        res.setHeader('X-Proxied-By', 'gemini-proxy-panel-node');
        res.setHeader('X-Selected-Key-ID', selectedKeyId); // Send back which key was used (optional)

        if (stream) {
            // --- Streaming Response ---
            res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
            res.setHeader('Cache-Control', 'no-cache');
            res.setHeader('Connection', 'keep-alive');
            // Apply CORS headers if not already handled globally by middleware
            // res.setHeader('Access-Control-Allow-Origin', '*'); // Example if needed


            // Check in advance if it's keepalive mode, if so, no need to check the body stream
            if (!result.isKeepAlive) {
                if (!geminiResponse.body || typeof geminiResponse.body.pipe !== 'function') {
                    console.error('Gemini response body is not a readable stream for streaming request.');
                    // Send a valid SSE error event before closing
                    const errorPayload = JSON.stringify({ error: { message: 'Upstream response body is not readable.', type: 'proxy_error' } });
                    res.write(`data: ${errorPayload}\n\n`);
                    res.write('data: [DONE]\n\n');
                    return res.end();
                }
            }

            const decoder = new TextDecoder();
            let buffer = '';
            let lineBuffer = '';
            let jsonCollector = '';
            let isCollectingJson = false;
            let openBraces = 0;
            let closeBraces = 0;

            // Implement stream processing transformer for both Gemini and Vertex streams
            const streamTransformer = new Transform({
                transform(chunk, encoding, callback) {
                    try {
                        const chunkStr = decoder.decode(chunk, { stream: true });
                        buffer += chunkStr;

                        // Process based on the source (Gemini or Vertex)
                        if (selectedKeyId === 'vertex-ai') {
                            // Vertex stream response is a series of continuous JSON objects without newline separation
                            // Use a method similar to Gemini to process JSON objects
                            let startPos = -1;
                            let endPos = -1;
                            let bracketDepth = 0;
                            let inString = false;
                            let escapeNext = false;
                            let flushed = false;

                            // Scan the entire buffer to find complete JSON objects
                            for (let i = 0; i < buffer.length; i++) {
                                const char = buffer[i];

                                // Handle characters inside strings
                                if (inString) {
                                    if (escapeNext) {
                                        escapeNext = false;
                                    } else if (char === '\\') {
                                        escapeNext = true;
                                    } else if (char === '"') {
                                        inString = false;
                                    }
                                    continue;
                                }

                                // Handle characters outside strings
                                if (char === '{') {
                                    if (bracketDepth === 0) {
                                        startPos = i; // Record the starting position of a new JSON object
                                    }
                                    bracketDepth++;
                                } else if (char === '}') {
                                    bracketDepth--;
                                    if (bracketDepth === 0 && startPos !== -1) {
                                        endPos = i;

                                        // Extract and process the complete JSON object
                                        const jsonStr = buffer.substring(startPos, endPos + 1);
                                        try {
                                            // Check if it's the 'done' marker from vertexProxyService's flush
                                            // We only need to parse if we suspect it might be the done object.
                                            // Otherwise, jsonStr is already the stringified chunk we want.
                                            if (jsonStr.includes('"done":true')) { // Quick check
                                                try {
                                                    const jsonObj = JSON.parse(jsonStr);
                                                    if (jsonObj.done) {
                                                        // This is the '{"done":true}' from vertexProxyService's flush.
                                                        // The main flush of apiV1's transformer will send 'data: [DONE]\n\n'. So, ignore this one.
                                                    } else {
                                                        // It wasn't the done object, but was parsable. Send it.
                                                        this.push(`data: ${jsonStr}\n\n`);
                                                        if (typeof res.flush === 'function') res.flush();
                                                    }
                                                } catch (e) {
                                                    // Parsing failed, but it might still be a valid (non-done) chunk.
                                                    // This case should ideally not happen if vertexProxyService sends valid JSONs.
                                                    console.error("Error parsing potential Vertex JSON object:", e, "Original string:", jsonStr);
                                                    this.push(`data: ${jsonStr}\n\n`); // Send as is if parsing fails but wasn't 'done'
                                                    if (typeof res.flush === 'function') res.flush();
                                                }
                                            } else {
                                                // Not the 'done' marker, so jsonStr is a data chunk.
                                                this.push(`data: ${jsonStr}\n\n`);
                                                if (typeof res.flush === 'function') res.flush();
                                            }
                                        } catch (e) {
                                            // This outer catch handles errors from buffer.substring or other unexpected issues
                                            console.error("Error processing Vertex JSON chunk:", e, "Original string:", jsonStr);
                                        }

                                        // Continue searching for the next object
                                        startPos = -1;

                                        // Truncate the processed part
                                        if (i + 1 < buffer.length) {
                                            buffer = buffer.substring(endPos + 1);
                                            i = -1; // Reset index to scan the remaining buffer from the beginning
                                        } else {
                                            buffer = '';
                                            break; // Exit loop if buffer is exhausted
                                        }
                                    }
                                } else if (char === '"') {
                                    inString = true;
                                }
                            }
                        } else {
                             // Original Gemini stream processing (find raw Gemini JSON chunks)
                            let startPos = -1;
                            let endPos = -1;
                        let bracketDepth = 0;
                        let inString = false;
                        let escapeNext = false;

                        // Scan the entire buffer to find complete JSON objects
                        for (let i = 0; i < buffer.length; i++) {
                            const char = buffer[i];

                            // Handle characters within strings
                            if (inString) {
                                if (escapeNext) {
                                    escapeNext = false;
                                } else if (char === '\\') {
                                    escapeNext = true;
                                } else if (char === '"') {
                                    inString = false;
                                }
                                continue;
                            }

                            // Handle characters outside strings
                            if (char === '{') {
                                if (bracketDepth === 0) {
                                    startPos = i; // Record the starting position of a new JSON object
                                }
                                bracketDepth++;
                            } else if (char === '}') {
                                bracketDepth--;
                                if (bracketDepth === 0 && startPos !== -1) {
                                    endPos = i;

                                    // Extract and process the complete JSON object
                                    const jsonStr = buffer.substring(startPos, endPos + 1);
                                    try {
                                        const jsonObj = JSON.parse(jsonStr);
                                        // Immediately process and send this object
                                        processGeminiObject(jsonObj, this);
                                    } catch (e) {
                                        console.error("Error parsing JSON object:", e);
                                    }

                                                // Continue searching for the next object
                                                startPos = -1;
                                            }
                                        } else if (char === '"') {
                                            inString = true;
                                        } else if (char === '[' && !inString && startPos === -1) {
                                            // Ignore the start marker of JSON arrays, as we process each object individually
                                            continue;
                                        } else if (char === ']' && !inString && bracketDepth === 0) {
                                            // Ignore the end marker of JSON arrays
                                            continue;
                                        } else if (char === ',') {
                                            // If there's a comma after an object, continue processing the next object
                                            continue;
                                        }
                                    }

                                    // Keep the unprocessed part for Gemini stream
                                    if (startPos !== -1 && endPos !== -1 && endPos > startPos) {
                                        buffer = buffer.substring(endPos + 1);
                                    } else if (startPos !== -1) {
                                        buffer = buffer.substring(startPos);
                                    } else {
                                        buffer = '';
                                    }
                            } // End of else (Gemini stream processing)

                        callback();
                    } catch (e) {
                        console.error("Error in stream transform:", e);
                        callback(e);
                    }
                },

                flush(callback) {
                    try {
                // Handling the remaining buffer
                if (buffer.trim()) {
                     if (selectedKeyId === 'vertex-ai') {
                        if (buffer.trim()) {
                            let startPos = -1;
                            let endPos = -1;
                            let bracketDepth = 0;
                            let inString = false;
                            let escapeNext = false;

                            for (let i = 0; i < buffer.length; i++) {
                                const char = buffer[i];

                                if (inString) {
                                    if (escapeNext) {
                                        escapeNext = false;
                                    } else if (char === '\\') {
                                        escapeNext = true;
                                    } else if (char === '"') {
                                        inString = false;
                                    }
                                    continue;
                                }

                                if (char === '{') {
                                    if (bracketDepth === 0) {
                                        startPos = i;
                                    }
                                    bracketDepth++;
                                } else if (char === '}') {
                                    bracketDepth--;
                                    if (bracketDepth === 0 && startPos !== -1) {
                                        endPos = i;

                                        try {
                                            const jsonStr = buffer.substring(startPos, endPos + 1);
                                            const jsonObj = JSON.parse(jsonStr);
                                            if (!jsonObj.done) { // Avoid duplicate DONE
                                                this.push(`data: ${JSON.stringify(jsonObj)}\n\n`);
                                            }
                                        } catch (e) {
                                            console.debug("Could not parse Vertex buffer JSON:", e);
                                        }

                                        // Update the buffer and reset the index
                                        if (endPos + 1 < buffer.length) {
                                            buffer = buffer.substring(endPos + 1);
                                            i = -1; // Reset index
                                        } else {
                                            buffer = '';
                                            break;
                                        }
                                    }
                                } else if (char === '"') {
                                    inString = true;
                                }
                            }
                        }
                     } else {
                                // Try parsing remaining Gemini JSON object
                                try {
                                    const jsonObj = JSON.parse(buffer);
                                    processGeminiObject(jsonObj, this); // Use existing Gemini processing
                                } catch (e) {
                                    console.debug("Could not parse final Gemini buffer:", buffer, e);
                                }
                             }
                        }

                        // Always send the final [DONE] event
                                                // console.log("Stream transformer flushing, sending [DONE]."); // Removed log
                                                this.push('data: [DONE]\n\n');
                                                callback();
                                            } catch (e) {
                                                console.error("Error in stream flush:", e); // Keep error log in English
                        callback(e);
                    }
                }
            });

            // Process a single Gemini API response object and convert it to OpenAI format
            function processGeminiObject(geminiObj, stream) {
                if (!geminiObj) return;

                // If it's a valid Gemini response object (contains candidates)
                if (geminiObj.candidates && geminiObj.candidates.length > 0) {
                    // Convert and send directly
                    const openaiChunkStr = transformUtils.transformGeminiStreamChunk(geminiObj, requestedModelId);
                    if (openaiChunkStr) {
                        stream.push(openaiChunkStr);
                    }
                } else if (Array.isArray(geminiObj)) {
                    // If it's an array, process each element
                    for (const item of geminiObj) {
                        processGeminiObject(item, stream);
                    }
                } else if (geminiObj.text) {
                    // Single text fragment, construct Gemini format
                    const mockGeminiChunk = {
                        candidates: [{
                            content: {
                                parts: [{ text: geminiObj.text }],
                                role: "model"
                            }
                        }]
                    };

                    const openaiChunkStr = transformUtils.transformGeminiStreamChunk(mockGeminiChunk, requestedModelId);
                    if (openaiChunkStr) {
                        stream.push(openaiChunkStr);
                    }
                }
                // May need to handle other response types...
            }

            // Standard (non-KEEPALIVE) Gemini and Vertex streams
            if (!geminiResponse || !geminiResponse.body || typeof geminiResponse.body.pipe !== 'function') {
                console.error('Upstream response body is not a readable stream for standard streaming request.');
                const errorPayload = JSON.stringify({ error: { message: 'Upstream response body is not readable.', type: 'proxy_error' } });
                res.write(`data: ${errorPayload}\n\n`); // Use res.write for SSE
                res.write('data: [DONE]\n\n');
                return res.end();
            }

            console.log(`Piping ${selectedKeyId === 'vertex-ai' ? 'Vertex' : 'Gemini'} stream through transformer.`);
            geminiResponse.body.pipe(streamTransformer).pipe(res);

            geminiResponse.body.on('error', (err) => {
                console.error(`Error reading stream from upstream (${selectedKeyId}):`, err);
                if (!res.headersSent) {
                    // If headers not sent, we can still send a JSON error
                    res.status(500).json({ error: { message: 'Error reading stream from upstream API.' } });
                } else if (!res.writableEnded) {
                    // If headers sent but stream not ended, try to send an SSE error then end
                    const sseError = JSON.stringify({ error: { message: 'Upstream stream error', type: 'upstream_error'} });
                    res.write(`data: ${sseError}\n\n`);
                    res.write('data: [DONE]\n\n');
                    res.end();
                }
                // If res.writableEnded is true, nothing more we can do.
            });

            streamTransformer.on('error', (err) => {
                console.error('Error in stream transformer:', err);
                if (!res.headersSent) {
                    res.status(500).json({ error: { message: 'Error processing stream data.' } });
                } else if (!res.writableEnded) {
                    const sseError = JSON.stringify({ error: { message: 'Stream processing error', type: 'transform_error'} });
                    res.write(`data: ${sseError}\n\n`);
                    res.write('data: [DONE]\n\n');
                    res.end();
                }
            });

             console.log(`Streaming response initiated for key ${selectedKeyId}`);


        } else {
            // --- Non-Streaming Response ---
            res.setHeader('Content-Type', 'application/json; charset=utf-8');

            try {
                if (selectedKeyId === 'vertex-ai') {
                    // Vertex service already transformed the response to OpenAI format
                    const openaiJson = await geminiResponse.json(); // Get the pre-transformed JSON
                    res.status(geminiResponse.status || 200).json(openaiJson); // Send it directly
                    console.log(`Non-stream Vertex request completed, status: ${geminiResponse.status || 200}`);
                } else {
                    // Original Gemini service response handling
                    const geminiJson = await geminiResponse.json(); // Parse the raw upstream Gemini JSON
                    const openaiJsonString = transformUtils.transformGeminiResponseToOpenAI(geminiJson, requestedModelId); // Transform it
                    // Use Gemini's original status code if available and OK, otherwise default to 200
                    res.status(geminiResponse.ok ? geminiResponse.status : 200).send(openaiJsonString);
                    console.log(`Non-stream Gemini request completed for key ${selectedKeyId}, status: ${geminiResponse.status}`);
                }
            } catch (jsonError) {
                 console.error("Error parsing Gemini non-stream JSON response:", jsonError);
                 // Check if response text might give clues
                 try {
                    const errorText = await geminiResponse.text(); // Need to re-read or clone earlier
                    console.error("Gemini non-stream response text:", errorText);
                 } catch(e){}
                 next(new Error("Failed to parse upstream API response.")); // Pass to global error handler
            }
        }

    } catch (error) {
        console.error("Error in /v1/chat/completions handler:", error);
        next(error); // Pass error to the global Express error handler
    }
});

module.exports = router;
