// --- Transformation logic migrated from Cloudflare Worker ---

/**
 * Parses a data URI string.
 * @param {string} dataUri - The data URI (e.g., "data:image/jpeg;base64,...").
 * @returns {{ mimeType: string; data: string } | null} Parsed data or null if invalid.
 */
function parseDataUri(dataUri) {
    if (!dataUri) return null;
	const match = dataUri.match(/^data:(.+?);base64,(.+)$/);
	if (!match) return null;
	return { mimeType: match[1], data: match[2] };
}

/**
 * Sanitize OpenAI-style JSON Schema for Gemini/Vertex functionDeclarations.parameters.
 * - Removes unsupported keywords (e.g., $schema, definitions, $defs, $ref, patternProperties)
 * - Normalizes combinators: any_of/one_of/all_of -> anyOf/oneOf/allOf
 * - Converts const -> enum with single value
 * - Recursively sanitizes nested schemas (properties/items/additionalProperties)
 * - Coerces numeric constraints to numbers, prunes invalid types
 *
 * @param {any} schema
 * @param {{dropTitle?:boolean, keepDescription?:boolean, dropFormatIfError?:boolean, debugEnvVar?:string}} [opts]
 * @returns {any}
 */
function sanitizeToolParameters(schema, opts = {}) {
    const options = {
        dropTitle: true,
        keepDescription: true,
        dropFormatIfError: false,
        debugEnvVar: 'TOOL_SCHEMA_DEBUG',
        ...opts,
    };

    const DEBUG = process && process.env && process.env[options.debugEnvVar] === '1';

    const SUPPORTED_TYPES = new Set(['string','number','integer','boolean','object','array']);

    function logChange(pointer, message) {
        if (DEBUG) console.warn(`[schema-sanitize] ${pointer}: ${message}`);
    }

    function clone(val) {
        try { return JSON.parse(JSON.stringify(val)); } catch { return val; }
    }

    function toNumber(n) {
        if (typeof n === 'number') return n;
        const parsed = Number(n);
        return Number.isFinite(parsed) ? parsed : undefined;
    }

    function sanitize(node, pointer = '#') {
        if (node == null || typeof node !== 'object') return node;
        const s = Array.isArray(node) ? node.map((v, i) => sanitize(v, `${pointer}/${i}`)) : clone(node);
        if (Array.isArray(s)) return s; // only for arrays that arrive here intentionally

        // Remove known unsupported top-level keys
        for (const k of ['$schema','definitions','$defs','$ref','patternProperties','examples','deprecated','readOnly','writeOnly']) {
            if (k in s) { delete s[k]; logChange(pointer, `dropped '${k}'`); }
        }

        // Titles/descriptions
        if (options.dropTitle && 'title' in s) { delete s.title; logChange(pointer, `dropped 'title'`); }
        if (!options.keepDescription && 'description' in s) { delete s.description; logChange(pointer, `dropped 'description'`); }

        // const -> enum
        if ('const' in s) {
            const val = s.const; delete s.const; s.enum = [val];
            logChange(pointer, `converted 'const' to 'enum' with single value`);
        }

        // Normalize combinators
        if ('any_of' in s) { s.anyOf = s.any_of; delete s.any_of; logChange(pointer, `renamed 'any_of' -> 'anyOf'`); }
        if ('one_of' in s) { s.oneOf = s.one_of; delete s.one_of; logChange(pointer, `renamed 'one_of' -> 'oneOf'`); }
        if ('all_of' in s) { s.allOf = s.all_of; delete s.all_of; logChange(pointer, `renamed 'all_of' -> 'allOf'`); }

        // Sanitize combinators
        for (const key of ['anyOf','oneOf','allOf']) {
            if (s[key]) {
                if (!Array.isArray(s[key])) {
                    s[key] = [s[key]]; logChange(pointer, `wrapped '${key}' into array`);
                }
                // First, sanitize each branch
                s[key] = s[key].map((sub, idx) => sanitize(sub, `${pointer}/${key}/${idx}`));

                // Post-process branches: remove null-only branches, coerce empty schemas to {type:'object'}
                const isNullOnly = (node) => {
                    if (!node || typeof node !== 'object') return false;
                    // enum:[null] or const:null already converted to enum earlier
                    if (Array.isArray(node.enum) && node.enum.length === 1 && node.enum[0] === null) return true;
                    if (node.type === 'null') return true;
                    return false;
                };
                const isEmptySchema = (node) => node && typeof node === 'object' && Object.keys(node).length === 0;

                let branches = s[key].filter((sub) => !isNullOnly(sub))
                                      .map((sub) => (isEmptySchema(sub) ? { type: 'object' } : sub));

                if (branches.length === 0) {
                    // Fallback: if everything was null-only, just assume object to satisfy Gemini
                    branches = [{ type: 'object' }];
                }

                s[key] = branches;

                // If the current node lacks 'type', try to infer from branches (prefer object)
                if (!('type' in s)) {
                    const branchTypes = branches.map(b => b && b.type).filter(Boolean);
                    if (branchTypes.includes('object')) {
                        s.type = 'object';
                        logChange(pointer, `inferred type 'object' from ${key} branches`);
                    } else if (branchTypes.includes('array')) {
                        s.type = 'array';
                        logChange(pointer, `inferred type 'array' from ${key} branches`);
                    }
                }

                // Optional: if single branch remains, keep as-is (leaving anyOf with 1 item is acceptable)
            }
        }

        // Type handling
        if ('type' in s) {
            if (Array.isArray(s.type)) {
                const filtered = s.type.filter(t => SUPPORTED_TYPES.has(t));
                if (filtered.length === 0) {
                    delete s.type; logChange(pointer, `removed unsupported 'type' array`);
                } else if (filtered.length === 1) {
                    s.type = filtered[0]; logChange(pointer, `reduced 'type' array to '${s.type}'`);
                } else {
                    // Multiple supported types → represent via anyOf
                    delete s.type;
                    s.anyOf = filtered.map((t, i) => ({ type: t }));
                    logChange(pointer, `converted multi-type to anyOf of supported types`);
                }
            } else if (typeof s.type === 'string') {
                if (!SUPPORTED_TYPES.has(s.type)) {
                    logChange(pointer, `unsupported type '${s.type}' dropped`);
                    delete s.type;
                }
            }
        }

        // Infer missing type from schema shape for Gemini strictness
        if (!('type' in s)) {
            const looksObject = !!(s && (s.properties || s.required || ('additionalProperties' in s)));
            const looksArray = !!(s && (s.items !== undefined || Array.isArray(s.prefixItems)));
            if (looksObject) {
                s.type = 'object';
                logChange(pointer, `inferred type 'object' from properties/required/additionalProperties`);
            } else if (looksArray) {
                s.type = 'array';
                logChange(pointer, `inferred type 'array' from items/prefixItems`);
            }
        }

        // Object specifics
        if (s.type === 'object' || s.properties || s.required || s.additionalProperties !== undefined) {
            if (s.properties && typeof s.properties === 'object') {
                for (const [prop, sub] of Object.entries(s.properties)) {
                    s.properties[prop] = sanitize(sub, `${pointer}/properties/${prop}`);
                }
            }
            if (Array.isArray(s.required)) {
                s.required = s.required.filter((r) => typeof r === 'string');
            }
            if ('additionalProperties' in s) {
                const ap = s.additionalProperties;
                if (ap === true || ap === false) {
                    // ok
                } else if (ap && typeof ap === 'object') {
                    s.additionalProperties = sanitize(ap, `${pointer}/additionalProperties`);
                } else {
                    // Fallback to boolean false to avoid complex schemas
                    s.additionalProperties = false;
                    logChange(pointer, `coerced additionalProperties to false`);
                }
            }
        }

        // Array specifics
        if (s.type === 'array' || s.items) {
            if (s.items) {
                s.items = sanitize(s.items, `${pointer}/items`);
            }
            if ('minItems' in s) { const v = toNumber(s.minItems); if (v === undefined) { delete s.minItems; logChange(pointer, `dropped invalid minItems`); } else { s.minItems = v; } }
            if ('maxItems' in s) { const v = toNumber(s.maxItems); if (v === undefined) { delete s.maxItems; logChange(pointer, `dropped invalid maxItems`); } else { s.maxItems = v; } }
            if ('uniqueItems' in s && typeof s.uniqueItems !== 'boolean') { delete s.uniqueItems; logChange(pointer, `dropped invalid uniqueItems`); }
        }

        // Numeric & string constraints coercion
        for (const k of ['minimum','maximum','exclusiveMinimum','exclusiveMaximum','multipleOf']) {
            if (k in s) {
                const v = toNumber(s[k]);
                if (v === undefined) { delete s[k]; logChange(pointer, `dropped invalid ${k}`); }
                else if (k === 'exclusiveMinimum' || k === 'exclusiveMaximum') { delete s[k]; logChange(pointer, `dropped ${k} (not supported)`); }
                else { s[k] = v; }
            }
        }
        for (const k of ['minLength','maxLength']) {
            if (k in s) { const v = toNumber(s[k]); if (v === undefined) { delete s[k]; logChange(pointer, `dropped invalid ${k}`); } else { s[k] = v; } }
        }
        if ('format' in s && options.dropFormatIfError) { delete s.format; logChange(pointer, `dropped 'format' due to config`); }

        return s;
    }

    return sanitize(clone(schema), '#');
}

/**
 * Transforms an OpenAI-compatible request body to the Gemini API format.
 * @param {object} requestBody - The OpenAI request body.
 * @param {string} [requestedModelId] - The specific model ID requested.
 * @param {boolean} [isSafetyEnabled=true] - Whether safety filtering is enabled for this request.
 * @returns {{ contents: any[]; systemInstruction?: any; tools?: any[]; toolConfig?: any }} Gemini formatted request parts.
 */
function transformOpenAiToGemini(requestBody, requestedModelId, isSafetyEnabled = true) {
	const messages = requestBody.messages || [];
	const openAiTools = requestBody.tools;
	const openAiToolChoice = requestBody.tool_choice;

	// 1. Transform Messages
	const contents = [];
	let systemInstruction = undefined;
	let systemMessageLogPrinted = false; // Add flag to track if log has been printed

 // Build mapping from assistant tool_calls id -> function name for later tool messages
	const toolCallIdToName = new Map();

	messages.forEach((msg) => {
		let role = undefined;
		let parts = [];

		// 1. Map Role
		switch (msg.role) {
			case 'user':
				role = 'user';
				break;
			case 'assistant':
				role = 'model';
				break;
			case 'tool':
				// OpenAI 'tool' role carries tool execution result; Gemini expects this as a user message with functionResponse
				role = 'user';
				break;
			case 'system':
                // If safety is disabled OR it's a gemma model, treat system as user
                if (isSafetyEnabled === false || (requestedModelId && requestedModelId.startsWith('gemma'))) {
                    // Only print the log message for the first system message encountered
                    if (!systemMessageLogPrinted) {
                        console.log(`Safety disabled (${isSafetyEnabled}) or Gemma model detected (${requestedModelId}). Treating system message as user message.`);
                        systemMessageLogPrinted = true;
                    }
                    role = 'user';
                    // Content processing for 'user' role will happen below
                }
                // Otherwise (safety enabled and not gemma), create systemInstruction
                else {
                    if (typeof msg.content === 'string') {
                        systemInstruction = { role: "system", parts: [{ text: msg.content }] };
                    } else if (Array.isArray(msg.content)) { // Handle complex system prompts if needed
                        const textContent = msg.content.find((p) => p.type === 'text')?.text;
                        if (textContent) {
                            systemInstruction = { role: "system", parts: [{ text: textContent }] };
                        }
                    }
                    return; // Skip adding this message to 'contents' when creating systemInstruction
                }
                break; // Break for 'system' role (safety disabled/gemma case falls through to content processing)
			default:
				console.warn(`Unknown role encountered: ${msg.role}. Skipping message.`);
				return; // Skip unknown roles
		}

		// 2. Map Content to Parts
		// Special handling for assistant with tool_calls (may have null/empty content)
		if (msg.role === 'assistant' && Array.isArray(msg.tool_calls) && msg.tool_calls.length > 0) {
			msg.tool_calls.forEach((tc, index) => {
				if (tc?.type === 'function' && tc.function?.name) {
					// Track mapping from tool_call id to function name for subsequent tool message
					if (tc.id) toolCallIdToName.set(tc.id, tc.function.name);
					let argsObj = {};
					try { argsObj = JSON.parse(tc.function.arguments || '{}'); } catch (e) { argsObj = { _error: 'Invalid JSON arguments', raw: tc.function.arguments }; }
					parts.push({ functionCall: { name: tc.function.name, args: argsObj } });
				}
			});
			// If there is also text content, include it
			if (typeof msg.content === 'string' && msg.content.length > 0) {
				parts.push({ text: msg.content });
			} else if (Array.isArray(msg.content)) {
				msg.content.forEach((part) => { if (part.type === 'text') parts.push({ text: part.text }); });
			}
		}
		else if (msg.role === 'tool') {
			// Convert tool result into functionResponse under a user message
			const name = msg.name || (msg.tool_call_id ? toolCallIdToName.get(msg.tool_call_id) : undefined);
			let responseObj;
			if (typeof msg.content === 'string') {
				try { responseObj = JSON.parse(msg.content); }
				catch (e) { responseObj = { content: msg.content }; }
			} else if (typeof msg.content === 'object' && msg.content !== null) {
				responseObj = msg.content;
			} else {
				responseObj = { content: String(msg.content ?? '') };
			}
			// Gemini/Vertex require functionResponse.response to be a JSON object (Struct)
			if (responseObj === null || Array.isArray(responseObj) || typeof responseObj !== 'object') {
				responseObj = { content: responseObj };
			}
			if (name) {
				parts.push({ functionResponse: { name, response: responseObj } });
			} else {
				console.warn(`Tool message without resolvable function name (tool_call_id: ${msg.tool_call_id}). Sending as text.`);
				parts.push({ text: typeof msg.content === 'string' ? msg.content : JSON.stringify(responseObj) });
			}
		}
		else if (typeof msg.content === 'string') {
			parts.push({ text: msg.content });
		} else if (Array.isArray(msg.content)) {
			// Handle multi-part messages (text and images)
			msg.content.forEach((part) => {
				if (part.type === 'text') {
					parts.push({ text: part.text });
				} else if (part.type === 'image_url') {
                    // In Node.js, image_url might just contain the URL, or a data URI
                    // Assuming it follows the OpenAI spec and provides a URL field within image_url
                    const imageUrl = part.image_url?.url;
                    if (!imageUrl) {
                        console.warn(`Missing url in image_url part. Skipping image part.`);
                        return;
                    }
					const imageData = parseDataUri(imageUrl); // Attempt to parse as data URI
					if (imageData) {
						parts.push({ inlineData: { mimeType: imageData.mimeType, data: imageData.data } }); // Structure expected by Gemini
					} else {
                        // If it's not a data URI, we can't directly include it as inlineData.
                        // Gemini API (currently) doesn't support fetching from URLs directly in the standard API.
                        // Consider alternatives:
                        // 1. Pre-fetch the image data server-side (adds complexity, requires fetch).
                        // 2. Reject requests with image URLs (simpler for now).
                        console.warn(`Image URL is not a data URI: ${imageUrl}. Gemini API requires inlineData (base64). Skipping image part.`);
                        // Decide how to handle this. For now, we skip.
                        // parts.push({ text: `[Unsupported Image URL: ${imageUrl}]` }); // Optional: replace with text placeholder
					}
				} else {
					console.warn(`Unknown content part type: ${part.type}. Skipping part.`);
				}
			});
		} else {
			// Allow assistant messages that only contain tool_calls (handled above) to pass without content
			if (!(msg.role === 'assistant' && Array.isArray(msg.tool_calls) && msg.tool_calls.length > 0)) {
				console.warn(`Unsupported content type for role ${msg.role}: ${typeof msg.content}. Skipping message.`);
				return;
			}
		}

		// Add the transformed message to contents if it has a role and parts
		if (role && parts.length > 0) {
			contents.push({ role, parts });
		}
	});

	// 2. Transform Tools
	let geminiTools = undefined;
	if (openAiTools && Array.isArray(openAiTools) && openAiTools.length > 0) {
		const functionDeclarations = openAiTools
			.filter(tool => tool.type === 'function' && tool.function)
			.map(tool => {
                // Deep clone parameters to avoid modifying the original request object
                let parameters = tool.function.parameters ? JSON.parse(JSON.stringify(tool.function.parameters)) : undefined;
                // Remove the $schema field if it exists in the clone (legacy cleanup)
                if (parameters && parameters.$schema !== undefined) {
                    delete parameters.$schema;
                    console.log(`Removed '$schema' from parameters for tool: ${tool.function.name}`);
                }
                // Sanitize schema for Gemini compatibility
                if (parameters) {
                    try {
                        parameters = sanitizeToolParameters(parameters);
                    } catch (e) {
                        console.warn(`Failed to sanitize tool parameters for ${tool.function.name}: ${e?.message || e}`);
                    }
                    // Defensive: ensure root parameters has explicit type when object-like
                    if (parameters && !parameters.type && (parameters.properties || parameters.required || ('additionalProperties' in parameters))) {
                        parameters.type = 'object';
                    }
                }
				return {
					name: tool.function.name,
					description: tool.function.description,
					parameters: parameters
				};
			});

		if (functionDeclarations.length > 0) {
			geminiTools = [{ functionDeclarations }];
		}
	}

	// 3. Transform Tool Choice to Tool Config
	let toolConfig = undefined;
	if (openAiToolChoice && geminiTools && geminiTools.length > 0) {
		const functionCallingConfig = {};

		if (typeof openAiToolChoice === 'string') {
			switch (openAiToolChoice) {
				case 'auto':
					functionCallingConfig.mode = 'AUTO';
					break;
				case 'none':
					functionCallingConfig.mode = 'NONE';
					break;
				default:
					// If it's a string but not 'auto' or 'none', treat it as a specific function name
					functionCallingConfig.mode = 'ANY';
					functionCallingConfig.allowedFunctionNames = [openAiToolChoice];
					break;
			}
		} else if (typeof openAiToolChoice === 'object' && openAiToolChoice.type === 'function') {
			// Handle {"type": "function", "function": {"name": "function_name"}}
			const functionName = openAiToolChoice.function?.name;
			if (functionName) {
				functionCallingConfig.mode = 'ANY';
				functionCallingConfig.allowedFunctionNames = [functionName];
			} else {
				// Fallback to AUTO if function name is missing
				functionCallingConfig.mode = 'AUTO';
			}
		} else {
			// Default to AUTO for any other cases
			functionCallingConfig.mode = 'AUTO';
		}

		toolConfig = { functionCallingConfig };
		console.log(`Tool choice transformed: ${JSON.stringify(openAiToolChoice)} -> ${JSON.stringify(toolConfig)}`);
	}

	return { contents, systemInstruction, tools: geminiTools, toolConfig };
}


/**
 * Transforms a single Gemini API stream chunk into an OpenAI-compatible SSE chunk.
 * @param {object} geminiChunk - The parsed JSON object from a Gemini stream line.
 * @param {string} modelId - The model ID used for the request.
 * @returns {string | null} An OpenAI SSE data line string ("data: {...}\n\n") or null if chunk is empty/invalid.
 */
function transformGeminiStreamChunk(geminiChunk, modelId) {
	try {
		if (!geminiChunk || !geminiChunk.candidates || !geminiChunk.candidates.length) {
            // Ignore chunks that only contain usageMetadata (often appear at the end)
            if (geminiChunk?.usageMetadata) {
                return null;
            }
			console.warn("Received empty or invalid Gemini stream chunk:", JSON.stringify(geminiChunk));
			return null; // Skip empty/invalid chunks
		}

		const candidate = geminiChunk.candidates[0];
		let contentText = null;
		let toolCalls = undefined;

		// Extract text content and function calls
        if (candidate.content?.parts?.length > 0) {
            const textParts = candidate.content.parts.filter((part) => part.text !== undefined);
            const functionCallParts = candidate.content.parts.filter((part) => part.functionCall !== undefined);

            if (textParts.length > 0) {
                contentText = textParts.map((part) => part.text).join("");
            }

            if (functionCallParts.length > 0) {
                // Generate unique IDs for tool calls within the stream context if needed,
                // or use a simpler identifier if absolute uniqueness isn't critical across chunks.
                toolCalls = functionCallParts.map((part, index) => ({
                    index: index, // Gemini doesn't provide a stable index in stream AFAIK, use loop index
                    id: `call_${part.functionCall.name}_${Date.now()}_${index}`, // Example ID generation
                    type: "function",
                    function: {
                        name: part.functionCall.name,
                        // Arguments in Gemini stream might be partial JSON, attempt to stringify
                        arguments: JSON.stringify(part.functionCall.args || {}),
                    },
                }));
            }
        }

		// Determine finish reason mapping
		let finishReason = candidate.finishReason;
        if (finishReason === "STOP") finishReason = "stop";
        else if (finishReason === "MAX_TOKENS") finishReason = "length";
        else if (finishReason === "SAFETY" || finishReason === "RECITATION") finishReason = "content_filter";
        else if (finishReason === "TOOL_CALLS" || (toolCalls && toolCalls.length > 0 && finishReason !== 'stop' && finishReason !== 'length')) {
            // If there are tool calls and the reason isn't stop/length, map it to tool_calls
            finishReason = "tool_calls";
        } else if (finishReason && finishReason !== "FINISH_REASON_UNSPECIFIED" && finishReason !== "OTHER") {
            // Keep known reasons like 'stop', 'length', 'content_filter'
        } else {
            finishReason = null; // Map unspecified/other/null to null
        }


		// Construct the delta part for the OpenAI chunk
		const delta = {};
        // Include role only if there's actual content or tool calls in this chunk
        if (candidate.content?.role && (contentText !== null || (toolCalls && toolCalls.length > 0))) {
            delta.role = candidate.content.role === 'model' ? 'assistant' : candidate.content.role;
        }

        if (toolCalls && toolCalls.length > 0) {
            delta.tool_calls = toolCalls;
             // IMPORTANT: Explicitly set content to null if there are tool_calls but no text content in THIS chunk
             // This aligns with OpenAI's behavior where a chunk might contain only tool_calls.
            if (contentText === null) {
                delta.content = null;
            } else {
                 delta.content = contentText; // Include text if it also exists
            }
        } else if (contentText !== null) {
             // Only include content if there's text and no tool calls in this chunk
            delta.content = contentText;
        }


		// Only create a chunk if there's something meaningful to send
		if (Object.keys(delta).length === 0 && !finishReason) {
			return null;
		}

		const openaiChunk = {
			id: `chatcmpl-${Date.now()}-${Math.random().toString(36).substring(2, 8)}`, // More unique ID
			object: "chat.completion.chunk",
			created: Math.floor(Date.now() / 1000),
			model: modelId,
			choices: [
				{
					index: candidate.index || 0,
					delta: delta,
					finish_reason: finishReason, // Use the mapped finishReason
                    logprobs: null, // Not provided by Gemini
				},
			],
            // Usage is typically not included in stream chunks, only at the end if at all
		};

		return `data: ${JSON.stringify(openaiChunk)}\n\n`;

	} catch (e) {
		console.error("Error transforming Gemini stream chunk:", e, "Chunk:", JSON.stringify(geminiChunk));
        // Optionally return an error chunk
        const errorChunk = {
            id: `chatcmpl-error-${Date.now()}`,
            object: "chat.completion.chunk",
            created: Math.floor(Date.now() / 1000),
            model: modelId,
            choices: [{ index: 0, delta: { content: `[Error transforming chunk: ${e.message}]` }, finish_reason: 'error' }]
        };
        return `data: ${JSON.stringify(errorChunk)}\n\n`;
	}
}


/**
 * Transforms a complete (non-streaming) Gemini API response into an OpenAI-compatible format.
 * @param {object} geminiResponse - The parsed JSON object from the Gemini API response.
 * @param {string} modelId - The model ID used for the request.
 * @returns {string} A JSON string representing the OpenAI-compatible response.
 */
function transformGeminiResponseToOpenAI(geminiResponse, modelId) {
	try {
        // Handle cases where the response indicates an error (e.g., blocked prompt)
        if (!geminiResponse.candidates || geminiResponse.candidates.length === 0) {
            let errorMessage = "Gemini response missing candidates.";
            let finishReason = "error"; // Default error finish reason

            // Check for prompt feedback indicating blocking
            if (geminiResponse.promptFeedback?.blockReason) {
                errorMessage = `Request blocked by Gemini: ${geminiResponse.promptFeedback.blockReason}.`;
                finishReason = "content_filter"; // More specific finish reason
                 console.warn(`Gemini request blocked: ${geminiResponse.promptFeedback.blockReason}`, JSON.stringify(geminiResponse.promptFeedback));
            } else {
                console.error("Invalid Gemini response structure:", JSON.stringify(geminiResponse));
            }

            // Construct an error response in OpenAI format
            const errorResponse = {
                id: `chatcmpl-error-${Date.now()}`,
                object: "chat.completion",
                created: Math.floor(Date.now() / 1000),
                model: modelId,
                choices: [{
                    index: 0,
                    message: { role: "assistant", content: errorMessage },
                    finish_reason: finishReason,
                    logprobs: null,
                }],
                usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
            };
            return JSON.stringify(errorResponse);
        }


		const candidate = geminiResponse.candidates[0];
		let contentText = null;
		let toolCalls = undefined;

		// Extract content and tool calls
		if (candidate.content?.parts?.length > 0) {
            const textParts = candidate.content.parts.filter((part) => part.text !== undefined);
            const functionCallParts = candidate.content.parts.filter((part) => part.functionCall !== undefined);

            if (textParts.length > 0) {
                contentText = textParts.map((part) => part.text).join("");
            }

            if (functionCallParts.length > 0) {
                toolCalls = functionCallParts.map((part, index) => ({
                    id: `call_${part.functionCall.name}_${Date.now()}_${index}`, // Example ID
                    type: "function",
                    function: {
                        name: part.functionCall.name,
                        // Arguments should be a stringified JSON in OpenAI format
                        arguments: JSON.stringify(part.functionCall.args || {}),
                    },
                }));
            }
        }

		// Map finish reason
		let finishReason = candidate.finishReason;
        if (finishReason === "STOP") finishReason = "stop";
        else if (finishReason === "MAX_TOKENS") finishReason = "length";
        else if (finishReason === "SAFETY" || finishReason === "RECITATION") finishReason = "content_filter";
        else if (finishReason === "TOOL_CALLS") finishReason = "tool_calls"; // Explicitly check for TOOL_CALLS
        else if (toolCalls && toolCalls.length > 0) {
             // If tools were called but reason is not TOOL_CALLS (e.g., STOP), still map to tool_calls
            finishReason = "tool_calls";
        } else if (finishReason && finishReason !== "FINISH_REASON_UNSPECIFIED" && finishReason !== "OTHER") {
            // Keep known reasons
        } else {
             finishReason = null; // Map unspecified/other to null
        }

        // Handle cases where content might be missing due to safety ratings, even if finishReason isn't SAFETY
        if (contentText === null && !toolCalls && candidate.finishReason === "SAFETY") {
             console.warn("Gemini response finished due to SAFETY, content might be missing.");
             contentText = "[Content blocked due to safety settings]";
             finishReason = "content_filter";
        } else if (candidate.finishReason === "RECITATION") {
             console.warn("Gemini response finished due to RECITATION.");
             // contentText might exist but could be partial/problematic
             finishReason = "content_filter"; // Map recitation to content_filter
        }


		// Construct the OpenAI message object
		const message = { role: "assistant" };
        if (toolCalls && toolCalls.length > 0) {
             message.tool_calls = toolCalls;
             // IMPORTANT: Set content to null if only tool calls exist, otherwise include text
             message.content = contentText !== null ? contentText : null;
        } else {
             message.content = contentText; // Assign text content if no tool calls
        }
         // Ensure content is at least null if nothing else was generated
         if (message.content === undefined && !message.tool_calls) {
            message.content = null;
         }


		// Map usage metadata
		const usage = {
			prompt_tokens: geminiResponse.usageMetadata?.promptTokenCount || 0,
			completion_tokens: geminiResponse.usageMetadata?.candidatesTokenCount || 0, // Sum across candidates if multiple
			total_tokens: geminiResponse.usageMetadata?.totalTokenCount || 0,
		};

		// Construct the final OpenAI response object
		const openaiResponse = {
			id: `chatcmpl-${Date.now()}-${Math.random().toString(36).substring(2, 8)}`,
			object: "chat.completion",
			created: Math.floor(Date.now() / 1000),
			model: modelId,
			choices: [
				{
					index: candidate.index || 0,
					message: message,
					finish_reason: finishReason,
                    logprobs: null, // Not provided by Gemini
				},
			],
			usage: usage,
            // Include system fingerprint if available (though Gemini doesn't provide one)
            system_fingerprint: null
		};

		return JSON.stringify(openaiResponse);

	} catch (e) {
		console.error("Error transforming Gemini non-stream response:", e, "Response:", JSON.stringify(geminiResponse));
		// Return an error structure in OpenAI format
		const errorResponse = {
			id: `chatcmpl-error-${Date.now()}`,
			object: "chat.completion",
			created: Math.floor(Date.now() / 1000),
			model: modelId,
			choices: [{
				index: 0,
				message: { role: "assistant", content: `Error processing Gemini response: ${e.message}` },
				finish_reason: "error",
                logprobs: null,
			}],
			usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
		};
		return JSON.stringify(errorResponse);
	}
}

/**
 * Gemini Embedding API 응답 → OpenAI Embedding API 포맷 변환
 * @param {object} geminiResponse - Gemini 임베딩 엔드포인트 응답(JSON)
 * @param {string} modelId - OpenAI embedding compatible API의 모델 ID
 * @returns {object} OpenAI Embedding API 스펙 결과 or error 포함 구조
 */
function transformGeminiEmbeddingResponseToOpenAI(geminiResponse, modelId, originalInput) {
    // 1. 입력 유효성 검사
    function isInputValid(input) {
        if (typeof input === 'string') return input.trim().length >= 5;
        if (Array.isArray(input)) return input.some(x => typeof x === 'string' && x.trim().length >= 5);
        return false;
    }
    // 만약 originalInput이 인자로 전달된다면(권장)
    if (originalInput && !isInputValid(originalInput)) {
        return {
            object: "list",
            data: [],
            model: modelId,
            usage: { prompt_tokens: 0, total_tokens: 0 },
            error: { message: 'Input too short or lacks semantic content for embedding.' }
        };
    }

    // 2. Gemini 응답 컨버팅 및 구조 검사
    if (Array.isArray(geminiResponse.embeddings)) {
        const dataArr = geminiResponse.embeddings.map((emb, idx) => ({
            object: "embedding",
            embedding: Array.isArray(emb.values) ? emb.values : [],
            index: idx,
        }));
        if (dataArr.length === 0) {
            return {
                object: "list",
                data: [],
                model: modelId,
                usage: { prompt_tokens: 0, total_tokens: 0 },
                error: { message: "Gemini returned empty embeddings for this input." }
            };
        }
        return {
            object: "list",
            data: dataArr,
            model: modelId,
            usage: { prompt_tokens: 0, total_tokens: 0 }
        };
    }
    if (
        geminiResponse.embedding &&
        Array.isArray(geminiResponse.embedding.values)
    ) {
        const valuesArr = geminiResponse.embedding.values;
        if (valuesArr.length === 0) {
            return {
                object: "list",
                data: [],
                model: modelId,
                usage: { prompt_tokens: 0, total_tokens: 0 },
                error: { message: "Gemini returned empty embedding vector." }
            };
        }
        return {
            object: "list",
            data: [{
                object: "embedding",
                embedding: valuesArr,
                index: 0
            }],
            model: modelId,
            usage: { prompt_tokens: 0, total_tokens: 0 }
        };
    }
    // 3. 구조 불일치 등 기타 예외
    return {
        object: "list",
        data: [],
        model: modelId,
        usage: { prompt_tokens: 0, total_tokens: 0 },
        error: { message: "Invalid Gemini embedding response structure." }
    };
}



module.exports = {
    parseDataUri,
    sanitizeToolParameters,
    transformOpenAiToGemini,
    transformGeminiStreamChunk,
    transformGeminiResponseToOpenAI,
    transformGeminiEmbeddingResponseToOpenAI,
};
