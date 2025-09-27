# Clean Logging Enhancement for Mesh Master

## Overview
The logging system has been enhanced with emoji-based, human-friendly output that hides technical details when not needed.

## Configuration Options

In `config.json`, you now have two logging-related options:

```json
{
  "debug": false,        // Show verbose technical details (original behavior)
  "clean_logs": true,    // Enable emoji-enhanced clean logging
  // ... other config options
}
```

## Logging Modes

### 1. Debug Mode (debug: true)
- Shows all technical details including raw AI responses, token counts, timing data
- Useful for troubleshooting and development
- Example output: `Ollama raw => {'model': 'llama3.2:latest', 'created_at': '...', 'response': 'Hello!', 'done': True, 'context': [128006, 9125, ...], 'total_duration': 18913918638, 'prompt_eval_count': 117, 'eval_count': 9}`

### 2. Clean Mode (debug: false, clean_logs: true) - DEFAULT
- Human-friendly emoji-enhanced messages
- Hides technical clutter like token counts, raw JSON responses
- Perfect for daily use and monitoring
- Example output:
  ```
  🚀 Starting Mesh Master server...
  🌐 Launching Flask web interface on port 5000...
  🔗 Connecting to Meshtastic device...
  🟢 Connection successful! Running until error or Ctrl+C.
  📨 Message from NodeABC (Ch3): Hello AI!
  🦙 OLLAMA: Processing message...
  💭 Prompt: Hello AI!
  🦙 OLLAMA: Response: Hi there! How can I help you today?
  📡 Broadcasting on Ch3: Hi there! How can I help you today?
  ```

### 3. Simple Mode (debug: false, clean_logs: false)
- Basic informational messages without emojis
- Good for environments where emojis aren't supported
- Example output:
  ```
  [Info] Starting Mesh Master server...
  [Info] Launching Flask web interface on port 5000...
  [OLLAMA] Processing message...
  [OLLAMA] Response: Hi there! How can I help you today?
  ```

## Emoji Legend

| Emoji | Meaning |
|-------|---------|
| 🚀 | System startup |
| 🌐 | Web interface |
| 🔗 | Connecting |
| 🟢 | Success/Connected |
| 🔄 | Reconnecting |
| 📨 | Incoming message |
| 📡 | Broadcasting message |
| 📤 | Direct message |
| ⚡ | Processing queue |
| ✅ | Task completed |
| 🎯 | Response delivered |
| 💭 | AI prompt |
| 🦙 | Ollama AI |
| 🏠 | Home Assistant |

## Benefits

1. **Less Clutter**: No more overwhelming technical details in normal operation
2. **Better Readability**: Emojis provide quick visual cues for different operations
3. **Faster Troubleshooting**: Important events are easy to spot at a glance
4. **Configurable**: Switch between modes based on your needs
5. **Backwards Compatible**: Debug mode preserves all original verbose output

## Migration Note

If you're upgrading from an older version:
- Your existing `debug: true/false` setting will continue to work
- Add `"clean_logs": true` to your config.json to enable the new emoji logging
- The system defaults to clean logging if the option isn't specified
