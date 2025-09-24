# MESH-AI Performance Optimizations

## Summary of Changes

The message buffer performance issues have been addressed through several key optimizations:

### 1. Enhanced Queue System
- **Separate Priority Queues**: Split into DM queue (higher priority, 20 slots) and channel queue (15 slots)
- **Priority Processing**: Direct messages processed before channel messages
- **Larger Queue Capacity**: Increased from 10 to 35 total slots across both queues
- **Deduplication**: Prevents processing duplicate requests from same sender

### 2. Message Processing Optimizations
- **Smart Request Filtering**: Duplicates detected and skipped within 5-minute window
- **Timeout Reduction**: Ollama timeout reduced from 120s to 45s for faster failures
- **Response Caching**: DM responses cached for 5 minutes to handle repeated questions
- **Async Processing**: Non-blocking message reception with background AI processing

### 3. AI Provider Optimizations (Ollama)
- **Reduced Context Window**: Decreased from 1536 to 1024 tokens
- **Shorter Response Limit**: Reduced from 200 to 150 tokens max
- **Optimized Parameters**:
  - Lower temperature (0.5) for faster, more focused responses
  - Smaller top_k (30) and top_p (0.8) for speed
  - CPU-only processing for predictable performance
  - Memory mapping enabled for efficiency

### 4. History Building Improvements
- **Time-based Filtering**: Only include messages from last 2 hours
- **Early Termination**: Stop processing when enough messages found
- **Character Budget**: Build history within character limits progressively
- **Smart Filtering**: Skip system messages and very short messages

### 5. Memory Management
- **Periodic Cleanup**: Remove expired pending requests and cache entries
- **Cache Size Limits**: Maximum 20 cached responses, auto-cleanup old entries
- **Message Window**: Only process last 50 messages for history building

### 6. Performance Monitoring
- **Queue Status Endpoint**: `/queue_status` API for monitoring system health
- **Enhanced Logging**: Better visibility into processing times and queue sizes
- **Error Handling**: Graceful degradation when queues are full

## Configuration Changes

Updated `config.json` with optimized settings:
```json
{
  "ollama_timeout": 45,           // Reduced from 120
  "ollama_context_chars": 2000,   // Reduced from 4000  
  "ollama_num_ctx": 1024,         // Reduced from 1536
  "ollama_max_messages": 6,       // Reduced from 8
  "system_prompt": "Be concise. Max 2 sentences. Mesh network."
}
```

## Expected Improvements

1. **Faster Response Times**: 
   - DM responses prioritized over channel messages
   - Reduced AI processing time through optimized parameters
   - Cached responses for repeated questions

2. **Better Message Handling**:
   - No more dropped messages due to larger, prioritized queues
   - Duplicate detection prevents wasted processing
   - Graceful fallback when queues are full

3. **Resource Efficiency**:
   - Lower memory usage through cleanup routines
   - Reduced context processing overhead
   - CPU-optimized AI parameters

4. **System Stability**:
   - Better error handling and recovery
   - Monitoring capabilities for troubleshooting
   - Automatic cleanup of stale requests

## Usage Notes

- Monitor system performance via `/queue_status` endpoint
- DM responses will be noticeably faster than channel responses
- Repeated questions within 5 minutes will return cached responses
- System automatically cleans up resources every minute

## Troubleshooting

If messages are still being dropped:
1. Check `/queue_status` for queue sizes
2. Increase queue sizes in the queue initialization
3. Consider reducing `ollama_max_messages` further for faster processing
4. Monitor logs for "[AsyncAI] Queue full" messages

The system should now handle concurrent DMs and channel messages much more efficiently while preventing message drops.