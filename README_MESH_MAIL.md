# 📧 Mesh-Mail Module for Mesh-Memory

A modular mail system designed to integrate with the Mesh-Memory AI system, providing secure PIN-based mailboxes for mesh network users.

## 🎯 Features

- **PIN-based Security**: Each mailbox is protected by a unique PIN
- **SQLite Storage**: Reliable message storage with full-text search
- **Modular Design**: Clean separation between storage and AI layers
- **Intrusion Detection**: Failed access attempts are logged and reported
- **Flexible Querying**: Filter messages by sender, time, or keywords
- **Admin Controls**: Full system wipe capabilities for administrators

## 📁 Files

- `mesh_mail.py` - Core mail system with database operations
- `mesh_mail_integration.py` - Integration layer for Mesh-Memory commands
- `pins.json` - PIN to mailbox owner mapping
- `README_MESH_MAIL.md` - This documentation

## 🚀 Quick Start

### Basic Usage

```python
from mesh_mail import MeshMail

# Initialize the mail system
mail = MeshMail()

# Send a message
result = mail.new_mail("5678", "alice@mesh", "Hello! How are you?")
print(result)

# Check mail
messages = mail.check_mail("5678")
print(messages)
```

### Command Integration

```python
from mesh_mail_integration import MeshMailIntegration

# Initialize integration
mail_integration = MeshMailIntegration()

# Process commands
response = mail_integration.process_command("/mail", "5678", "user_node_123")
print(response)
```

## 📋 Database Schema

### Messages Table
```sql
CREATE TABLE messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sender TEXT NOT NULL,
    recipient TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    body TEXT NOT NULL,
    read_status INTEGER DEFAULT 0
);
```

### Intrusion Attempts Table
```sql
CREATE TABLE intrusion_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pin TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    attempt_info TEXT NOT NULL
);
```

## 🔐 PIN Management

The `pins.json` file maps PINs to mailbox owners:

```json
{
  "1234": "admin",
  "5678": "alice_mesh",
  "9012": "bob_mesh"
}
```

## 🎮 Available Commands

### `/mail <PIN> [options]`
Check mailbox messages with optional filters:
- `limit=N` - Number of messages to retrieve (default: 3)
- `sender=X` - Filter by sender
- `keywords=Y` - Search message content
- `hours=Z` - Only messages from last Z hours

**Examples:**
```
/mail 5678
/mail 5678 limit=5
/mail 5678 sender=alice@mesh
/mail 5678 keywords=urgent hours=24
```

### `/sendmail <PIN> <message>`
Send a message to a mailbox:
```
/sendmail 5678 Hello! Meeting at 3PM today.
```

### `/mailstats <PIN>`
Get mailbox statistics:
```
/mailstats 5678
```

### `/wipemail <PIN> [admin]`
Delete messages:
```
/wipemail 5678        # Delete user's messages
/wipemail 1234 admin  # Admin: delete ALL messages
```

## 🔧 Core Functions

### `new_mail(pin, sender, body)`
Insert a new message for the recipient linked to the PIN.

**Parameters:**
- `pin` (str): Recipient's PIN
- `sender` (str): Message sender identifier  
- `body` (str): Message content

**Returns:**
```python
{
    "status": "success|error",
    "message": "Status message",
    "mail_id": 123,
    "timestamp": "2025-09-25 12:34:56 UTC"
}
```

### `check_mail(pin, query=None)`
Retrieve messages for a mailbox with optional filtering.

**Parameters:**
- `pin` (str): User's PIN
- `query` (dict, optional): Filter options
  - `limit` (int): Number of messages (default: 3)
  - `sender` (str): Filter by sender
  - `hours_back` (int): Only messages from last N hours
  - `keywords` (str): Search in message body

**Returns:**
```python
{
    "status": "success|error",
    "message": "Status message",
    "messages": [
        {
            "id": 1,
            "sender": "alice@mesh",
            "timestamp": "2025-09-25 12:34:56 UTC",
            "body": "Message content",
            "read": false
        }
    ],
    "total_found": 1
}
```

### `notify_intrusion(pin, attempt_info)`
Record a failed access attempt and return security warning.

**Parameters:**
- `pin` (str): PIN that was attempted
- `attempt_info` (str): Information about the attempt

**Returns:**
```python
{
    "status": "intrusion_detected",
    "warning_level": "LOW|MEDIUM|HIGH",
    "message": "Warning message",
    "timestamp": "2025-09-25 12:34:56 UTC",
    "recent_attempts": 3
}
```

### `wipe_mail(pin, scope='user')`
Delete messages based on scope and permissions.

**Parameters:**
- `pin` (str): User's PIN
- `scope` (str): 'user' (delete user's mail) or 'admin' (delete all mail)

**Returns:**
```python
{
    "status": "success|error",
    "message": "Operation result",
    "deleted_count": 5
}
```

## 🧪 Testing

Run the built-in test suite:

```bash
python3 mesh_mail.py
```

Test the integration layer:

```bash
python3 mesh_mail_integration.py
```

## 🔗 Integration with Mesh-Memory

To integrate with your existing Mesh-Memory system, add this to your main `mesh-ai.py`:

```python
from mesh_mail_integration import MeshMailIntegration

# Initialize during startup
mail_integration = MeshMailIntegration()

# In your handle_command function, add:
def handle_command(cmd, full_text, sender_id, is_direct=False, channel_idx=None):
    # ... existing code ...
    
    # Check for mail commands
    args = full_text[len(cmd):].strip()
    mail_response = mail_integration.process_command(cmd, args, sender_id)
    if mail_response:
        return mail_response
    
    # ... continue with existing commands ...
```

## 🛡️ Security Features

- **PIN Protection**: All mailboxes require valid PINs
- **Intrusion Logging**: Failed access attempts are recorded
- **Warning Levels**: Escalating security alerts based on attempt frequency
- **Admin Scope**: Administrative functions require admin PIN
- **Input Validation**: All inputs are sanitized and validated

## 📊 Performance

- **SQLite Indexes**: Optimized queries on recipient, timestamp, and sender
- **Configurable Limits**: Default 3-message limit prevents resource abuse
- **Efficient Storage**: Minimal schema design for fast operations
- **Connection Pooling**: Proper database connection management

## 🎯 Future Enhancements

The modular design allows for easy addition of:
- **AI Integration**: Natural language mail queries using existing Mesh-Memory AI
- **Encryption**: End-to-end message encryption
- **Attachments**: File attachment support
- **Threading**: Message threading and conversations
- **Notifications**: Real-time mesh network notifications
- **Web Interface**: Browser-based mail management

## 📝 Example Use Cases

1. **Emergency Communications**: Send important messages that persist until read
2. **Mesh Network Coordination**: Leave messages for offline users
3. **Private Messaging**: Secure PIN-based communication
4. **Network Administration**: Admin broadcasts and system messages
5. **Community Bulletin**: Shared information exchange

---

**Ready to enhance your Mesh-Memory system with secure, persistent messaging!** 📧✨