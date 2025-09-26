#!/usr/bin/env python3
"""
Mesh-Mail Integration for Mesh-Memory System
Provides command handlers and integration points for the mail system.
"""

from mesh_mail import MeshMail
import json
from typing import Dict, Any, Optional

class MeshMailIntegration:
    """Integration layer between Mesh-Memory and Mesh-Mail systems."""
    
    def __init__(self, db_path: str = "mesh_mail.db", pins_file: str = "pins.json"):
        """Initialize the mail integration."""
        self.mail_system = MeshMail(db_path, pins_file)
        
        # Command mappings for Mesh-Memory integration
        self.commands = {
            "/mail": self.handle_mail_command,
            "/sendmail": self.handle_sendmail_command,
            "/mailstats": self.handle_mailstats_command,
            "/wipemail": self.handle_wipemail_command
        }
    
    def handle_mail_command(self, args: str, sender_id: str) -> str:
        """
        Handle /mail command - check mail with optional filters.
        Usage: /mail <PIN> [limit=N] [sender=X] [keywords=Y] [hours=Z]
        """
        if not args.strip():
            return "📧 Usage: /mail <PIN> [limit=N] [sender=X] [keywords=Y] [hours=Z]"
        
        parts = args.strip().split()
        if len(parts) < 1:
            return "❌ PIN required for mail access"
        
        pin = parts[0]
        query = {}
        
        # Parse optional parameters
        for part in parts[1:]:
            if '=' in part:
                key, value = part.split('=', 1)
                if key == 'limit':
                    try:
                        query['limit'] = int(value)
                    except ValueError:
                        return "❌ Invalid limit value"
                elif key == 'sender':
                    query['sender'] = value
                elif key == 'keywords':
                    query['keywords'] = value
                elif key == 'hours':
                    try:
                        query['hours_back'] = int(value)
                    except ValueError:
                        return "❌ Invalid hours value"
        
        result = self.mail_system.check_mail(pin, query if query else None)
        
        if result['status'] != 'success':
            # Log intrusion attempt
            self.mail_system.notify_intrusion(pin, f"Mail access attempt from {sender_id}")
            return f"❌ {result['message']}"
        
        if not result['messages']:
            return "📭 No mail found."
        
        # Format messages for display
        response = f"📧 Mail for your mailbox ({result['total_found']} messages):\n\n"
        
        for i, msg in enumerate(result['messages'], 1):
            read_indicator = "📖" if msg['read'] else "📩"
            response += f"{read_indicator} #{msg['id']} From: {msg['sender']}\n"
            response += f"   Time: {msg['timestamp']}\n"
            response += f"   Message: {msg['body'][:100]}{'...' if len(msg['body']) > 100 else ''}\n\n"
        
        return response.strip()
    
    def handle_sendmail_command(self, args: str, sender_id: str) -> str:
        """
        Handle /sendmail command - send mail to a PIN.
        Usage: /sendmail <PIN> <message>
        """
        if not args.strip():
            return "📤 Usage: /sendmail <PIN> <message>"
        
        parts = args.strip().split(' ', 1)
        if len(parts) < 2:
            return "❌ Both PIN and message required"
        
        pin, message = parts
        result = self.mail_system.new_mail(pin, sender_id, message)
        
        if result['status'] == 'success':
            return f"✅ Mail delivered! Message ID: {result['mail_id']}"
        else:
            return f"❌ Failed to send: {result['message']}"
    
    def handle_mailstats_command(self, args: str, sender_id: str) -> str:
        """
        Handle /mailstats command - get mailbox statistics.
        Usage: /mailstats <PIN>
        """
        if not args.strip():
            return "📊 Usage: /mailstats <PIN>"
        
        pin = args.strip()
        result = self.mail_system.get_stats(pin)
        
        if result['status'] != 'success':
            self.mail_system.notify_intrusion(pin, f"Stats access attempt from {sender_id}")
            return f"❌ {result['message']}"
        
        return f"📊 Mailbox Stats for {result['mailbox']}:\n" \
               f"   📧 Total messages: {result['total_messages']}\n" \
               f"   📩 Unread messages: {result['unread_messages']}\n" \
               f"   🕐 Recent (24h): {result['recent_messages_24h']}"
    
    def handle_wipemail_command(self, args: str, sender_id: str) -> str:
        """
        Handle /wipemail command - wipe mailbox.
        Usage: /wipemail <PIN> [admin]
        """
        if not args.strip():
            return "🗑️ Usage: /wipemail <PIN> [admin]"
        
        parts = args.strip().split()
        pin = parts[0]
        scope = 'admin' if len(parts) > 1 and parts[1] == 'admin' else 'user'
        
        result = self.mail_system.wipe_mail(pin, scope)
        
        if result['status'] == 'success':
            return f"✅ {result['message']}"
        else:
            return f"❌ {result['message']}"
    
    def process_command(self, command: str, args: str, sender_id: str) -> Optional[str]:
        """
        Process a mail-related command.
        
        Args:
            command: The command (e.g., "/mail")
            args: Command arguments
            sender_id: ID of the sender
            
        Returns:
            Response string or None if command not handled
        """
        handler = self.commands.get(command.lower())
        if handler:
            try:
                return handler(args, sender_id)
            except Exception as e:
                return f"❌ Mail system error: {e}"
        return None
    
    def auto_notify_new_mail(self, pin: str) -> Optional[str]:
        """
        Check for new mail and return notification if any.
        Can be called periodically or on mesh events.
        """
        try:
            result = self.mail_system.check_mail(pin, {"limit": 1})
            if result['status'] == 'success' and result['messages']:
                latest = result['messages'][0]
                if not latest['read']:  # New unread message
                    return f"📧 New mail from {latest['sender']}: {latest['body'][:50]}..."
        except:
            pass
        return None


# Example integration into existing Mesh-Memory command handler
def integrate_with_mesh_memory():
    """
    Example of how to integrate Mesh-Mail with existing Mesh-Memory system.
    This would be added to your main mesh-ai.py file.
    """
    
    # Initialize mail integration
    mail_integration = MeshMailIntegration()
    
    # Example: Add to your existing handle_command function
    def enhanced_handle_command(cmd, full_text, sender_id, is_direct=False, channel_idx=None, thread_root_ts=None):
        """Enhanced command handler with mail support."""
        
        # Extract arguments (everything after the command)
        args = full_text[len(cmd):].strip()
        
        # Try mail system first
        mail_response = mail_integration.process_command(cmd, args, sender_id)
        if mail_response:
            return mail_response
        
        # ... continue with existing command handling ...
        # Your existing commands like /about, /help, etc.
        
        return "Command not found"
    
    # Example: Periodic mail notifications (add to your main loop)
    def check_mail_notifications():
        """Check for new mail notifications."""
        # This could be called periodically or triggered by mesh events
        
        # Example: Check mail for all known PINs
        pins = {"1234": "admin", "5678": "user1", "9012": "user2"}  # Load from pins.json
        
        for pin, user in pins.items():
            notification = mail_integration.auto_notify_new_mail(pin)
            if notification:
                # Send notification to mesh network
                print(f"Mail notification for {user}: {notification}")
                # You could send this as a mesh message or show in UI


if __name__ == "__main__":
    # Test the integration
    print("🧪 Testing Mesh-Mail Integration...")
    
    integration = MeshMailIntegration(db_path="test_integration.db", pins_file="test_integration_pins.json")
    
    # Test commands
    print("\n📤 Testing sendmail command:")
    result = integration.process_command("/sendmail", "5678 Hello from integration test!", "test_sender")
    print(f"   Result: {result}")
    
    print("\n📧 Testing mail command:")
    result = integration.process_command("/mail", "5678", "test_sender")
    print(f"   Result: {result}")
    
    print("\n📊 Testing mailstats command:")
    result = integration.process_command("/mailstats", "5678", "test_sender")
    print(f"   Result: {result}")
    
    print("\n✅ Integration test complete!")
    
    # Cleanup
    import os
    try:
        os.remove("test_integration.db")
        os.remove("test_integration_pins.json")
        print("🧹 Test files cleaned up.")
    except:
        pass