#!/usr/bin/env python3
"""
Mesh-Mail Module for Mesh-Memory System
A modular mail system for mesh networks with PIN-based access control.
"""

import sqlite3
import json
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
import logging

class MeshMail:
    def __init__(self, db_path: str = "mesh_mail.db", pins_file: str = "pins.json"):
        """
        Initialize Mesh-Mail system.
        
        Args:
            db_path: Path to SQLite database file
            pins_file: Path to PIN mapping JSON file
        """
        self.db_path = db_path
        self.pins_file = pins_file
        self.logger = logging.getLogger(__name__)
        
        # Initialize database and pins
        self._init_database()
        self._init_pins()
    
    def _init_database(self):
        """Initialize SQLite database with required tables."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Messages table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS messages (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        sender TEXT NOT NULL,
                        recipient TEXT NOT NULL,
                        timestamp TEXT NOT NULL,
                        body TEXT NOT NULL,
                        read_status INTEGER DEFAULT 0
                    )
                ''')
                
                # Intrusion attempts table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS intrusion_attempts (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        pin TEXT NOT NULL,
                        timestamp TEXT NOT NULL,
                        attempt_info TEXT NOT NULL
                    )
                ''')
                
                # Create indexes for better performance
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_recipient ON messages(recipient)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON messages(timestamp)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_sender ON messages(sender)')
                
                conn.commit()
                self.logger.info("Database initialized successfully")
                
        except Exception as e:
            self.logger.error(f"Failed to initialize database: {e}")
            raise
    
    def _init_pins(self):
        """Initialize pins.json file if it doesn't exist."""
        if not os.path.exists(self.pins_file):
            default_pins = {
                "1234": "admin",
                "5678": "user1",
                "9012": "user2"
            }
            try:
                with open(self.pins_file, 'w') as f:
                    json.dump(default_pins, f, indent=2)
                self.logger.info(f"Created default {self.pins_file}")
            except Exception as e:
                self.logger.error(f"Failed to create pins file: {e}")
                raise
    
    def _load_pins(self) -> Dict[str, str]:
        """Load PIN to mailbox owner mapping."""
        try:
            with open(self.pins_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Failed to load pins: {e}")
            return {}
    
    def _get_mailbox_owner(self, pin: str) -> Optional[str]:
        """Get mailbox owner for given PIN."""
        pins = self._load_pins()
        return pins.get(pin)
    
    def _format_timestamp(self, dt: datetime = None) -> str:
        """Format timestamp for storage."""
        if dt is None:
            dt = datetime.utcnow()
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    
    def new_mail(self, pin: str, sender: str, body: str) -> Dict[str, Any]:
        """
        Insert a new message for recipient linked to the PIN.
        
        Args:
            pin: Recipient's PIN
            sender: Message sender identifier
            body: Message content
            
        Returns:
            Dict with status and message info
        """
        recipient = self._get_mailbox_owner(pin)
        if not recipient:
            return {
                "status": "error",
                "message": "Invalid PIN - mailbox not found",
                "mail_id": None
            }
        
        timestamp = self._format_timestamp()
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO messages (sender, recipient, timestamp, body)
                    VALUES (?, ?, ?, ?)
                ''', (sender, recipient, timestamp, body))
                
                mail_id = cursor.lastrowid
                conn.commit()
                
                return {
                    "status": "success",
                    "message": f"Mail delivered to {recipient}",
                    "mail_id": mail_id,
                    "timestamp": timestamp
                }
                
        except Exception as e:
            self.logger.error(f"Failed to insert new mail: {e}")
            return {
                "status": "error",
                "message": f"Failed to deliver mail: {e}",
                "mail_id": None
            }
    
    def check_mail(self, pin: str, query: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Return messages for the mailbox linked to the PIN.
        
        Args:
            pin: User's PIN
            query: Optional filters (limit, sender, time_range, keywords)
                  - limit: Number of messages (default: 3)
                  - sender: Filter by sender
                  - hours_back: Only messages from last N hours
                  - keywords: Search in message body
                  
        Returns:
            Dict with status and messages
        """
        recipient = self._get_mailbox_owner(pin)
        if not recipient:
            return {
                "status": "error",
                "message": "Invalid PIN - mailbox not found",
                "messages": []
            }
        
        # Default query parameters
        limit = 3
        sender_filter = None
        hours_back = None
        keywords = None
        
        if query:
            limit = query.get('limit', 3)
            sender_filter = query.get('sender')
            hours_back = query.get('hours_back')
            keywords = query.get('keywords')
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Build query
                sql = "SELECT id, sender, timestamp, body, read_status FROM messages WHERE recipient = ?"
                params = [recipient]
                
                # Add filters
                if sender_filter:
                    sql += " AND sender = ?"
                    params.append(sender_filter)
                
                if hours_back:
                    cutoff_time = datetime.utcnow() - timedelta(hours=hours_back)
                    sql += " AND timestamp >= ?"
                    params.append(self._format_timestamp(cutoff_time))
                
                if keywords:
                    sql += " AND body LIKE ?"
                    params.append(f"%{keywords}%")
                
                sql += " ORDER BY timestamp DESC LIMIT ?"
                params.append(limit)
                
                cursor.execute(sql, params)
                rows = cursor.fetchall()
                
                messages = []
                for row in rows:
                    messages.append({
                        "id": row[0],
                        "sender": row[1],
                        "timestamp": row[2],
                        "body": row[3],
                        "read": bool(row[4])
                    })
                
                return {
                    "status": "success",
                    "message": f"Retrieved {len(messages)} messages for {recipient}",
                    "messages": messages,
                    "total_found": len(messages)
                }
                
        except Exception as e:
            self.logger.error(f"Failed to check mail: {e}")
            return {
                "status": "error",
                "message": f"Failed to retrieve mail: {e}",
                "messages": []
            }
    
    def notify_intrusion(self, pin: str, attempt_info: str) -> Dict[str, Any]:
        """
        Record a failed access attempt and return warning.
        
        Args:
            pin: PIN that was attempted
            attempt_info: Information about the attempt
            
        Returns:
            Dict with warning information
        """
        timestamp = self._format_timestamp()
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO intrusion_attempts (pin, timestamp, attempt_info)
                    VALUES (?, ?, ?)
                ''', (pin, timestamp, attempt_info))
                
                # Get recent attempts for this PIN
                cursor.execute('''
                    SELECT COUNT(*) FROM intrusion_attempts 
                    WHERE pin = ? AND timestamp >= ?
                ''', (pin, self._format_timestamp(datetime.utcnow() - timedelta(hours=1))))
                
                recent_attempts = cursor.fetchone()[0]
                conn.commit()
                
                warning_level = "LOW"
                if recent_attempts >= 5:
                    warning_level = "HIGH"
                elif recent_attempts >= 3:
                    warning_level = "MEDIUM"
                
                return {
                    "status": "intrusion_detected",
                    "warning_level": warning_level,
                    "message": f"Unauthorized access attempt recorded. {recent_attempts} attempts in last hour.",
                    "timestamp": timestamp,
                    "recent_attempts": recent_attempts
                }
                
        except Exception as e:
            self.logger.error(f"Failed to record intrusion: {e}")
            return {
                "status": "error",
                "message": f"Failed to record intrusion: {e}",
                "warning_level": "UNKNOWN"
            }
    
    def wipe_mail(self, pin: str, scope: str = 'user') -> Dict[str, Any]:
        """
        Delete messages based on scope.
        
        Args:
            pin: User's PIN (must be valid for user scope)
            scope: 'user' (delete user's mail) or 'admin' (delete all mail)
            
        Returns:
            Dict with operation status
        """
        if scope == 'user':
            recipient = self._get_mailbox_owner(pin)
            if not recipient:
                return {
                    "status": "error",
                    "message": "Invalid PIN - cannot wipe mail",
                    "deleted_count": 0
                }
        elif scope == 'admin':
            # For admin scope, check if PIN belongs to admin
            recipient = self._get_mailbox_owner(pin)
            if recipient != 'admin':
                return {
                    "status": "error",
                    "message": "Admin privileges required for full wipe",
                    "deleted_count": 0
                }
        else:
            return {
                "status": "error",
                "message": "Invalid scope - use 'user' or 'admin'",
                "deleted_count": 0
            }
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                if scope == 'user':
                    cursor.execute('DELETE FROM messages WHERE recipient = ?', (recipient,))
                    deleted_count = cursor.rowcount
                    message = f"Wiped {deleted_count} messages for {recipient}"
                elif scope == 'admin':
                    cursor.execute('DELETE FROM messages')
                    deleted_count = cursor.rowcount
                    cursor.execute('DELETE FROM intrusion_attempts')
                    deleted_intrusions = cursor.rowcount
                    message = f"Admin wipe: {deleted_count} messages, {deleted_intrusions} intrusion records"
                
                conn.commit()
                
                return {
                    "status": "success",
                    "message": message,
                    "deleted_count": deleted_count
                }
                
        except Exception as e:
            self.logger.error(f"Failed to wipe mail: {e}")
            return {
                "status": "error",
                "message": f"Failed to wipe mail: {e}",
                "deleted_count": 0
            }
    
    def get_stats(self, pin: str) -> Dict[str, Any]:
        """Get mailbox statistics (bonus function)."""
        recipient = self._get_mailbox_owner(pin)
        if not recipient:
            return {"status": "error", "message": "Invalid PIN"}
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Total messages
                cursor.execute('SELECT COUNT(*) FROM messages WHERE recipient = ?', (recipient,))
                total_messages = cursor.fetchone()[0]
                
                # Unread messages
                cursor.execute('SELECT COUNT(*) FROM messages WHERE recipient = ? AND read_status = 0', (recipient,))
                unread_messages = cursor.fetchone()[0]
                
                # Recent messages (last 24 hours)
                cutoff = self._format_timestamp(datetime.utcnow() - timedelta(hours=24))
                cursor.execute('SELECT COUNT(*) FROM messages WHERE recipient = ? AND timestamp >= ?', (recipient, cutoff))
                recent_messages = cursor.fetchone()[0]
                
                return {
                    "status": "success",
                    "mailbox": recipient,
                    "total_messages": total_messages,
                    "unread_messages": unread_messages,
                    "recent_messages_24h": recent_messages
                }
                
        except Exception as e:
            return {"status": "error", "message": f"Failed to get stats: {e}"}


def run_test_suite():
    """Test suite to verify Mesh-Mail functionality."""
    print("🚀 Running Mesh-Mail Test Suite...")
    
    # Initialize system
    mail_system = MeshMail(db_path="test_mesh_mail.db", pins_file="test_pins.json")
    
    print("\n📧 Test 1: Sending new mail...")
    result = mail_system.new_mail("5678", "alice@mesh", "Hello user1! How are you doing on the mesh?")
    print(f"   Result: {result}")
    
    result = mail_system.new_mail("5678", "bob@mesh", "Don't forget about the mesh meetup tomorrow!")
    print(f"   Result: {result}")
    
    result = mail_system.new_mail("9012", "alice@mesh", "Hi user2! Welcome to the mesh network.")
    print(f"   Result: {result}")
    
    result = mail_system.new_mail("0000", "spam@mesh", "This should fail - invalid PIN")
    print(f"   Result: {result}")
    
    print("\n📬 Test 2: Checking mail...")
    result = mail_system.check_mail("5678")
    print(f"   user1 mail: {result}")
    
    result = mail_system.check_mail("9012", {"limit": 1})
    print(f"   user2 mail (limit 1): {result}")
    
    print("\n🔍 Test 3: Filtered mail queries...")
    result = mail_system.check_mail("5678", {"sender": "alice@mesh"})
    print(f"   user1 mail from alice: {result}")
    
    result = mail_system.check_mail("5678", {"keywords": "meetup"})
    print(f"   user1 mail with 'meetup': {result}")
    
    print("\n🚨 Test 4: Intrusion attempts...")
    result = mail_system.notify_intrusion("0000", "Failed PIN attempt from node_123")
    print(f"   Intrusion 1: {result}")
    
    result = mail_system.notify_intrusion("0000", "Another failed attempt from node_456")
    print(f"   Intrusion 2: {result}")
    
    # Simulate multiple attempts
    for i in range(3):
        result = mail_system.notify_intrusion("0000", f"Brute force attempt #{i+3}")
    print(f"   Final intrusion result: {result}")
    
    print("\n📊 Test 5: Mail statistics...")
    result = mail_system.get_stats("5678")
    print(f"   user1 stats: {result}")
    
    result = mail_system.get_stats("1234")
    print(f"   admin stats: {result}")
    
    print("\n🗑️ Test 6: Mail wiping...")
    result = mail_system.wipe_mail("9012", "user")
    print(f"   user2 wipe: {result}")
    
    result = mail_system.wipe_mail("5678", "admin")  # Should fail - not admin
    print(f"   user1 admin wipe (should fail): {result}")
    
    result = mail_system.wipe_mail("1234", "admin")  # Should work - admin PIN
    print(f"   admin full wipe: {result}")
    
    print("\n✅ Mesh-Mail Test Suite Complete!")
    
    # Cleanup test files
    import os
    try:
        os.remove("test_mesh_mail.db")
        os.remove("test_pins.json")
        print("🧹 Test files cleaned up.")
    except:
        pass


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Run tests
    run_test_suite()