import os
import logging
import sqlite3
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from dotenv import load_dotenv
import csv
from io import StringIO, BytesIO
import gspread
from google.oauth2.service_account import Credentials
import json
import asyncio

load_dotenv()

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Google Sheets setup
def setup_google_sheets():
    """Setup Google Sheets client"""
    try:
        if 'GOOGLE_CREDENTIALS' in os.environ:
            logger.info("Using production Google credentials from environment variable")
            creds_json = json.loads(os.environ['GOOGLE_CREDENTIALS'])
            credentials = Credentials.from_service_account_info(
                creds_json,
                scopes=['https://www.googleapis.com/auth/spreadsheets']
            )
            gc = gspread.authorize(credentials)
        else:
            logger.info("Using local credentials.json file")
            gc = gspread.service_account(filename='credentials.json')
        
        return gc
    except Exception as e:
        logger.error(f"Error setting up Google Sheets: {e}")
        return None

class ActivityTracker:
    def __init__(self, db_path, super_admin_id, backup_sheet_id=None):
        self.db_path = db_path
        self.super_admin_id = int(super_admin_id)
        self.backup_sheet_id = backup_sheet_id
        self.gc = setup_google_sheets() if backup_sheet_id else None
        self.backup_sheet = None
        self.setup_database()
        self.setup_backup_sheet()
        
    def setup_database(self):
        """Initialize SQLite database with proper schema"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create activity table (removed char_count)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS activity (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    username TEXT,
                    first_name TEXT,
                    timestamp DATETIME NOT NULL,
                    message_type TEXT,
                    date TEXT NOT NULL,
                    hour INTEGER NOT NULL
                )
            ''')
            
            # Create indexes
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_chat_id ON activity(chat_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_id ON activity(user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_date ON activity(date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON activity(timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_chat_user ON activity(chat_id, user_id)')
            
            # Create groups table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS groups (
                    chat_id INTEGER PRIMARY KEY,
                    group_name TEXT,
                    status TEXT DEFAULT 'pending',
                    trial_end_date DATETIME,
                    subscription_end_date DATETIME,
                    added_date DATETIME NOT NULL
                )
            ''')
            
            # Create group admins table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS group_admins (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    role TEXT DEFAULT 'admin',
                    added_date DATETIME NOT NULL,
                    UNIQUE(chat_id, user_id)
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("Database setup completed")
        except Exception as e:
            logger.error(f"Database setup failed: {e}")
            raise
    
    def setup_backup_sheet(self):
        """Setup Google Sheets backup"""
        if not self.gc or not self.backup_sheet_id:
            return
        
        try:
            sheet = self.gc.open_by_key(self.backup_sheet_id)
            
            # Try to get or create backup worksheet
            try:
                self.backup_sheet = sheet.worksheet('Backup')
            except:
                self.backup_sheet = sheet.add_worksheet('Backup', rows=1000, cols=10)
                headers = ['chat_id', 'group_name', 'status', 'trial_end_date', 
                          'subscription_end_date', 'added_date', 'last_backup']
                self.backup_sheet.insert_row(headers, 1)
            
            logger.info("Backup sheet ready")
        except Exception as e:
            logger.error(f"Failed to setup backup sheet: {e}")
    
    def backup_to_sheets(self):
        """Backup groups data to Google Sheets"""
        if not self.backup_sheet:
            return False
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('SELECT * FROM groups')
            groups = cursor.fetchall()
            conn.close()
            
            # Clear existing data (except header)
            self.backup_sheet.clear()
            headers = ['chat_id', 'group_name', 'status', 'trial_end_date', 
                      'subscription_end_date', 'added_date', 'last_backup']
            self.backup_sheet.insert_row(headers, 1)
            
            # Add backup timestamp
            backup_time = datetime.now().isoformat()
            rows = []
            for group in groups:
                row = list(group) + [backup_time]
                rows.append(row)
            
            if rows:
                self.backup_sheet.append_rows(rows)
            
            logger.info(f"Backed up {len(rows)} groups to Google Sheets")
            return True
        except Exception as e:
            logger.error(f"Backup failed: {e}")
            return False
    
    def restore_from_sheets(self):
        """Restore groups data from Google Sheets backup"""
        if not self.backup_sheet:
            return False, "Backup sheet not configured"
        
        try:
            records = self.backup_sheet.get_all_records()
            
            if not records:
                return False, "No backup data found"
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            restored = 0
            for record in records:
                cursor.execute('''
                    INSERT OR REPLACE INTO groups 
                    (chat_id, group_name, status, trial_end_date, subscription_end_date, added_date)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    record['chat_id'],
                    record['group_name'],
                    record['status'],
                    record.get('trial_end_date'),
                    record.get('subscription_end_date'),
                    record['added_date']
                ))
                restored += 1
            
            conn.commit()
            conn.close()
            
            logger.info(f"Restored {restored} groups from backup")
            return True, f"Restored {restored} groups from backup"
        except Exception as e:
            logger.error(f"Restore failed: {e}")
            return False, f"Restore failed: {str(e)}"
    
    def is_super_admin(self, user_id):
        """Check if user is the super admin"""
        return user_id == self.super_admin_id
    
    def is_group_admin(self, chat_id, user_id):
        """Check if user is admin for specific group"""
        if self.is_super_admin(user_id):
            return True
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT COUNT(*) FROM group_admins
                WHERE chat_id = ? AND user_id = ?
            ''', (chat_id, user_id))
            
            result = cursor.fetchone()[0] > 0
            conn.close()
            return result
        except Exception as e:
            logger.error(f"Error checking group admin: {e}")
            return False
    
    def add_group_admin(self, chat_id, user_id):
        """Add admin for specific group"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR IGNORE INTO group_admins (chat_id, user_id, role, added_date)
                VALUES (?, ?, 'admin', ?)
            ''', (chat_id, user_id, datetime.now()))
            
            conn.commit()
            conn.close()
            logger.info(f"Added admin {user_id} to group {chat_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to add group admin: {e}")
            return False
    
    def get_group_status(self, chat_id):
        """Get group approval and subscription status"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT status, trial_end_date, subscription_end_date
                FROM groups WHERE chat_id = ?
            ''', (chat_id,))
            
            result = cursor.fetchone()
            conn.close()
            
            if not result:
                return None
            
            status, trial_end, sub_end = result
            
            # Check if trial or subscription expired
            now = datetime.now()
            if status == 'trial' and trial_end:
                trial_end_dt = datetime.fromisoformat(trial_end)
                if now > trial_end_dt:
                    self.update_group_status(chat_id, 'expired')
                    return 'expired'
            
            if status == 'active' and sub_end:
                sub_end_dt = datetime.fromisoformat(sub_end)
                if now > sub_end_dt:
                    self.update_group_status(chat_id, 'expired')
                    return 'expired'
            
            return status
        except Exception as e:
            logger.error(f"Error getting group status: {e}")
            return None
    
    def register_group(self, chat_id, group_name):
        """Register a new group (pending approval)"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Check if already exists
            cursor.execute('SELECT chat_id FROM groups WHERE chat_id = ?', (chat_id,))
            if cursor.fetchone():
                conn.close()
                return False  # Already registered
            
            cursor.execute('''
                INSERT INTO groups (chat_id, group_name, status, added_date)
                VALUES (?, ?, 'pending', ?)
            ''', (chat_id, group_name, datetime.now()))
            
            conn.commit()
            conn.close()
            logger.info(f"Registered new group {chat_id}: {group_name}")
            return True  # New registration
        except Exception as e:
            logger.error(f"Failed to register group: {e}")
            return False
    
    def approve_group_trial(self, chat_id, hours=48):
        """Approve group for trial period"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            trial_end = datetime.now() + timedelta(hours=hours)
            
            cursor.execute('''
                UPDATE groups
                SET status = 'trial', trial_end_date = ?
                WHERE chat_id = ?
            ''', (trial_end, chat_id))
            
            conn.commit()
            conn.close()
            logger.info(f"Approved trial for group {chat_id} until {trial_end}")
            return True
        except Exception as e:
            logger.error(f"Failed to approve trial: {e}")
            return False
    
    def extend_subscription(self, chat_id, days=30):
        """Extend subscription for paid group"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            sub_end = datetime.now() + timedelta(days=days)
            
            cursor.execute('''
                UPDATE groups
                SET status = 'active', subscription_end_date = ?
                WHERE chat_id = ?
            ''', (sub_end, chat_id))
            
            conn.commit()
            conn.close()
            logger.info(f"Extended subscription for group {chat_id} until {sub_end}")
            return True
        except Exception as e:
            logger.error(f"Failed to extend subscription: {e}")
            return False
    
    def update_group_status(self, chat_id, status):
        """Update group status"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('UPDATE groups SET status = ? WHERE chat_id = ?', (status, chat_id))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Failed to update status: {e}")
            return False
    
    def log_activity(self, chat_id, user_id, username, first_name, message_type):
        """Log user activity to database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            now = datetime.now()
            date_str = now.strftime('%Y-%m-%d')
            hour = now.hour
            
            cursor.execute('''
                INSERT INTO activity (chat_id, user_id, username, first_name, timestamp, message_type, date, hour)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (chat_id, user_id, username, first_name, now, message_type, date_str, hour))
            
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Failed to log activity: {e}")
    
    def get_top_contributors(self, chat_id, days=7, limit=10):
        """Get top contributors for specified period"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            
            cursor.execute('''
                SELECT 
                    user_id,
                    COALESCE(username, first_name, 'Unknown') as display_name,
                    COUNT(*) as message_count
                FROM activity
                WHERE chat_id = ? AND date >= ?
                GROUP BY user_id
                ORDER BY message_count DESC
                LIMIT ?
            ''', (chat_id, cutoff_date, limit))
            
            results = cursor.fetchall()
            conn.close()
            
            return results
        except Exception as e:
            logger.error(f"Failed to get top contributors: {e}")
            return []
    
    def get_peak_hours(self, chat_id, days=7):
        """Get peak activity hours"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            
            cursor.execute('''
                SELECT hour, COUNT(*) as count
                FROM activity
                WHERE chat_id = ? AND date >= ?
                GROUP BY hour
                ORDER BY count DESC
                LIMIT 5
            ''', (chat_id, cutoff_date))
            
            results = cursor.fetchall()
            conn.close()
            
            return results
        except Exception as e:
            logger.error(f"Failed to get peak hours: {e}")
            return []
    
    def export_to_csv(self, chat_id, days=30):
        """Export activity data to CSV"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            
            cursor.execute('''
                SELECT 
                    user_id,
                    COALESCE(username, 'N/A') as username,
                    COALESCE(first_name, 'Unknown') as first_name,
                    COUNT(*) as total_messages,
                    MIN(date) as first_activity,
                    MAX(date) as last_activity
                FROM activity
                WHERE chat_id = ? AND date >= ?
                GROUP BY user_id
                ORDER BY total_messages DESC
            ''', (chat_id, cutoff_date))
            
            results = cursor.fetchall()
            conn.close()
            
            output = StringIO()
            writer = csv.writer(output)
            writer.writerow(['User ID', 'Username', 'First Name', 'Total Messages', 
                           'First Activity', 'Last Activity'])
            writer.writerows(results)
            
            return output.getvalue()
        except Exception as e:
            logger.error(f"Failed to export CSV: {e}")
            return None
    
    def get_overall_stats(self, chat_id):
        """Get overall community statistics"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('SELECT COUNT(*) FROM activity WHERE chat_id = ?', (chat_id,))
            total_messages = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(DISTINCT user_id) FROM activity WHERE chat_id = ?', (chat_id,))
            unique_users = cursor.fetchone()[0]
            
            today = datetime.now().strftime('%Y-%m-%d')
            cursor.execute('SELECT COUNT(*) FROM activity WHERE chat_id = ? AND date = ?', (chat_id, today))
            messages_today = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(DISTINCT user_id) FROM activity WHERE chat_id = ? AND date = ?', (chat_id, today))
            active_today = cursor.fetchone()[0]
            
            week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
            cursor.execute('SELECT COUNT(*) FROM activity WHERE chat_id = ? AND date >= ?', (chat_id, week_ago))
            messages_week = cursor.fetchone()[0]
            
            avg_per_user = round(total_messages / unique_users, 1) if unique_users > 0 else 0
            
            conn.close()
            
            return {
                'total_messages': total_messages,
                'unique_users': unique_users,
                'messages_today': messages_today,
                'active_today': active_today,
                'messages_week': messages_week,
                'avg_per_user': avg_per_user
            }
        except Exception as e:
            logger.error(f"Failed to get overall stats: {e}")
            return None
    
    def get_pending_groups(self):
        """Get list of pending groups"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT chat_id, group_name, added_date
                FROM groups
                WHERE status = 'pending'
                ORDER BY added_date DESC
            ''')
            
            results = cursor.fetchall()
            conn.close()
            
            return results
        except Exception as e:
            logger.error(f"Failed to get pending groups: {e}")
            return []
    
    def get_all_active_groups(self):
        """Get list of all active/trial groups"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT g.chat_id, g.group_name, g.status, g.trial_end_date, g.subscription_end_date,
                       COUNT(DISTINCT a.user_id) as unique_users,
                       COUNT(a.id) as total_messages
                FROM groups g
                LEFT JOIN activity a ON g.chat_id = a.chat_id
                WHERE g.status IN ('trial', 'active')
                GROUP BY g.chat_id
                ORDER BY g.added_date DESC
            ''')
            
            results = cursor.fetchall()
            conn.close()
            
            return results
        except Exception as e:
            logger.error(f"Failed to get groups: {e}")
            return []

# Global tracker and application instances
tracker = None
app_instance = None

# Message handler
async def track_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Track all messages in approved groups"""
    if not update.message or not update.effective_chat:
        return
    
    if update.effective_chat.type not in ['group', 'supergroup']:
        return
    
    user = update.effective_user
    message = update.message
    chat = update.effective_chat
    
    # Register group if first time and notify admin
    is_new = tracker.register_group(chat.id, chat.title)
    if is_new:
        # Send notification to super admin
        try:
            await context.bot.send_message(
                chat_id=tracker.super_admin_id,
                text=f"New group added:\n\nName: {chat.title}\nChat ID: {chat.id}\n\nUse /approve_trial {chat.id} to activate."
            )
            logger.info(f"Sent new group notification to admin for {chat.id}")
        except Exception as e:
            logger.error(f"Failed to send notification: {e}")
    
    # Check group status
    status = tracker.get_group_status(chat.id)
    
    if status not in ['trial', 'active']:
        return
    
    # Determine message type
    if message.text:
        message_type = 'text'
    elif message.photo:
        message_type = 'photo'
    elif message.video:
        message_type = 'video'
    elif message.sticker:
        message_type = 'sticker'
    elif message.document:
        message_type = 'document'
    elif message.voice:
        message_type = 'voice'
    else:
        message_type = 'other'
    
    # Log activity
    tracker.log_activity(
        chat.id,
        user.id,
        user.username,
        user.first_name,
        message_type
    )

# Background task for daily backup
async def daily_backup_task(context: ContextTypes.DEFAULT_TYPE):
    """Run daily backup to Google Sheets"""
    if tracker.backup_to_sheets():
        logger.info("Daily backup completed")
        try:
            await context.bot.send_message(
                chat_id=tracker.super_admin_id,
                text="Daily backup completed successfully."
            )
        except:
            pass

# Command handlers
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command"""
    chat = update.effective_chat
    
    if chat.type in ['group', 'supergroup']:
        status = tracker.get_group_status(chat.id)
        
        if status == 'pending':
            await update.message.reply_text(
                "This bot requires authorization to track activity.\n\n"
                "Contact the bot owner to activate tracking for this group."
            )
            return
        elif status == 'expired':
            await update.message.reply_text(
                "Your trial/subscription has expired.\n\n"
                "Contact the bot owner to renew access."
            )
            return
    
    welcome_msg = """Welcome to FelixActivityBot!

This bot tracks community engagement and provides insights.

Available Commands:
/leaderboard - Top contributors (last 7 days)
/peak_times - Most active hours
/community_stats - Overall community statistics

Admin Commands (group admins only):
/export_data - Export activity data to CSV

The bot automatically tracks all messages to help identify the most active and engaged members."""
    
    await update.message.reply_text(welcome_msg)

async def leaderboard_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show top contributors"""
    chat = update.effective_chat
    
    if chat.type not in ['group', 'supergroup']:
        await update.message.reply_text("This command only works in groups.")
        return
    
    status = tracker.get_group_status(chat.id)
    if status not in ['trial', 'active']:
        await update.message.reply_text("This group is not authorized. Contact the bot owner.")
        return
    
    days = 7
    if context.args and context.args[0].isdigit():
        days = int(context.args[0])
        days = min(days, 30)
    
    contributors = tracker.get_top_contributors(chat.id, days=days, limit=10)
    
    if not contributors:
        await update.message.reply_text("No activity data available yet.")
        return
    
    msg = f"Top Contributors (Last {days} Days):\n\n"
    for i, (user_id, display_name, msg_count) in enumerate(contributors, 1):
        msg += f"{i}. {display_name}: {msg_count} messages\n"
    
    await update.message.reply_text(msg)

async def peak_times_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show peak activity hours"""
    chat = update.effective_chat
    
    if chat.type not in ['group', 'supergroup']:
        await update.message.reply_text("This command only works in groups.")
        return
    
    status = tracker.get_group_status(chat.id)
    if status not in ['trial', 'active']:
        await update.message.reply_text("This group is not authorized. Contact the bot owner.")
        return
    
    peak_hours = tracker.get_peak_hours(chat.id, days=7)
    
    if not peak_hours:
        await update.message.reply_text("No activity data available yet.")
        return
    
    msg = "Peak Activity Hours (Last 7 Days):\n\n"
    for hour, count in peak_hours:
        time_range = f"{hour:02d}:00 - {hour:02d}:59"
        msg += f"{time_range}: {count} messages\n"
    
    await update.message.reply_text(msg)

async def community_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show overall community statistics"""
    chat = update.effective_chat
    
    if chat.type not in ['group', 'supergroup']:
        await update.message.reply_text("This command only works in groups.")
        return
    
    status = tracker.get_group_status(chat.id)
    if status not in ['trial', 'active']:
        await update.message.reply_text("This group is not authorized. Contact the bot owner.")
        return
    
    stats = tracker.get_overall_stats(chat.id)
    
    if not stats:
        await update.message.reply_text("Unable to retrieve statistics.")
        return
    
    msg = f"""Community Statistics:

Total Messages: {stats['total_messages']}
Total Members: {stats['unique_users']}
Avg Messages/Member: {stats['avg_per_user']}

Today:
Messages: {stats['messages_today']}
Active Members: {stats['active_today']}

This Week:
Messages: {stats['messages_week']}"""
    
    await update.message.reply_text(msg)

async def export_data_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Export activity data"""
    user = update.effective_user
    chat = update.effective_chat
    
    if chat.type not in ['group', 'supergroup']:
        await update.message.reply_text("This command only works in groups.")
        return
    
    if not tracker.is_group_admin(chat.id, user.id):
        await update.message.reply_text("This command is only available to group administrators.")
        return
    
    status = tracker.get_group_status(chat.id)
    if status not in ['trial', 'active']:
        await update.message.reply_text("This group is not authorized. Contact the bot owner.")
        return
    
    days = 30
    if context.args and context.args[0].isdigit():
        days = int(context.args[0])
        days = min(days, 90)
    
    csv_data = tracker.export_to_csv(chat.id, days=days)
    
    if not csv_data:
        await update.message.reply_text("Failed to export data.")
        return
    
    file = BytesIO(csv_data.encode('utf-8'))
    file.name = f'activity_export_{chat.title}_{datetime.now().strftime("%Y%m%d")}.csv'
    
    await update.message.reply_document(
        document=file,
        filename=file.name,
        caption=f"Activity data for last {days} days"
    )

# Super admin commands
async def pending_groups_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List pending groups"""
    user = update.effective_user
    
    if not tracker.is_super_admin(user.id):
        return
    
    groups = tracker.get_pending_groups()
    
    if not groups:
        await update.message.reply_text("No pending groups.")
        return
    
    msg = "Pending Groups:\n\n"
    for chat_id, group_name, added_date in groups:
        msg += f"{group_name}\n"
        msg += f"  Chat ID: {chat_id}\n"
        msg += f"  Added: {added_date[:10]}\n\n"
    
    msg += "\nUse /approve_trial <chat_id> to start trial"
    
    await update.message.reply_text(msg)

async def approve_trial_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Approve group for trial"""
    user = update.effective_user
    
    if not tracker.is_super_admin(user.id):
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /approve_trial <chat_id> [hours]\nExample: /approve_trial -1001234567890 48")
        return
    
    try:
        chat_id = int(context.args[0])
        hours = 48
        if len(context.args) > 1:
            hours = int(context.args[1])
        
        if tracker.approve_group_trial(chat_id, hours):
            await update.message.reply_text(f"Trial approved for {hours} hours.")
        else:
            await update.message.reply_text("Failed to approve trial.")
    except ValueError:
        await update.message.reply_text("Invalid chat ID or hours value.")

async def extend_subscription_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Extend subscription for paid group"""
    user = update.effective_user
    
    if not tracker.is_super_admin(user.id):
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /extend_subscription <chat_id> [days]\nExample: /extend_subscription -1001234567890 30")
        return
    
    try:
        chat_id = int(context.args[0])
        days = 30
        if len(context.args) > 1:
            days = int(context.args[1])
        
        if tracker.extend_subscription(chat_id, days):
            await update.message.reply_text(f"Subscription extended for {days} days.")
        else:
            await update.message.reply_text("Failed to extend subscription.")
    except ValueError:
        await update.message.reply_text("Invalid chat ID or days value.")

async def add_group_admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add admin to a group"""
    user = update.effective_user
    
    if not tracker.is_super_admin(user.id):
        return
    
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /add_group_admin <chat_id> <user_id>\nExample: /add_group_admin -1001234567890 123456789")
        return
    
    try:
        chat_id = int(context.args[0])
        admin_user_id = int(context.args[1])
        
        if tracker.add_group_admin(chat_id, admin_user_id):
            await update.message.reply_text(f"Added admin {admin_user_id} to group {chat_id}")
        else:
            await update.message.reply_text("Failed to add admin.")
    except ValueError:
        await update.message.reply_text("Invalid chat ID or user ID.")

async def my_groups_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all active groups"""
    user = update.effective_user
    
    if not tracker.is_super_admin(user.id):
        return
    
    groups = tracker.get_all_active_groups()
    
    if not groups:
        await update.message.reply_text("No active groups.")
        return
    
    msg = "Active Groups:\n\n"
    for chat_id, group_name, status, trial_end, sub_end, unique_users, total_messages in groups:
        msg += f"{group_name}\n"
        msg += f"  Chat ID: {chat_id}\n"
        msg += f"  Status: {status}\n"
        if status == 'trial' and trial_end:
            msg += f"  Trial ends: {trial_end[:16]}\n"
        if status == 'active' and sub_end:
            msg += f"  Subscription ends: {sub_end[:16]}\n"
        msg += f"  Members: {unique_users} | Messages: {total_messages}\n\n"
    
    await update.message.reply_text(msg)

async def revoke_access_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Revoke access to a group"""
    user = update.effective_user
    
    if not tracker.is_super_admin(user.id):
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /revoke_access <chat_id>\nExample: /revoke_access -1001234567890")
        return
    
    try:
        chat_id = int(context.args[0])
        
        if tracker.update_group_status(chat_id, 'expired'):
            await update.message.reply_text(f"Access revoked for group {chat_id}")
        else:
            await update.message.reply_text("Failed to revoke access.")
    except ValueError:
        await update.message.reply_text("Invalid chat ID.")

async def backup_now_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manually trigger backup"""
    user = update.effective_user
    
    if not tracker.is_super_admin(user.id):
        return
    
    if tracker.backup_to_sheets():
        await update.message.reply_text("Backup completed successfully.")
    else:
        await update.message.reply_text("Backup failed. Check logs.")

async def restore_backup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Restore from Google Sheets backup"""
    user = update.effective_user
    
    if not tracker.is_super_admin(user.id):
        return
    
    success, message = tracker.restore_from_sheets()
    await update.message.reply_text(message)

async def download_db_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Download database file"""
    user = update.effective_user
    
    if not tracker.is_super_admin(user.id):
        return
    
    try:
        with open(tracker.db_path, 'rb') as f:
            await update.message.reply_document(
                document=f,
                filename=f'backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.db',
                caption="Database backup"
            )
    except Exception as e:
        await update.message.reply_text(f"Failed to send database: {str(e)}")

async def admin_help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show super admin commands"""
    user = update.effective_user
    
    if not tracker.is_super_admin(user.id):
        return
    
    help_msg = """Super Admin Commands:

Group Management:
/pending_groups - List groups awaiting approval
/approve_trial <chat_id> [hours] - Start trial (default 48h)
/extend_subscription <chat_id> [days] - Extend subscription (default 30d)
/revoke_access <chat_id> - Revoke group access
/my_groups - List all active groups

Admin Management:
/add_group_admin <chat_id> <user_id> - Add group admin

Backup & Recovery:
/backup_now - Manually trigger backup to Google Sheets
/restore_backup - Restore groups from Google Sheets backup
/download_db - Download database file

Examples:
/approve_trial -1001234567890 48
/extend_subscription -1001234567890 30
/add_group_admin -1001234567890 123456789"""
    
    await update.message.reply_text(help_msg)

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Log errors"""
    logger.warning('Update "%s" caused error "%s"', update, context.error)

def main():
    """Run the bot"""
    global tracker, app_instance
    
    # Get configuration
    BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
    SUPER_ADMIN_ID = os.getenv('ADMIN_USER_IDS')
    DB_PATH = os.getenv('DB_PATH', 'activity_tracker.db')
    BACKUP_SHEET_ID = os.getenv('GOOGLE_SPREADSHEET_ID')
    
    if not all([BOT_TOKEN, SUPER_ADMIN_ID]):
        logger.error("Missing required environment variables")
        return
    
    try:
        # Initialize tracker
        tracker = ActivityTracker(DB_PATH, SUPER_ADMIN_ID, BACKUP_SHEET_ID)
        
        # Create application
        application = Application.builder().token(BOT_TOKEN).build()
        app_instance = application
        
        # Public commands
        application.add_handler(CommandHandler("start", start_command))
        application.add_handler(CommandHandler("leaderboard", leaderboard_command))
        application.add_handler(CommandHandler("peak_times", peak_times_command))
        application.add_handler(CommandHandler("community_stats", community_stats_command))
        application.add_handler(CommandHandler("export_data", export_data_command))
        
        # Super admin commands
        application.add_handler(CommandHandler("admin_help", admin_help_command))
        application.add_handler(CommandHandler("pending_groups", pending_groups_command))
        application.add_handler(CommandHandler("approve_trial", approve_trial_command))
        application.add_handler(CommandHandler("extend_subscription", extend_subscription_command))
        application.add_handler(CommandHandler("add_group_admin", add_group_admin_command))
        application.add_handler(CommandHandler("my_groups", my_groups_command))
        application.add_handler(CommandHandler("revoke_access", revoke_access_command))
        application.add_handler(CommandHandler("backup_now", backup_now_command))
        application.add_handler(CommandHandler("restore_backup", restore_backup_command))
        application.add_handler(CommandHandler("download_db", download_db_command))
        
        # Message handler to track all messages
        application.add_handler(MessageHandler(
            filters.ALL & ~filters.COMMAND, 
            track_message
        ))
        
        # Error handler
        application.add_error_handler(error_handler)
        
        # Schedule daily backup (runs at 2 AM daily)
        if BACKUP_SHEET_ID:
            job_queue = application.job_queue
            job_queue.run_daily(daily_backup_task, time=datetime.strptime("02:00", "%H:%M").time())
            logger.info("Daily backup scheduled")
        
        # Run bot
        logger.info("FelixActivityBot starting...")
        application.run_polling(drop_pending_updates=True)
        
    except Exception as e:
        logger.error(f"Failed to start bot: {e}")

if __name__ == '__main__':
    main()
