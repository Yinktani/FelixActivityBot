import os
import logging
import sqlite3
from datetime import datetime, timedelta
from collections import defaultdict
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from dotenv import load_dotenv
import csv
from io import StringIO

load_dotenv()

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class ActivityTracker:
    def __init__(self, db_path, admin_user_ids):
        self.db_path = db_path
        self.admin_user_ids = set(map(int, admin_user_ids.split(',')))
        self.setup_database()
    
    def setup_database(self):
        """Initialize SQLite database with proper schema"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create activity table with chat_id for multi-group support
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS activity (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    username TEXT,
                    first_name TEXT,
                    timestamp DATETIME NOT NULL,
                    message_type TEXT,
                    char_count INTEGER,
                    date TEXT NOT NULL,
                    hour INTEGER NOT NULL
                )
            ''')
            
            # Create indexes for faster queries
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_chat_id ON activity(chat_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_id ON activity(user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_date ON activity(date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON activity(timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_chat_user ON activity(chat_id, user_id)')
            
            # Create groups table to store group info
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS groups (
                    chat_id INTEGER PRIMARY KEY,
                    group_name TEXT,
                    added_date DATETIME NOT NULL
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("Database setup completed")
        except Exception as e:
            logger.error(f"Database setup failed: {e}")
            raise
    
    def is_admin(self, user_id):
        """Check if user is an admin"""
        return user_id in self.admin_user_ids
    
    def register_group(self, chat_id, group_name):
        """Register a new group in the database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR IGNORE INTO groups (chat_id, group_name, added_date)
                VALUES (?, ?, ?)
            ''', (chat_id, group_name, datetime.now()))
            
            conn.commit()
            conn.close()
            logger.info(f"Registered group {chat_id}: {group_name}")
        except Exception as e:
            logger.error(f"Failed to register group: {e}")
    
    def log_activity(self, chat_id, user_id, username, first_name, message_type, char_count):
        """Log user activity to database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            now = datetime.now()
            date_str = now.strftime('%Y-%m-%d')
            hour = now.hour
            
            cursor.execute('''
                INSERT INTO activity (chat_id, user_id, username, first_name, timestamp, message_type, char_count, date, hour)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (chat_id, user_id, username, first_name, now, message_type, char_count, date_str, hour))
            
            conn.commit()
            conn.close()
            logger.info(f"Logged activity for user {user_id} in chat {chat_id}")
        except Exception as e:
            logger.error(f"Failed to log activity: {e}")
    
    def get_top_contributors(self, chat_id, days=7, limit=10):
        """Get top contributors for specified period in specific chat"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            
            cursor.execute('''
                SELECT 
                    user_id,
                    COALESCE(username, first_name, 'Unknown') as display_name,
                    COUNT(*) as message_count,
                    SUM(char_count) as total_chars
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
    
    def get_user_activity(self, chat_id, user_id, days=30):
        """Get activity stats for specific user in specific chat"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            
            # Total messages
            cursor.execute('''
                SELECT COUNT(*) FROM activity
                WHERE chat_id = ? AND user_id = ? AND date >= ?
            ''', (chat_id, user_id, cutoff_date))
            total_messages = cursor.fetchone()[0]
            
            # Messages by type
            cursor.execute('''
                SELECT message_type, COUNT(*) as count
                FROM activity
                WHERE chat_id = ? AND user_id = ? AND date >= ?
                GROUP BY message_type
                ORDER BY count DESC
            ''', (chat_id, user_id, cutoff_date))
            message_types = cursor.fetchall()
            
            # Most active day
            cursor.execute('''
                SELECT date, COUNT(*) as count
                FROM activity
                WHERE chat_id = ? AND user_id = ? AND date >= ?
                GROUP BY date
                ORDER BY count DESC
                LIMIT 1
            ''', (chat_id, user_id, cutoff_date))
            most_active_day = cursor.fetchone()
            
            # Average messages per day
            cursor.execute('''
                SELECT COUNT(DISTINCT date) FROM activity
                WHERE chat_id = ? AND user_id = ? AND date >= ?
            ''', (chat_id, user_id, cutoff_date))
            active_days = cursor.fetchone()[0]
            avg_per_day = round(total_messages / active_days, 1) if active_days > 0 else 0
            
            conn.close()
            
            return {
                'total_messages': total_messages,
                'message_types': message_types,
                'most_active_day': most_active_day,
                'avg_per_day': avg_per_day,
                'active_days': active_days
            }
        except Exception as e:
            logger.error(f"Failed to get user activity: {e}")
            return None
    
    def get_peak_hours(self, chat_id, days=7):
        """Get peak activity hours for specific chat"""
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
    
    def get_daily_activity(self, chat_id, days=7):
        """Get daily activity breakdown for specific chat"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            
            cursor.execute('''
                SELECT date, COUNT(*) as count, COUNT(DISTINCT user_id) as unique_users
                FROM activity
                WHERE chat_id = ? AND date >= ?
                GROUP BY date
                ORDER BY date DESC
            ''', (chat_id, cutoff_date))
            
            results = cursor.fetchall()
            conn.close()
            
            return results
        except Exception as e:
            logger.error(f"Failed to get daily activity: {e}")
            return []
    
    def export_to_csv(self, chat_id, days=30):
        """Export activity data to CSV format for specific chat"""
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
                    SUM(char_count) as total_characters,
                    MIN(date) as first_activity,
                    MAX(date) as last_activity
                FROM activity
                WHERE chat_id = ? AND date >= ?
                GROUP BY user_id
                ORDER BY total_messages DESC
            ''', (chat_id, cutoff_date))
            
            results = cursor.fetchall()
            conn.close()
            
            # Create CSV
            output = StringIO()
            writer = csv.writer(output)
            writer.writerow(['User ID', 'Username', 'First Name', 'Total Messages', 
                           'Total Characters', 'First Activity', 'Last Activity'])
            writer.writerows(results)
            
            return output.getvalue()
        except Exception as e:
            logger.error(f"Failed to export CSV: {e}")
            return None
    
    def get_overall_stats(self, chat_id):
        """Get overall community statistics for specific chat"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Total messages
            cursor.execute('SELECT COUNT(*) FROM activity WHERE chat_id = ?', (chat_id,))
            total_messages = cursor.fetchone()[0]
            
            # Unique users
            cursor.execute('SELECT COUNT(DISTINCT user_id) FROM activity WHERE chat_id = ?', (chat_id,))
            unique_users = cursor.fetchone()[0]
            
            # Messages today
            today = datetime.now().strftime('%Y-%m-%d')
            cursor.execute('SELECT COUNT(*) FROM activity WHERE chat_id = ? AND date = ?', (chat_id, today))
            messages_today = cursor.fetchone()[0]
            
            # Active users today
            cursor.execute('SELECT COUNT(DISTINCT user_id) FROM activity WHERE chat_id = ? AND date = ?', (chat_id, today))
            active_today = cursor.fetchone()[0]
            
            # Messages this week
            week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
            cursor.execute('SELECT COUNT(*) FROM activity WHERE chat_id = ? AND date >= ?', (chat_id, week_ago))
            messages_week = cursor.fetchone()[0]
            
            # Average messages per user
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
    
    def get_all_groups(self):
        """Get list of all registered groups (admin only)"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT g.chat_id, g.group_name, g.added_date, 
                       COUNT(DISTINCT a.user_id) as unique_users,
                       COUNT(a.id) as total_messages
                FROM groups g
                LEFT JOIN activity a ON g.chat_id = a.chat_id
                GROUP BY g.chat_id
                ORDER BY g.added_date DESC
            ''')
            
            results = cursor.fetchall()
            conn.close()
            
            return results
        except Exception as e:
            logger.error(f"Failed to get groups: {e}")
            return []

# Global tracker instance
tracker = None

# Message handler
async def track_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Track all messages in the group"""
    if not update.message or not update.effective_chat:
        return
    
    # Only track group messages, not private chats
    if update.effective_chat.type not in ['group', 'supergroup']:
        return
    
    user = update.effective_user
    message = update.message
    chat = update.effective_chat
    
    # Register group if first time seeing it
    tracker.register_group(chat.id, chat.title)
    
    # Determine message type
    if message.text:
        message_type = 'text'
        char_count = len(message.text)
    elif message.photo:
        message_type = 'photo'
        char_count = 0
    elif message.video:
        message_type = 'video'
        char_count = 0
    elif message.sticker:
        message_type = 'sticker'
        char_count = 0
    elif message.document:
        message_type = 'document'
        char_count = 0
    elif message.voice:
        message_type = 'voice'
        char_count = 0
    else:
        message_type = 'other'
        char_count = 0
    
    # Log activity with chat_id
    tracker.log_activity(
        chat.id,
        user.id,
        user.username,
        user.first_name,
        message_type,
        char_count
    )

# Command handlers
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command"""
    welcome_msg = """Welcome to FelixActivityBot!

This bot tracks community engagement and provides insights.

Available Commands:
/leaderboard - Top contributors (last 7 days)
/my_activity - Your personal stats
/peak_times - Most active hours
/community_stats - Overall community statistics

Admin Commands:
/export_data - Export activity data to CSV
/daily_report - Daily activity breakdown
/list_groups - List all groups using the bot

The bot automatically tracks all messages to help identify the most active and engaged members.

Note: This bot only works in groups, not private chats."""
    
    await update.message.reply_text(welcome_msg)

async def leaderboard_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show top contributors"""
    chat = update.effective_chat
    
    # Only work in groups
    if chat.type not in ['group', 'supergroup']:
        await update.message.reply_text("This command only works in groups.")
        return
    
    # Check if days parameter provided
    days = 7
    if context.args and context.args[0].isdigit():
        days = int(context.args[0])
        days = min(days, 30)  # Cap at 30 days
    
    contributors = tracker.get_top_contributors(chat.id, days=days, limit=10)
    
    if not contributors:
        await update.message.reply_text("No activity data available yet.")
        return
    
    msg = f"Top Contributors (Last {days} Days):\n\n"
    for i, (user_id, display_name, msg_count, total_chars) in enumerate(contributors, 1):
        avg_chars = round(total_chars / msg_count) if msg_count > 0 else 0
        msg += f"{i}. {display_name}\n"
        msg += f"   Messages: {msg_count} | Avg length: {avg_chars} chars\n\n"
    
    await update.message.reply_text(msg)

async def my_activity_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show user's personal activity stats"""
    user = update.effective_user
    chat = update.effective_chat
    
    # Only work in groups
    if chat.type not in ['group', 'supergroup']:
        await update.message.reply_text("This command only works in groups.")
        return
    
    # Check if admin is querying another user
    target_user_id = user.id
    if context.args and tracker.is_admin(user.id):
        try:
            target_user_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text("Invalid user ID")
            return
    
    stats = tracker.get_user_activity(chat.id, target_user_id, days=30)
    
    if not stats or stats['total_messages'] == 0:
        await update.message.reply_text("No activity data found for this user.")
        return
    
    msg = f"Activity Stats (Last 30 Days):\n\n"
    msg += f"Total Messages: {stats['total_messages']}\n"
    msg += f"Active Days: {stats['active_days']}\n"
    msg += f"Avg Messages/Day: {stats['avg_per_day']}\n\n"
    
    if stats['most_active_day']:
        msg += f"Most Active Day: {stats['most_active_day'][0]} ({stats['most_active_day'][1]} messages)\n\n"
    
    if stats['message_types']:
        msg += "Message Types:\n"
        for msg_type, count in stats['message_types'][:5]:
            msg += f"  {msg_type}: {count}\n"
    
    await update.message.reply_text(msg)

async def peak_times_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show peak activity hours"""
    chat = update.effective_chat
    
    # Only work in groups
    if chat.type not in ['group', 'supergroup']:
        await update.message.reply_text("This command only works in groups.")
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
    
    # Only work in groups
    if chat.type not in ['group', 'supergroup']:
        await update.message.reply_text("This command only works in groups.")
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
    """Export activity data (admin only)"""
    user = update.effective_user
    chat = update.effective_chat
    
    if not tracker.is_admin(user.id):
        await update.message.reply_text("This command is only available to administrators.")
        return
    
    # Only work in groups
    if chat.type not in ['group', 'supergroup']:
        await update.message.reply_text("This command only works in groups.")
        return
    
    # Get days parameter
    days = 30
    if context.args and context.args[0].isdigit():
        days = int(context.args[0])
        days = min(days, 90)
    
    csv_data = tracker.export_to_csv(chat.id, days=days)
    
    if not csv_data:
        await update.message.reply_text("Failed to export data.")
        return
    
    # Send as file
    from io import BytesIO
    file = BytesIO(csv_data.encode('utf-8'))
    file.name = f'activity_export_{chat.title}_{datetime.now().strftime("%Y%m%d")}.csv'
    
    await update.message.reply_document(
        document=file,
        filename=file.name,
        caption=f"Activity data for last {days} days"
    )

async def daily_report_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show daily activity breakdown (admin only)"""
    user = update.effective_user
    chat = update.effective_chat
    
    if not tracker.is_admin(user.id):
        await update.message.reply_text("This command is only available to administrators.")
        return
    
    # Only work in groups
    if chat.type not in ['group', 'supergroup']:
        await update.message.reply_text("This command only works in groups.")
        return
    
    daily_data = tracker.get_daily_activity(chat.id, days=7)
    
    if not daily_data:
        await update.message.reply_text("No activity data available.")
        return
    
    msg = "Daily Activity (Last 7 Days):\n\n"
    for date, count, unique_users in daily_data:
        msg += f"{date}:\n"
        msg += f"  Messages: {count}\n"
        msg += f"  Active Users: {unique_users}\n\n"
    
    await update.message.reply_text(msg)

async def list_groups_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all groups using the bot (admin only)"""
    user = update.effective_user
    
    if not tracker.is_admin(user.id):
        await update.message.reply_text("This command is only available to administrators.")
        return
    
    groups = tracker.get_all_groups()
    
    if not groups:
        await update.message.reply_text("No groups registered yet.")
        return
    
    msg = "Registered Groups:\n\n"
    for chat_id, group_name, added_date, unique_users, total_messages in groups:
        msg += f"{group_name}\n"
        msg += f"  Chat ID: {chat_id}\n"
        msg += f"  Members: {unique_users}\n"
        msg += f"  Messages: {total_messages}\n"
        msg += f"  Added: {added_date[:10]}\n\n"
    
    await update.message.reply_text(msg)

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Log errors"""
    logger.warning('Update "%s" caused error "%s"', update, context.error)

def main():
    """Run the bot"""
    global tracker
    
    # Get configuration
    BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
    ADMIN_USER_IDS = os.getenv('ADMIN_USER_IDS')
    DB_PATH = os.getenv('DB_PATH', 'activity_tracker.db')
    
    if not all([BOT_TOKEN, ADMIN_USER_IDS]):
        logger.error("Missing required environment variables")
        return
    
    try:
        # Initialize tracker
        tracker = ActivityTracker(DB_PATH, ADMIN_USER_IDS)
        
        # Create application
        application = Application.builder().token(BOT_TOKEN).build()
        
        # Add command handlers
        application.add_handler(CommandHandler("start", start_command))
        application.add_handler(CommandHandler("leaderboard", leaderboard_command))
        application.add_handler(CommandHandler("my_activity", my_activity_command))
        application.add_handler(CommandHandler("peak_times", peak_times_command))
        application.add_handler(CommandHandler("community_stats", community_stats_command))
        application.add_handler(CommandHandler("export_data", export_data_command))
        application.add_handler(CommandHandler("daily_report", daily_report_command))
        application.add_handler(CommandHandler("list_groups", list_groups_command))
        
        # Add message handler to track all messages
        application.add_handler(MessageHandler(
            filters.ALL & ~filters.COMMAND, 
            track_message
        ))
        
        # Error handler
        application.add_error_handler(error_handler)
        
        # Run bot
        logger.info("FelixActivityBot starting...")
        application.run_polling(drop_pending_updates=True)
        
    except Exception as e:
        logger.error(f"Failed to start bot: {e}")

if __name__ == '__main__':
    main()