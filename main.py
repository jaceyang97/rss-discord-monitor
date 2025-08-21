import requests
import feedparser
from datetime import datetime
import time
import sqlite3
import json
import os
from typing import Dict, List, Optional
from loguru import logger

# Configure loguru
logger.remove()  # Remove default handler
logger.add(
    "rss_monitor.log",
    rotation="1 day",
    retention="7 days",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    level="INFO"
)
logger.add(
    lambda msg: print(msg, end=""),  # Console output without extra formatting
    format="{message}",
    level="INFO"
)

class ConfigManager:
    def __init__(self, config_file: str = "config.json"):
        self.config_file = config_file
        self.config = self.load_config()
    
    def load_config(self) -> Dict:
        """Load configuration from JSON file"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                logger.info(f"📋 Configuration loaded: {self.config_file}")
                return config
            else:
                logger.error(f"❌ Configuration file {self.config_file} not found")
                return {}
        except Exception as e:
            logger.error(f"❌ Error loading config: {e}")
            return {}
    
    def validate_config(self, config: Dict) -> bool:
        """Validate that config has all required fields"""
        required_fields = ["discord_webhook", "monitoring_interval", "feeds"]
        
        for field in required_fields:
            if field not in config:
                logger.error(f"❌ Missing required config field: {field}")
                return False
        
        if not config["feeds"]:
            logger.error("❌ No feeds configured")
            return False
        
        # Check that at least one feed is enabled
        enabled_feeds = [feed for feed in config["feeds"] if feed.get("enabled", True)]
        if not enabled_feeds:
            logger.error("❌ No enabled feeds found")
            return False
        
        return True
    
    def get_default_config(self) -> Dict:
        """Return default configuration if config file is missing"""
        return {}
    
    def get_proxies(self) -> Dict:
        """Get proxy configuration"""
        if self.config.get("proxy", {}).get("enabled", False):
            return {
                'http': self.config["proxy"]["http"],
                'https': self.config["proxy"]["https"]
            }
        return {}
    
    def get_feeds(self) -> List[Dict]:
        """Get enabled feeds from configuration"""
        return [feed for feed in self.config.get("feeds", []) if feed.get("enabled", True)]
    
    def get_discord_webhook(self) -> str:
        """Get Discord webhook URL"""
        return self.config.get("discord_webhook", "")
    
    def get_monitoring_interval(self) -> int:
        """Get monitoring interval in seconds"""
        return self.config.get("monitoring_interval", 1)

class DatabaseManager:
    def __init__(self, db_file: str = "rss_monitor.db"):
        self.db_file = db_file
        self.init_database()
    
    def init_database(self):
        """Initialize SQLite database with tables"""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            # Create feeds table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS feeds (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL,
                    url TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create items table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    feed_id INTEGER NOT NULL,
                    guid TEXT NOT NULL,
                    title TEXT NOT NULL,
                    link TEXT NOT NULL,
                    description TEXT,
                    published TEXT,
                    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (feed_id) REFERENCES feeds (id),
                    UNIQUE(feed_id, guid)
                )
            ''')
            
            # Create indexes for better performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_items_guid ON items (guid)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_items_feed_id ON items (feed_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_items_last_seen ON items (last_seen)')
            
            conn.commit()
            conn.close()
            logger.info(f"💾 Database initialized: {self.db_file}")
            
        except Exception as e:
            logger.error(f"❌ Database initialization error: {e}")
    
    def get_or_create_feed(self, name: str, url: str) -> int:
        """Get feed ID or create new feed, returns feed_id"""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            # Try to get existing feed
            cursor.execute('SELECT id FROM feeds WHERE name = ?', (name,))
            result = cursor.fetchone()
            
            if result:
                feed_id = result[0]
            else:
                # Create new feed
                cursor.execute('INSERT INTO feeds (name, url) VALUES (?, ?)', (name, url))
                feed_id = cursor.lastrowid
            
            conn.commit()
            conn.close()
            return feed_id
            
        except Exception as e:
            logger.error(f"❌ Database error getting/creating feed: {e}")
            return None
    
    def get_item_history(self, feed_id: int, guid: str) -> Optional[Dict]:
        """Get item from database by feed_id and guid"""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT title, link, description, published, first_seen, last_seen 
                FROM items 
                WHERE feed_id = ? AND guid = ?
            ''', (feed_id, guid))
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                return {
                    'title': result[0],
                    'link': result[1],
                    'description': result[2],
                    'published': result[3],
                    'first_seen': result[4],
                    'last_seen': result[5]
                }
            return None
            
        except Exception as e:
            logger.error(f"❌ Database error getting item: {e}")
            return None
    
    def save_item(self, feed_id: int, guid: str, title: str, link: str, description: str, published: str, is_new: bool = False):
        """Save or update item in database"""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            current_time = datetime.now().isoformat()
            
            if is_new:
                # Insert new item
                cursor.execute('''
                    INSERT INTO items (feed_id, guid, title, link, description, published, first_seen, last_seen)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (feed_id, guid, title, link, description, published, current_time, current_time))
            else:
                # Update existing item
                cursor.execute('''
                    UPDATE items 
                    SET title = ?, link = ?, description = ?, published = ?, last_seen = ?
                    WHERE feed_id = ? AND guid = ?
                ''', (title, link, description, published, current_time, feed_id, guid))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"❌ Database error saving item: {e}")
    
    def get_feed_stats(self, feed_id: int) -> Dict:
        """Get statistics for a specific feed"""
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            cursor.execute('SELECT COUNT(*) FROM items WHERE feed_id = ?', (feed_id,))
            total_items = cursor.fetchone()[0]
            
            conn.close()
            return {'total_items': total_items}
            
        except Exception as e:
            logger.error(f"❌ Database error getting stats: {e}")
            return {'total_items': 0}

class DiscordWebhook:
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
        
    def send_notification(self, feed_name: str, items: List[Dict], change_type: str):
        """Send Discord notification for RSS feed changes"""
        try:
            if not items:
                return
                
            # Create Discord embed
            embed = {
                "title": f"🔄 RSS Feed Update - {feed_name}",
                "description": f"**{change_type.upper()}** items detected",
                "color": 0x00ff00 if change_type == "new" else 0xffa500,  # Green for new, Orange for updates
                "timestamp": datetime.now().isoformat(),
                "fields": []
            }
            
            # Add items to embed
            for item in items:
                if change_type == "new":
                    title = item['item']['title']
                    description = item['item']['description'][:200] + "..." if len(item['item']['description']) > 200 else item['item']['description']
                    link = item['item']['link']
                else:  # updated
                    title = item['new']['title']
                    description = item['new']['description'][:200] + "..." if len(item['new']['description']) > 200 else item['new']['description']
                    link = item['new']['link']
                
                field = {
                    "name": f"📰 {title}",
                    "value": f"{description}\n🔗 [Read More]({link})",
                    "inline": False
                }
                embed["fields"].append(field)
            
            # Discord webhook payload
            payload = {
                "embeds": [embed],
                "username": "Monitor Bot",
            }
            
            # Send webhook via proxy
            response = requests.post(
                self.webhook_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                proxies=PROXIES,
                timeout=10
            )
            
            if response.status_code == 204:
                logger.info(f"✅ Discord: {feed_name} - {change_type}")
            else:
                logger.warning(f"⚠️ Discord failed: {response.status_code}")
                
        except Exception as e:
            logger.error(f"❌ Discord error: {e}")

class RSSFeed:
    def __init__(self, name: str, url: str, db_manager: DatabaseManager):
        self.name = name
        self.url = url
        self.db_manager = db_manager
        self.feed_id = db_manager.get_or_create_feed(name, url)
        self.stats = db_manager.get_feed_stats(self.feed_id)
        
        if not self.feed_id:
            logger.error(f"❌ {self.name}: failed to initialize")

class RSSMonitorService:
    def __init__(self, config_manager: ConfigManager):
        # Initialize configuration, database and Discord webhook
        self.config = config_manager
        self.db_manager = DatabaseManager()
        self.discord = DiscordWebhook(config_manager.get_discord_webhook())
        
        # Get feeds from configuration
        self.feeds = []
        for feed_config in config_manager.get_feeds():
            feed = RSSFeed(
                name=feed_config["name"],
                url=feed_config["url"],
                db_manager=self.db_manager
            )
            self.feeds.append(feed)
        
        # Get proxy configuration
        global PROXIES
        PROXIES = config_manager.get_proxies()
        
    def fetch_rss(self, feed: RSSFeed) -> Optional[feedparser.FeedParserDict]:
        """Fetch RSS feed from URL via proxy"""
        try:
            response = requests.get(feed.url, proxies=PROXIES, timeout=10)
            if response.status_code == 200:
                logger.success(f"📡 Fetching {feed.name}... SUCCESS ✅")
                return feedparser.parse(response.content)
            else:
                logger.error(f"📡 Fetching {feed.name}... ❌ HTTP {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            logger.error(f"📡 Fetching {feed.name}... ❌ Network error")
            return None
        except Exception as e:
            logger.error(f"📡 Fetching {feed.name}... ❌ Fetch error")
            return None
    
    def compare_and_update(self, feed: RSSFeed, rss_data: feedparser.FeedParserDict) -> Dict[str, List]:
        """Compare new feed with history and return changes"""
        changes = {
            'new_items': [],
            'updated_items': [],
            'unchanged_items': []
        }
        
        for entry in rss_data.entries:
            guid = entry.guid if hasattr(entry, 'guid') else entry.link
            
            # Create current item data
            current_item = {
                'title': entry.title,
                'link': entry.link,
                'description': entry.description,
                'published': entry.published if hasattr(entry, 'published') else ''
            }
            
            # Check if item exists in database
            historical_item = self.db_manager.get_item_history(feed.feed_id, guid)
            
            if historical_item:
                # Item exists - check for updates
                if (historical_item['title'] != current_item['title'] or 
                    historical_item['description'] != current_item['description'] or
                    historical_item['link'] != current_item['link']):
                    
                    # Content changed
                    changes['updated_items'].append({
                        'guid': guid,
                        'old': historical_item,
                        'new': current_item
                    })
                    
                    logger.info(f"🔄 {feed.name}: {current_item['title']}")
                    
                    # Update item in database
                    self.db_manager.save_item(
                        feed.feed_id, guid, current_item['title'], 
                        current_item['link'], current_item['description'], 
                        current_item['published'], is_new=False
                    )
                else:
                    # Content unchanged
                    changes['unchanged_items'].append(guid)
                    
                    # Update last_seen timestamp
                    self.db_manager.save_item(
                        feed.feed_id, guid, current_item['title'], 
                        current_item['link'], current_item['description'], 
                        current_item['published'], is_new=False
                    )
            else:
                # New item
                changes['new_items'].append({
                    'guid': guid,
                    'item': current_item
                })
                logger.info(f"🆕 {feed.name}: {current_item['title']}")
                
                # Save new item to database
                self.db_manager.save_item(
                    feed.feed_id, guid, current_item['title'], 
                    current_item['link'], current_item['description'], 
                    current_item['published'], is_new=True
                )
        
        return changes
    
    def send_discord_notifications(self, feed: RSSFeed, changes: Dict[str, List]):
        """Send Discord notifications for changes"""
        # Send notification for new items
        if changes['new_items']:
            self.discord.send_notification(feed.name, changes['new_items'], "new")
        
        # Send notification for updated items
        if changes['updated_items']:
            self.discord.send_notification(feed.name, changes['updated_items'], "updated")
    
    def monitor_all_feeds(self):
        """Monitor all RSS feeds continuously"""
        interval = self.config.get_monitoring_interval()
        
        logger.info(f"🚀 RSS Monitor Started - {len(self.feeds)} feeds")
        logger.info(f"⏱️ Request Interval: {interval}s")
        if PROXIES:
            logger.info(f"🌐 Proxy: {PROXIES.get('http', 'N/A')}")
        logger.info("-" * 50)
        
        try:
            while True:
                for feed in self.feeds:
                    rss_data = self.fetch_rss(feed)
                    if rss_data:
                        changes = self.compare_and_update(feed, rss_data)
                        
                        # Send Discord notifications for changes
                        self.send_discord_notifications(feed, changes)
                        
                        # Update feed stats
                        feed.stats = self.db_manager.get_feed_stats(feed.feed_id)
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info(f"\n🛑 Stopped")
        except Exception as e:
            logger.error(f"❌ Error: {e}")

# Main execution
if __name__ == "__main__":
    # Load configuration
    config_manager = ConfigManager()
    
    # Validate configuration
    if not config_manager.validate_config(config_manager.config):
        logger.error("❌ Invalid configuration. Please check your config.json file.")
        exit(1)
    
    # Create and start monitoring service
    monitor = RSSMonitorService(config_manager)
    monitor.monitor_all_feeds()
