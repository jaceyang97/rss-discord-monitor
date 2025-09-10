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
logger.remove()
logger.add("rss_monitor.log", rotation="1 day", retention="7 days", 
          format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}", level="INFO")
logger.add(lambda msg: print(msg, end=""), format="{message}", level="INFO")

class ConfigManager:
    def __init__(self, config_file: str = "config.json"):
        self.config_file = config_file
        self.config = self.load_config()
    
    def load_config(self) -> Dict:
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
        required_fields = ["discord_webhook", "monitoring_interval", "feeds"]
        for field in required_fields:
            if field not in config:
                logger.error(f"❌ Missing required config field: {field}")
                return False
        
        if not config["feeds"]:
            logger.error("❌ No feeds configured")
            return False
        
        enabled_feeds = [feed for feed in config["feeds"] if feed.get("enabled", True)]
        if not enabled_feeds:
            logger.error("❌ No enabled feeds found")
            return False
        return True
    
    def get_proxies(self) -> Dict:
        if self.config.get("proxy", {}).get("enabled", False):
            return {'http': self.config["proxy"]["http"], 'https': self.config["proxy"]["https"]}
        return {}
    
    def get_feeds(self) -> List[Dict]:
        return [feed for feed in self.config.get("feeds", []) if feed.get("enabled", True)]
    
    def get_discord_webhook(self) -> str:
        return self.config.get("discord_webhook", "")
    
    def get_monitoring_interval(self) -> int:
        return self.config.get("monitoring_interval", 1)

class DatabaseManager:
    def __init__(self, db_file: str = "rss_monitor.db"):
        self.db_file = db_file
        self.init_database()
    
    def init_database(self):
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            cursor.execute('''CREATE TABLE IF NOT EXISTS feeds (
                id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL, 
                url TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
            
            cursor.execute('''CREATE TABLE IF NOT EXISTS items (
                id INTEGER PRIMARY KEY AUTOINCREMENT, feed_id INTEGER NOT NULL, guid TEXT NOT NULL,
                title TEXT NOT NULL, link TEXT NOT NULL, description TEXT, published TEXT,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP, last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (feed_id) REFERENCES feeds (id), UNIQUE(feed_id, guid))''')
            
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_items_guid ON items (guid)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_items_feed_id ON items (feed_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_items_last_seen ON items (last_seen)')
            
            conn.commit()
            conn.close()
            logger.info(f"💾 Database initialized: {self.db_file}")
        except Exception as e:
            logger.error(f"❌ Database initialization error: {e}")
    
    def get_or_create_feed(self, name: str, url: str) -> int:
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            cursor.execute('SELECT id FROM feeds WHERE name = ?', (name,))
            result = cursor.fetchone()
            
            if result:
                feed_id = result[0]
            else:
                cursor.execute('INSERT INTO feeds (name, url) VALUES (?, ?)', (name, url))
                feed_id = cursor.lastrowid
            
            conn.commit()
            conn.close()
            return feed_id
        except Exception as e:
            logger.error(f"❌ Database error getting/creating feed: {e}")
            return None
    
    def get_item_history(self, feed_id: int, guid: str) -> Optional[Dict]:
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            cursor.execute('''SELECT title, link, description, published, first_seen, last_seen 
                FROM items WHERE feed_id = ? AND guid = ?''', (feed_id, guid))
            result = cursor.fetchone()
            conn.close()
            
            if result:
                return {'title': result[0], 'link': result[1], 'description': result[2],
                       'published': result[3], 'first_seen': result[4], 'last_seen': result[5]}
            return None
        except Exception as e:
            logger.error(f"❌ Database error getting item: {e}")
            return None
    
    def save_item(self, feed_id: int, guid: str, title: str, link: str, description: str, published: str, is_new: bool = False):
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            current_time = datetime.now().isoformat()
            
            if is_new:
                cursor.execute('''INSERT INTO items (feed_id, guid, title, link, description, published, first_seen, last_seen)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', (feed_id, guid, title, link, description, published, current_time, current_time))
            else:
                cursor.execute('''UPDATE items SET title = ?, link = ?, description = ?, published = ?, last_seen = ?
                    WHERE feed_id = ? AND guid = ?''', (title, link, description, published, current_time, feed_id, guid))
            
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"❌ Database error saving item: {e}")
    
    def get_feed_stats(self, feed_id: int) -> Dict:
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
        if not items:
            return
        
        try:
            embed = {
                "title": f"🔄 RSS Feed Update - {feed_name}",
                "description": f"**{change_type.upper()}** items detected",
                "color": 0x00ff00 if change_type == "new" else 0xffa500,
                "timestamp": datetime.now().isoformat(),
                "fields": []
            }
            
            for item in items:
                if change_type == "new":
                    title, description, link = item['item']['title'], item['item']['description'][:200], item['item']['link']
                else:
                    title, description, link = item['new']['title'], item['new']['description'][:200], item['new']['link']
                
                if len(description) > 200:
                    description = description + "..."
                
                embed["fields"].append({
                    "name": f"📰 {title}",
                    "value": f"{description}\n🔗 [Read More]({link})",
                    "inline": False
                })
            
            payload = {"embeds": [embed], "username": "Monitor Bot"}
            response = requests.post(self.webhook_url, json=payload, headers={"Content-Type": "application/json"}, 
                                  proxies=PROXIES, timeout=10)
            
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
        self.config = config_manager
        self.db_manager = DatabaseManager()
        self.discord = DiscordWebhook(config_manager.get_discord_webhook())
        
        self.feeds = []
        for feed_config in config_manager.get_feeds():
            feed = RSSFeed(feed_config["name"], feed_config["url"], self.db_manager)
            self.feeds.append(feed)
        
        global PROXIES
        PROXIES = config_manager.get_proxies()
    
    def log_fetch_status(self, feed_name: str, data_type: str, status: str, icon: str = "📡"):
        """Log fetch status with proper alignment and timestamp"""
        # Calculate padding to align status messages
        max_feed_length = max(len(feed.name) for feed in self.feeds) if self.feeds else 10
        max_data_type_length = 20  # Approximate max length for data types
        
        # Pad feed name and data type for alignment
        padded_feed = feed_name.ljust(max_feed_length)
        padded_data_type = data_type.ljust(max_data_type_length)
        
        # Choose status icon and format
        if status == "SUCCESS":
            status_icon = "✅"
            status_text = f"SUCCESS {status_icon}"
        else:
            status_icon = "❌"
            status_text = f"FAILED  {status_icon}"
        
        # Get current timestamp with date and time
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Format the log message with timestamp and reduced spacing
        log_message = f"[{current_time}] {icon} Fetching {padded_feed} {padded_data_type}... {status_text}"
        logger.info(log_message)
        
    def fetch_rss(self, feed: RSSFeed) -> Optional[feedparser.FeedParserDict]:
        try:
            response = requests.get(feed.url, proxies=PROXIES, timeout=10)
            if response.status_code == 200:
                self.log_fetch_status(feed.name, "RSS Feed", "SUCCESS")
                return feedparser.parse(response.content)
            else:
                self.log_fetch_status(feed.name, "RSS Feed", f"HTTP {response.status_code}")
                return None
        except requests.exceptions.RequestException:
            self.log_fetch_status(feed.name, "RSS Feed", "Network Error")
            return None
        except Exception:
            self.log_fetch_status(feed.name, "RSS Feed", "Parse Error")
            return None
    
    def compare_and_update(self, feed: RSSFeed, rss_data: feedparser.FeedParserDict) -> Dict[str, List]:
        changes = {'new_items': [], 'updated_items': [], 'unchanged_items': []}
        
        for entry in rss_data.entries:
            guid = entry.guid if hasattr(entry, 'guid') else entry.link
            current_item = {
                'title': entry.title, 'link': entry.link, 'description': entry.description,
                'published': entry.published if hasattr(entry, 'published') else ''
            }
            
            historical_item = self.db_manager.get_item_history(feed.feed_id, guid)
            
            if historical_item:
                if (historical_item['title'] != current_item['title'] or 
                    historical_item['description'] != current_item['description'] or
                    historical_item['link'] != current_item['link']):
                    
                    changes['updated_items'].append({'guid': guid, 'old': historical_item, 'new': current_item})
                    logger.info(f"🔄 {feed.name}: Updated - {current_item['title']}")
                    self.db_manager.save_item(feed.feed_id, guid, current_item['title'], 
                                           current_item['link'], current_item['description'], 
                                           current_item['published'], is_new=False)
                else:
                    changes['unchanged_items'].append(guid)
                    self.db_manager.save_item(feed.feed_id, guid, current_item['title'], 
                                           current_item['link'], current_item['description'], 
                                           current_item['published'], is_new=False)
            else:
                changes['new_items'].append({'guid': guid, 'item': current_item})
                logger.info(f"🆕 {feed.name}: New - {current_item['title']}")
                self.db_manager.save_item(feed.feed_id, guid, current_item['title'], 
                                       current_item['link'], current_item['description'], 
                                       current_item['published'], is_new=True)
        
        return changes
    
    def send_discord_notifications(self, feed: RSSFeed, changes: Dict[str, List]):
        if changes['new_items']:
            self.discord.send_notification(feed.name, changes['new_items'], "new")
        if changes['updated_items']:
            self.discord.send_notification(feed.name, changes['updated_items'], "updated")
    
    def monitor_all_feeds(self):
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
                        self.send_discord_notifications(feed, changes)
                        feed.stats = self.db_manager.get_feed_stats(feed.feed_id)
                time.sleep(interval)
        except KeyboardInterrupt:
            logger.info(f"\n🛑 Stopped")
        except Exception as e:
            logger.error(f"❌ Error: {e}")

if __name__ == "__main__":
    config_manager = ConfigManager()
    
    if not config_manager.validate_config(config_manager.config):
        logger.error("❌ Invalid configuration. Please check your config.json file.")
        exit(1)
    
    monitor = RSSMonitorService(config_manager)
    monitor.monitor_all_feeds()
