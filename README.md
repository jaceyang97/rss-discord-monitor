# RSS Monitor

A Python RSS feed monitor with Discord notifications.

## Features

- Real-time RSS feed monitoring
- Discord webhook notifications
- SQLite database storage
- Proxy support

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Create `config.json`:
```json
{
  "discord_webhook": "your_webhook_url",
  "monitoring_interval": 60,
  "feeds": [
    {
      "name": "Feed Name",
      "url": "https://example.com/rss.xml",
      "enabled": true
    }
  ]
}
```

3. Run the monitor:
```bash
python main.py
```

## Configuration

- `discord_webhook`: Your Discord webhook URL
- `monitoring_interval`: Check interval in seconds
- `feeds`: Array of RSS feeds to monitor


