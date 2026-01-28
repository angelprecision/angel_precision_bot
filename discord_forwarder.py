import discord
import requests
import os

# Discord bot token (create at https://discord.com/developers/applications)
DISCORD_TOKEN = os.getenv("DISCORD_BOT_TOKEN")

# Your scanner channel ID (right-click channel → Copy ID)
SCANNER_CHANNEL_ID = int(os.getenv("SCANNER_CHANNEL_ID"))

# Your Render bot URL
BOT_URL = "https://angel-precision-bot-official-1.onrender.com/scanner/discord"

intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)

@client.event
async def on_ready():
    print(f"✅ Discord forwarder connected as {client.user}")

@client.event
async def on_message(message):
    # Ignore own messages
    if message.author == client.user:
        return
    
    # Only process messages from scanner channel
    if message.channel.id != SCANNER_CHANNEL_ID:
        return
    
    # Forward to your bot
    try:
        response = requests.post(
            BOT_URL,
            headers={
                "Content-Type": "application/json",
                "X-Client-Id": "default"
            },
            json={"content": message.content},
            timeout=10
        )
        
        if response.ok:
            result = response.json()
            print(f"✅ Forwarded: {result.get('queued', 0)} signals queued")
            # Optional: React to message
            await message.add_reaction("✅")
        else:
            print(f"❌ Bot error: {response.status_code}")
            await message.add_reaction("❌")
            
    except Exception as e:
        print(f"❌ Forward failed: {e}")
        await message.add_reaction("⚠️")

client.run(DISCORD_TOKEN)
```

---

## Setup Instructions

### 1. Create Discord Bot (5 min)

1. Go to: https://discord.com/developers/applications
2. Click "New Application"
3. Name it: "Scanner Forwarder"
4. Go to "Bot" tab
5. Click "Add Bot"
6. **Copy the TOKEN** (save it)
7. Enable "Message Content Intent" (under Privileged Gateway Intents)
8. Go to OAuth2 → URL Generator
9. Select: `bot`
10. Bot Permissions: `Send Messages`, `Read Messages`, `Add Reactions`
11. **Copy the URL** and open it
12. Add bot to your Discord server

---

### 2. Get Your Scanner Channel ID (30 sec)

1. Open Discord
2. Go to User Settings → Advanced
3. Enable "Developer Mode"
4. Right-click your scanner channel → Copy ID
5. **Save this ID**

---

### 3. Deploy Discord Forwarder

**Where do you want to run the forwarder?**

**Option A: On Render** (Recommended - $7/month)
1. Create new GitHub repo: `discord-forwarder`
2. Add `discord_forwarder.py` and `requirements.txt`:
```
   discord.py==2.3.2
   requests==2.31.0
