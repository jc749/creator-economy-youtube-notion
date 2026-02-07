# YouTube Creator Economy Transcriber - Simple Setup Guide

## Your 18 Channels (Already Configured!)

✅ All channel IDs are already hardcoded in the script - no API key needed!

### Industry News & Strategy
1. **Mogul Mail** (Ludwig)
2. **Creator Support** 
3. **Jon Youshaei**
4. **YouTube Creators Hub** (Dusty Porter)
5. **VidIQ**
6. **TubeBuddy**

### Trend Forecasters & Talent Scouts
7. **Coco Mocoe**
8. **Oren John**
9. **Jules Terpak**
10. **Internet Anarchist**
11. **Aprilynne Alter**

### Deep Dives & Internet Culture
12. **Film Booth** (Ed Lawrence)
13. **Izzzyzzz**
14. **ISAAC** (Isaac Garcia)
15. **Tiffany Ferg**
16. **Alice Cappelle**
17. **Khadija Mbowe**
18. **Taylor Lorenz**

---

## Step 1: Create Notion Database

1. In Notion, create a new **Table** (not Page)
2. Name it: **"YouTube Creator Economy"**
3. Add these properties:
   - **Channel** (Title) - channel name
   - **Title** (Text) - video title
   - **Date** (Date) - publish date
   - **Summary** (Text) - 2-3 sentence summary
   - **Video ID** (Text) - for tracking processed videos
   - **URL** (URL) - clickable YouTube link

---

## Step 2: Create Notion Integration

1. Go to: https://www.notion.so/my-integrations
2. Click **"+ New integration"**
3. Name it: **"YouTube Transcriber"**
4. Select **"Internal Integration"**
5. Copy the **Internal Integration Secret** (this is your `NOTION_API_KEY`)

---

## Step 3: Connect Integration to Database

1. Open your **"YouTube Creator Economy"** database in Notion
2. Click **"..."** (top right corner)
3. Click **"Connections"**
4. Add your **"YouTube Transcriber"** integration
5. **CRITICAL**: Without this step, the API will return 404 errors

---

## Step 4: Get Database ID

1. Open your database in Notion
2. Copy the URL: `https://www.notion.so/XXXXXXXXXX?v=...`
3. The Database ID is the 32-character hex string: `XXXXXXXXXX`
4. Example: `300eef71c27780b0b4d4ccd44379566f`

---

## Step 5: Deploy to Railway

### Files needed:
- `youtube_creator_economy_NOTION.py` (the main script)
- `requirements.txt` (rename from youtube_requirements.txt)
- `Procfile` (rename from youtube_Procfile)
- `runtime.txt` (create with content: `python-3.11.0`)

### Environment Variables (ONLY 3 NEEDED):
```
GEMINI_API_KEY=your_gemini_api_key
NOTION_API_KEY=your_notion_integration_secret
NOTION_DATABASE_ID=your_database_id_from_step_4
```

**NO YouTube API key needed!** The script uses RSS feeds instead.

---

## What It Does

- Pulls **last 5 videos** from each of the 18 channels (90 videos total)
- Downloads audio using yt-dlp
- Transcribes with Gemini 2.0 Flash
- Generates 2-3 sentence summary
- Formats transcript with speaker labels, paragraph breaks
- Saves everything to Notion with clickable YouTube links
- Runs automatically on Railway (set schedule in Railway dashboard)

---

## For ICYMI Podcast (Slate)

**ICYMI is an audio podcast, NOT a YouTube channel.**

Add it to your existing **Creator Economy podcast RSS transcriber** by adding this to that config:

```json
{
  "name": "ICYMI",
  "rss": "https://feeds.megaphone.fm/icymi"
}
```

Use `creator_economy_NOTION.py` for ICYMI, not the YouTube transcriber.

---

## Troubleshooting

**404 Not Found from Notion API**
→ Integration not connected to database (see Step 3)

**No videos found**
→ RSS feed issue, check Railway logs

**403 Forbidden on video download**
→ yt-dlp needs updating: `pip install --upgrade yt-dlp`

**Safety filter blocking transcripts**
→ Already handled with relaxed safety settings in script

---

## Differences from Old Version

✅ **No YouTube API key needed** - uses RSS feeds
✅ **No config.json needed** - channels hardcoded
✅ **Simpler setup** - just 3 environment variables
✅ **Same quality transcripts** - still uses Gemini
