#!/usr/bin/env python3
"""
YouTube Creator Economy Transcriber - Notion
Pulls latest videos from YouTube channels and transcribes them to Notion
"""

import os
import json
import time
import requests
from datetime import datetime
from dateutil import parser
import google.generativeai as genai
import feedparser

class YouTubeCreatorEconomyAutomation:
    def __init__(self):
        self.load_env_configs()
        self.force_clear_api_storage()
        self.processed_videos_cache = self.load_processed_videos_from_notion()
        
        # Channel mapping: ID -> Name
        self.channel_names = {
            "UCjK0F1DopxQ5U0sCwOlXwOg": "Mogul Mail",
            "UCgIzTPYitha6idOdrr7M8sQ": "Creator Support",
            "UC0HdzoHv5W62W7gEINiuqVA": "Jon Youshaei",
            "UCzQUP1qoWDoEbmsQxvdjxgQ": "YouTube Creators Hub",
            "UCKuHFYu3smtrl2uhTdk748w": "VidIQ",
            "UCyxPuL7VYqUfBW0rPH3f-Ow": "TubeBuddy",
            "UCbxy8qJgfi3JA2e9JkX7hRQ": "Coco Mocoe",
            "UCoNHD0-Qh8HVEH1kC1bMnuA": "Oren John",
            "UCJ6qy2E6FIIGkRkBVGmn1rg": "Jules Terpak",
            "UCGMGFoNsKY4JJUpBWIViQvQ": "Internet Anarchist",
            "UCCj956IF62FbT7Gouszaj9w": "Aprilynne Alter",
            "UCXuqSBlHAE6Xw-yeJA0Tunw": "Film Booth",
            "UCX-9TgLZUaQf0aCIQ3gBx0g": "Izzzyzzz",
            "UCQn_kXNZKLrWoUTCfxZpqhw": "ISAAC",
            "UCY6DpwQetygtye7GM1zVHmw": "Tiffany Ferg",
            "UCrCTC5_t-HaVJ025DbYITiw": "Alice Cappelle",
            "UCbuf70y__Wh3MRxZcbj778Q": "Khadija Mbowe",
            "UCp38w5n099xkvoqciOaeFag": "Taylor Lorenz"
        }

    def load_env_configs(self):
        """Load configuration from environment variables."""
        self.gemini_key = os.environ.get('GEMINI_API_KEY')
        self.notion_token = os.environ.get('NOTION_API_KEY')
        self.notion_database_id = os.environ.get('NOTION_DATABASE_ID')
        
        if not all([self.gemini_key, self.notion_token, self.notion_database_id]):
            raise ValueError("Missing required environment variables")
        
        genai.configure(api_key=self.gemini_key)
        print("✓ Environment configured")

    def force_clear_api_storage(self):
        """Delete orphaned Gemini files."""
        print("\n--- Cleaning Gemini Storage ---")
        try:
            files = list(genai.list_files())
            if files:
                for f in files:
                    try:
                        genai.delete_file(f.name)
                    except:
                        pass
                print(f"  ✓ Deleted {len(files)} files")
        except Exception as e:
            print(f"  Warning: {e}")

    def load_processed_videos_from_notion(self):
        """Load all processed video IDs from Notion."""
        print("\n--- Loading Processed Videos ---")
        try:
            url = f"https://api.notion.com/v1/databases/{self.notion_database_id}/query"
            headers = {
                "Authorization": f"Bearer {self.notion_token}",
                "Notion-Version": "2022-06-28",
                "Content-Type": "application/json"
            }
            
            all_video_ids = set()
            has_more = True
            start_cursor = None
            
            while has_more:
                body = {}
                if start_cursor:
                    body["start_cursor"] = start_cursor
                
                response = requests.post(url, headers=headers, json=body)
                response.raise_for_status()
                data = response.json()
                
                for page in data.get('results', []):
                    try:
                        # Extract video ID from "Video ID" property
                        video_id_prop = page['properties'].get('Video ID', {})
                        if video_id_prop.get('rich_text'):
                            video_id = video_id_prop['rich_text'][0]['text']['content']
                            all_video_ids.add(video_id)
                    except:
                        pass
                
                has_more = data.get('has_more', False)
                start_cursor = data.get('next_cursor')
            
            print(f"  ✓ Loaded {len(all_video_ids)} processed videos")
            return all_video_ids
            
        except Exception as e:
            print(f"  Warning: {e}")
            return set()

    def get_channel_videos(self, channel_id, max_results=5):
        """Get latest videos from a YouTube channel using RSS feed."""
        try:
            # YouTube RSS feed URL
            rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
            
            feed = feedparser.parse(rss_url)
            
            videos = []
            for entry in feed.entries[:max_results]:
                video_id = entry.yt_videoid
                videos.append({
                    'video_id': video_id,
                    'title': entry.title,
                    'channel': self.channel_names.get(channel_id, entry.author),
                    'published': entry.published
                })
            
            return videos
        except Exception as e:
            print(f"  Error getting channel videos: {e}")
            return []

    def download_youtube_audio(self, video_id):
        """Download audio from YouTube video using yt-dlp."""
        import subprocess
        
        output_path = f"temp_{video_id}.m4a"
        
        try:
            # Use yt-dlp to download audio
            cmd = [
                'yt-dlp',
                '-f', 'bestaudio',
                '-x',
                '--audio-format', 'm4a',
                '-o', output_path,
                f'https://www.youtube.com/watch?v={video_id}'
            ]
            
            subprocess.run(cmd, check=True, capture_output=True)
            return output_path
            
        except Exception as e:
            print(f"  Error downloading video: {e}")
            return None

    def transcribe_with_retry(self, audio_file, max_retries=5):
        """Transcribe audio with retry logic."""
        for attempt in range(max_retries):
            gemini_file = None
            try:
                print(f"  [{attempt + 1}/{max_retries}] Uploading...")
                gemini_file = genai.upload_file(path=audio_file)
                
                print("  → Processing...")
                start_time = time.time()
                
                while gemini_file.state.name == "PROCESSING":
                    if time.time() - start_time > 600:
                        raise Exception("Timeout")
                    time.sleep(5)
                    gemini_file = genai.get_file(gemini_file.name)
                
                if gemini_file.state.name == "FAILED":
                    raise Exception("Processing failed")
                
                print("  → Generating transcript...")
                model = genai.GenerativeModel("gemini-2.5-flash")
                
                # Get summary
                summary_prompt = "Provide a 2-3 sentence summary of this video's main topics."
                try:
                    summary_response = model.generate_content([summary_prompt, gemini_file])
                    summary = summary_response.text.strip()
                except:
                    summary = "Video discussing creator economy topics."
                
                # Get transcript with formatting
                transcript_prompt = """Transcribe this video with formatting:
1. Paragraph breaks every 2-3 sentences
2. Speaker labels if multiple speakers
3. Mark ads/sponsors as [AD]
4. Readable structure"""
                
                try:
                    response = model.generate_content(
                        [transcript_prompt, gemini_file],
                        safety_settings={
                            'HARASSMENT': 'BLOCK_NONE',
                            'HATE_SPEECH': 'BLOCK_NONE',
                            'SEXUALLY_EXPLICIT': 'BLOCK_NONE',
                            'DANGEROUS_CONTENT': 'BLOCK_NONE'
                        }
                    )
                    transcript = response.text
                except:
                    basic_response = model.generate_content(
                        ["Transcribe this video.", gemini_file],
                        safety_settings={'HARASSMENT': 'BLOCK_NONE', 'HATE_SPEECH': 'BLOCK_NONE', 'SEXUALLY_EXPLICIT': 'BLOCK_NONE', 'DANGEROUS_CONTENT': 'BLOCK_NONE'}
                    )
                    transcript = basic_response.text
                
                print("  ✓ Transcript generated")
                
                try:
                    genai.delete_file(gemini_file.name)
                except:
                    pass
                
                return {'summary': summary, 'transcript': transcript}
                
            except Exception as e:
                print(f"  ✗ Error: {e}")
                if gemini_file:
                    try:
                        genai.delete_file(gemini_file.name)
                    except:
                        pass
                
                if attempt < max_retries - 1:
                    time.sleep(60 * (2 ** attempt))
                else:
                    raise

    def add_to_notion(self, channel, title, published, video_id, summary, transcript):
        """Add video to Notion with full transcript."""
        try:
            print("  → Adding to Notion...")
            
            try:
                notion_date = parser.parse(published).strftime('%Y-%m-%d')
            except:
                notion_date = datetime.now().strftime('%Y-%m-%d')
            
            # Trim summary
            if len(summary) > 2000:
                summary = summary[:1997] + "..."
            
            # Split transcript into chunks
            chunks = []
            for i in range(0, len(transcript), 2000):
                chunks.append({
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {"rich_text": [{"text": {"content": transcript[i:i + 2000]}}]}
                })
            
            headers = {
                "Authorization": f"Bearer {self.notion_token}",
                "Content-Type": "application/json",
                "Notion-Version": "2022-06-28"
            }
            
            # Create page with first 98 chunks
            initial_blocks = [
                {"object": "block", "type": "heading_1", "heading_1": {"rich_text": [{"text": {"content": "Full Transcript"}}]}},
                {"object": "block", "type": "divider", "divider": {}}
            ]
            initial_blocks.extend(chunks[:98])
            
            video_url = f"https://www.youtube.com/watch?v={video_id}"
            
            create_data = {
                "parent": {"database_id": self.notion_database_id},
                "properties": {
                    "Channel": {"title": [{"text": {"content": channel}}]},
                    "Title": {"rich_text": [{"text": {"content": title}}]},
                    "Date": {"date": {"start": notion_date}},
                    "Summary": {"rich_text": [{"text": {"content": summary}}]},
                    "Video ID": {"rich_text": [{"text": {"content": video_id}}]},
                    "URL": {"url": video_url}
                },
                "children": initial_blocks
            }
            
            response = requests.post("https://api.notion.com/v1/pages", headers=headers, json=create_data)
            response.raise_for_status()
            
            page = response.json()
            page_id = page['id']
            
            print(f"  ✓ Created page")
            
            # Append remaining chunks
            remaining = chunks[98:]
            if remaining:
                print(f"  → Appending {len(remaining)} more blocks...")
                for i in range(0, len(remaining), 100):
                    batch = remaining[i:i + 100]
                    requests.patch(f"https://api.notion.com/v1/blocks/{page_id}/children", headers=headers, json={"children": batch}).raise_for_status()
                    time.sleep(0.3)
                print(f"  ✓ Full transcript added")
            
            return page.get('url', '')
            
        except Exception as e:
            print(f"  ✗ Failed: {e}")
            raise

    def process_video(self, video_info):
        """Process a single YouTube video."""
        video_id = video_info['video_id']
        title = video_info['title']
        
        if video_id in self.processed_videos_cache:
            print(f"  Already processed: {title}")
            return False
        
        print(f"\n{'='*80}\nProcessing: {title}\n{'='*80}")
        
        audio_path = None
        
        try:
            # Download audio
            print("  → Downloading...")
            audio_path = self.download_youtube_audio(video_id)
            if not audio_path:
                return False
            print("  ✓ Downloaded")
            
            # Transcribe
            result = self.transcribe_with_retry(audio_path)
            
            # Save to Notion
            self.add_to_notion(
                video_info['channel'],
                title,
                video_info['published'],
                video_id,
                result['summary'],
                result['transcript']
            )
            
            self.processed_videos_cache.add(video_id)
            print(f"  ✓ SUCCESS")
            return True
            
        except Exception as e:
            print(f"  ✗ FAILED: {e}")
            return False
            
        finally:
            if audio_path and os.path.exists(audio_path):
                try:
                    os.remove(audio_path)
                except:
                    pass

    def run(self):
        """Main loop."""
        print("\n" + "="*80)
        print("YOUTUBE CREATOR ECONOMY TRANSCRIBER - Notion")
        print("="*80)
        
        total = 0
        all_videos = []
        
        # Get videos from all channels
        for channel_id in self.channel_names.keys():
            try:
                channel_name = self.channel_names[channel_id]
                print(f"\n{'#'*80}\nChannel: {channel_name}\n{'#'*80}")
                videos = self.get_channel_videos(channel_id, max_results=5)
                all_videos.extend(videos)
                print(f"  Found {len(videos)} videos")
            except Exception as e:
                print(f"  Error: {e}")
        
        # Remove duplicates
        unique_videos = {v['video_id']: v for v in all_videos}.values()
        
        # Process videos
        for video in unique_videos:
            if self.process_video(video):
                total += 1
                time.sleep(5)
        
        print(f"\n{'='*80}\nCOMPLETE - Processed {total} videos\n{'='*80}")


if __name__ == "__main__":
    YouTubeCreatorEconomyAutomation().run()
