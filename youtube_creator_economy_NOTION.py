#!/usr/bin/env python3
"""
YouTube Creator Economy Transcriber - ACTUALLY WORKING VERSION
Uses NEW google-genai SDK that supports YouTube URLs
"""

import os
import time
import requests
from datetime import datetime
from dateutil import parser
from google import genai
from google.genai import types
from googleapiclient.discovery import build

class YouTubeCreatorEconomyAutomation:
    def __init__(self):
        self.load_env_configs()
        self.processed_videos_cache = self.load_processed_videos_from_notion()
        
        # Channel @ handles
        self.channel_handles = [
            "@mogulmail",
            "@creatorsupportpod",
            "@youshaei",
            "@youtubecreatorshubpod",
            "@vidiq",
            "@tubebuddy",
            "@CocoMocoe",
            "@orenmeetsworld",
            "@julesterpak",
            "@InternetAnarchist",
            "@AprilynneAlter",
            "@FilmBooth",
            "@Izzzyzzz",
            "@IsaacGarciaFilms",
            "@tiffanyferg",
            "@AliceCappelle",
            "@KhadijaMbowe",
            "@taylorlorenz"
        ]

    def load_env_configs(self):
        """Load configuration from environment variables."""
        self.gemini_key = os.environ.get('GEMINI_API_KEY')
        self.youtube_api_key = os.environ.get('YOUTUBE_API_KEY')
        self.notion_token = os.environ.get('NOTION_API_KEY')
        self.notion_database_id = os.environ.get('NOTION_DATABASE_ID')
        
        if not all([self.gemini_key, self.youtube_api_key, self.notion_token, self.notion_database_id]):
            raise ValueError("Missing required environment variables")
        
        # NEW SDK!
        self.gemini_client = genai.Client(api_key=self.gemini_key)
        self.youtube = build('youtube', 'v3', developerKey=self.youtube_api_key)
        print("✓ Environment configured")

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

    def get_channel_videos_by_handle(self, handle, max_results=5):
        """Get latest videos from a YouTube channel using @ handle."""
        try:
            # Get the channel ID from the handle
            search_response = self.youtube.search().list(
                part='snippet',
                q=handle,
                type='channel',
                maxResults=1
            ).execute()
            
            if not search_response.get('items'):
                print(f"  Channel not found: {handle}")
                return []
            
            channel_id = search_response['items'][0]['snippet']['channelId']
            channel_title = search_response['items'][0]['snippet']['title']
            
            # Get the uploads playlist ID
            channel_response = self.youtube.channels().list(
                part='contentDetails',
                id=channel_id
            ).execute()
            
            uploads_playlist_id = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
            
            # Get videos from uploads playlist
            playlist_response = self.youtube.playlistItems().list(
                part='snippet',
                playlistId=uploads_playlist_id,
                maxResults=max_results
            ).execute()
            
            videos = []
            for item in playlist_response.get('items', []):
                video_id = item['snippet']['resourceId']['videoId']
                videos.append({
                    'video_id': video_id,
                    'title': item['snippet']['title'],
                    'channel': channel_title,
                    'published': item['snippet']['publishedAt']
                })
            
            return videos
        except Exception as e:
            print(f"  Error getting videos for {handle}: {e}")
            return []

    def transcribe_youtube_url(self, video_url):
        """Transcribe YouTube video using NEW SDK that actually supports YouTube URLs."""
        try:
            print(f"  Transcribing from YouTube URL...")
            
            # Get summary
            summary_prompt = f"Provide a 2-3 sentence summary of the main topics discussed in this video."
            
            summary_response = self.gemini_client.models.generate_content(
                model="gemini-2.5-flash",
                contents=[
                    types.Content(
                        parts=[
                            types.Part(file_data=types.FileData(file_uri=video_url)),
                            types.Part(text=summary_prompt)
                        ]
                    )
                ]
            )
            summary = summary_response.text.strip()
            
            # Get transcript
            transcript_prompt = """Generate a complete transcript of this video with:
- Paragraph breaks for readability
- Speaker labels if multiple speakers
- Timestamps where helpful
- Mark ads/sponsors as [AD]"""
            
            transcript_response = self.gemini_client.models.generate_content(
                model="gemini-2.5-flash",
                contents=[
                    types.Content(
                        parts=[
                            types.Part(file_data=types.FileData(file_uri=video_url)),
                            types.Part(text=transcript_prompt)
                        ]
                    )
                ]
            )
            transcript = transcript_response.text
            
            print("  ✓ Transcript generated")
            return {'summary': summary, 'transcript': transcript}
            
        except Exception as e:
            print(f"  ✗ Error: {e}")
            return {
                'summary': 'Video transcription failed.',
                'transcript': f'Error: {str(e)}'
            }

    def add_to_notion(self, channel, title, published, video_id, summary, transcript):
        """Add video to Notion with full transcript."""
        try:
            print("  → Adding to Notion...")
            
            try:
                notion_date = parser.parse(published).strftime('%Y-%m-%d')
            except:
                notion_date = datetime.now().strftime('%Y-%m-%d')
            
            if len(summary) > 2000:
                summary = summary[:1997] + "..."
            
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
        
        try:
            video_url = f"https://www.youtube.com/watch?v={video_id}"
            
            # Transcribe using NEW SDK
            result = self.transcribe_youtube_url(video_url)
            
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

    def run(self):
        """Main loop."""
        print("\n" + "="*80)
        print("YOUTUBE CREATOR ECONOMY TRANSCRIBER")
        print("="*80)
        
        total = 0
        all_videos = []
        
        for handle in self.channel_handles:
            try:
                print(f"\n{'#'*80}\nChannel: {handle}\n{'#'*80}")
                videos = self.get_channel_videos_by_handle(handle, max_results=5)
                all_videos.extend(videos)
                print(f"  Found {len(videos)} videos")
            except Exception as e:
                print(f"  Error: {e}")
        
        unique_videos = {v['video_id']: v for v in all_videos}.values()
        
        for video in unique_videos:
            if self.process_video(video):
                total += 1
                time.sleep(5)
        
        print(f"\n{'='*80}\nCOMPLETE - Processed {total} videos\n{'='*80}")


if __name__ == "__main__":
    YouTubeCreatorEconomyAutomation().run()
