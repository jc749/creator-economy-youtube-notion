#!/usr/bin/env python3
"""
YouTube Creator Economy Transcriber - FIXED VERSION WITH RATE LIMITING + ERROR RETRY
Handles 429 errors AND automatically retries videos with failed transcriptions
"""

import os
import time
import requests
from datetime import datetime
from dateutil import parser
from google import genai
from google.genai import types
from googleapiclient.discovery import build
import random

class YouTubeCreatorEconomyAutomation:
    def __init__(self):
        self.load_env_configs()
        self.processed_videos_cache, self.error_videos = self.load_processed_videos_from_notion()
        
        # Rate limiting configuration
        self.request_delay = 3  # Seconds between requests
        self.max_retries = 3
        self.base_backoff = 5  # Base seconds for exponential backoff
        
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
        
        self.gemini_client = genai.Client(api_key=self.gemini_key)
        self.youtube = build('youtube', 'v3', developerKey=self.youtube_api_key)
        print("✓ Environment configured")

    def load_processed_videos_from_notion(self):
        """Load all processed video IDs from Notion and identify ones with errors."""
        print("\n--- Loading Processed Videos ---")
        try:
            url = f"https://api.notion.com/v1/databases/{self.notion_database_id}/query"
            headers = {
                "Authorization": f"Bearer {self.notion_token}",
                "Notion-Version": "2022-06-28",
                "Content-Type": "application/json"
            }
            
            all_video_ids = set()
            error_videos = {}  # video_id -> page_id for videos with errors
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
                            
                            # Check if transcript has errors
                            summary_prop = page['properties'].get('Summary', {})
                            if summary_prop.get('rich_text'):
                                summary = summary_prop['rich_text'][0]['text']['content']
                                
                                # Detect error indicators
                                error_indicators = [
                                    'Video transcription failed',
                                    'Rate limit exceeded',
                                    'Error: 429',
                                    'RESOURCE_EXHAUSTED',
                                    'Error:',
                                    'transcription failed'
                                ]
                                
                                if any(indicator.lower() in summary.lower() for indicator in error_indicators):
                                    error_videos[video_id] = page['id']
                                    print(f"  ⚠️  Found error video: {video_id}")
                    except:
                        pass
                
                has_more = data.get('has_more', False)
                start_cursor = data.get('next_cursor')
            
            print(f"  ✓ Loaded {len(all_video_ids)} processed videos")
            print(f"  ⚠️  Found {len(error_videos)} videos with errors to retry")
            return all_video_ids, error_videos
            
        except Exception as e:
            print(f"  Warning: {e}")
            return set(), {}

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

    def exponential_backoff_delay(self, retry_count):
        """Calculate exponential backoff delay with jitter."""
        delay = self.base_backoff * (2 ** retry_count)
        jitter = random.uniform(0, 1)
        total_delay = delay + jitter
        print(f"  ⏳ Backing off for {total_delay:.1f} seconds (attempt {retry_count + 1}/{self.max_retries})")
        time.sleep(total_delay)

    def transcribe_youtube_url(self, video_url, retry_count=0):
        """Transcribe YouTube video with rate limiting and retry logic."""
        try:
            print(f"  Transcribing from YouTube URL...")
            
            # Rate limit delay before making request
            if retry_count == 0:
                time.sleep(self.request_delay)
            
            # Get summary
            summary_prompt = f"Provide a 2-3 sentence summary of the main topics discussed in this video."
            
            try:
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
            except Exception as e:
                if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                    if retry_count < self.max_retries:
                        print(f"  ⚠️  Rate limit hit on summary. Retrying...")
                        self.exponential_backoff_delay(retry_count)
                        return self.transcribe_youtube_url(video_url, retry_count + 1)
                    else:
                        print(f"  ✗ Max retries reached for summary")
                        raise
                else:
                    raise
            
            # Add delay between API calls
            time.sleep(self.request_delay)
            
            # Get transcript
            transcript_prompt = """Generate a complete transcript of this video with:
- Paragraph breaks for readability
- Speaker labels if multiple speakers
- Timestamps where helpful
- Mark ads/sponsors as [AD]"""
            
            try:
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
            except Exception as e:
                if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                    if retry_count < self.max_retries:
                        print(f"  ⚠️  Rate limit hit on transcript. Retrying...")
                        self.exponential_backoff_delay(retry_count)
                        return self.transcribe_youtube_url(video_url, retry_count + 1)
                    else:
                        print(f"  ✗ Max retries reached for transcript")
                        raise
                else:
                    raise
            
            print("  ✓ Transcript generated")
            return {'summary': summary, 'transcript': transcript}
            
        except Exception as e:
            error_msg = str(e)
            if "429" in error_msg or "RESOURCE_EXHAUSTED" in error_msg:
                print(f"  ✗ Rate limit error after retries: {e}")
                return {
                    'summary': 'Video transcription failed due to rate limits. Will retry later.',
                    'transcript': f'Error: Rate limit exceeded. Video ID preserved for retry.'
                }
            else:
                print(f"  ✗ Error: {e}")
                return {
                    'summary': 'Video transcription failed.',
                    'transcript': f'Error: {str(e)}'
                }

    def update_notion_page(self, page_id, summary, transcript):
        """Update an existing Notion page with new transcript."""
        try:
            print("  → Updating existing Notion page...")
            
            headers = {
                "Authorization": f"Bearer {self.notion_token}",
                "Content-Type": "application/json",
                "Notion-Version": "2022-06-28"
            }
            
            # Update the properties
            if len(summary) > 2000:
                summary = summary[:1997] + "..."
            
            update_data = {
                "properties": {
                    "Summary": {"rich_text": [{"text": {"content": summary}}]}
                }
            }
            
            response = requests.patch(f"https://api.notion.com/v1/pages/{page_id}", headers=headers, json=update_data)
            response.raise_for_status()
            
            # Get existing blocks
            blocks_response = requests.get(f"https://api.notion.com/v1/blocks/{page_id}/children", headers=headers)
            blocks_response.raise_for_status()
            existing_blocks = blocks_response.json().get('results', [])
            
            # Delete all existing blocks (transcript content)
            for block in existing_blocks:
                if block['type'] != 'child_page':
                    try:
                        requests.delete(f"https://api.notion.com/v1/blocks/{block['id']}", headers=headers)
                    except:
                        pass
            
            time.sleep(0.5)
            
            # Add new transcript
            chunks = []
            for i in range(0, len(transcript), 2000):
                chunks.append({
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {"rich_text": [{"text": {"content": transcript[i:i + 2000]}}]}
                })
            
            new_blocks = [
                {"object": "block", "type": "heading_1", "heading_1": {"rich_text": [{"text": {"content": "Full Transcript"}}]}},
                {"object": "block", "type": "divider", "divider": {}}
            ]
            new_blocks.extend(chunks[:98])
            
            # Add initial blocks
            add_response = requests.patch(
                f"https://api.notion.com/v1/blocks/{page_id}/children",
                headers=headers,
                json={"children": new_blocks}
            )
            add_response.raise_for_status()
            
            print(f"  ✓ Updated page")
            
            # Add remaining blocks if needed
            remaining = chunks[98:]
            if remaining:
                print(f"  → Appending {len(remaining)} more blocks...")
                for i in range(0, len(remaining), 100):
                    batch = remaining[i:i + 100]
                    requests.patch(f"https://api.notion.com/v1/blocks/{page_id}/children", headers=headers, json={"children": batch}).raise_for_status()
                    time.sleep(0.3)
                print(f"  ✓ Full transcript updated")
            
            return True
            
        except Exception as e:
            print(f"  ✗ Update failed: {e}")
            raise

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

    def process_video(self, video_info, is_retry=False):
        """Process a single YouTube video."""
        video_id = video_info['video_id']
        title = video_info['title']
        
        # Skip if already successfully processed (not in error list)
        if video_id in self.processed_videos_cache and video_id not in self.error_videos:
            print(f"  Already processed: {title}")
            return False
        
        retry_label = " [RETRY]" if is_retry else ""
        print(f"\n{'='*80}\nProcessing{retry_label}: {title}\n{'='*80}")
        
        try:
            video_url = f"https://www.youtube.com/watch?v={video_id}"
            
            # Transcribe with rate limiting
            result = self.transcribe_youtube_url(video_url)
            
            # Check if transcription actually failed due to rate limits
            if "Rate limit exceeded" in result['transcript']:
                print(f"  ⚠️  Skipping due to rate limits - will process later")
                return False
            
            # Update or create in Notion
            if video_id in self.error_videos:
                # Update existing page
                page_id = self.error_videos[video_id]
                self.update_notion_page(page_id, result['summary'], result['transcript'])
                # Remove from error list
                del self.error_videos[video_id]
            else:
                # Create new page
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
        """Main loop with rate limiting and error retry."""
        print("\n" + "="*80)
        print("YOUTUBE CREATOR ECONOMY TRANSCRIBER - RATE LIMITED + ERROR RETRY")
        print("="*80)
        print(f"⏱️  Request delay: {self.request_delay}s between videos")
        print(f"🔄 Max retries: {self.max_retries} with exponential backoff")
        
        total = 0
        all_videos = []
        
        # First, retry any videos with errors
        if self.error_videos:
            print(f"\n{'*'*80}\n🔄 RETRYING {len(self.error_videos)} VIDEOS WITH ERRORS\n{'*'*80}")
            
            for video_id, page_id in list(self.error_videos.items()):
                # We need to get video info - create minimal info object
                video_info = {
                    'video_id': video_id,
                    'title': f'Video {video_id} (retry)',
                    'channel': 'Unknown',
                    'published': datetime.now().isoformat()
                }
                
                if self.process_video(video_info, is_retry=True):
                    total += 1
                time.sleep(self.request_delay)
        
        # Then process new videos
        print(f"\n{'*'*80}\n📥 FETCHING NEW VIDEOS\n{'*'*80}")
        
        for handle in self.channel_handles:
            try:
                print(f"\n{'#'*80}\nChannel: {handle}\n{'#'*80}")
                videos = self.get_channel_videos_by_handle(handle, max_results=5)
                all_videos.extend(videos)
                print(f"  Found {len(videos)} videos")
            except Exception as e:
                print(f"  Error: {e}")
        
        unique_videos = {v['video_id']: v for v in all_videos}.values()
        unprocessed_videos = [v for v in unique_videos if v['video_id'] not in self.processed_videos_cache or v['video_id'] in self.error_videos]
        
        print(f"\n📊 Total unique videos: {len(unique_videos)}")
        print(f"📊 Already processed: {len(unique_videos) - len(unprocessed_videos)}")
        print(f"📊 To process: {len(unprocessed_videos)}")
        
        if unprocessed_videos:
            estimated_time = len(unprocessed_videos) * (self.request_delay * 2 + 5)
            print(f"⏱️  Estimated time: ~{estimated_time // 60} minutes")
        
        for video in unprocessed_videos:
            if self.process_video(video):
                total += 1
            # Additional delay between videos
            time.sleep(self.request_delay)
        
        print(f"\n{'='*80}\nCOMPLETE - Processed {total} videos (new + retries)\n{'='*80}")
        
        if self.error_videos:
            print(f"⚠️  {len(self.error_videos)} videos still have errors - run again to retry")


if __name__ == "__main__":
    YouTubeCreatorEconomyAutomation().run()
