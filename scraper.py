import sys, io, os
from datetime import datetime, timezone
from googleapiclient.discovery import build
from supabase import create_client, Client
from dotenv import load_dotenv

# Fix for potential encoding issues
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
load_dotenv(dotenv_path='.env.local')

# --- CREDENTIALS ---
YT_API_KEY = "AIzaSyBaAkbADa5vxD1AaQU0ykSFYJmudN-B_VI"
SB_URL = "https://djgwkmlwktgohqregxpf.supabase.co"
SB_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImRqZ3drbWx3a3Rnb2hxcmVneHBmIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjY5MTU5MTYsImV4cCI6MjA4MjQ5MTkxNn0.lAzrKKSJ9ajGjT3CkUntvacMGmsywUBxR7QX7UfpCm8"

supabase: Client = create_client(SB_URL, SB_KEY)
youtube = build('youtube', 'v3', developerKey=YT_API_KEY)

def update_status(keyword, status, progress, current_video=""):
    try:
        supabase.table("scrape_status").upsert({
            "id": 1,
            "keyword": keyword,
            "status": status,
            "progress": progress,
            "current_video": current_video
        }).execute()
    except Exception as e:
        print(f"Status Sync Error: {e}")

def run_automated_hunt(keyword, lang="en"):
    print(f"--- AUTO-HUNT START: {keyword} ---")
    update_status(keyword, "Bot Initializing...", 10)
    
    try:
        # 1. Fetch search results
        search = youtube.search().list(q=keyword, part='id', type='video', maxResults=20, relevanceLanguage=lang).execute()
        v_ids = [item['id']['videoId'] for item in search.get('items', [])]
        
        if not v_ids:
            update_status(keyword, "No results found.", 0)
            return

        # 2. Get detailed stats
        v_res = youtube.videos().list(part='statistics,snippet', id=",".join(v_ids)).execute()
        
        # 3. Get channel details for outlier math
        c_ids = [v['snippet']['channelId'] for v in v_res.get('items', [])]
        c_res = youtube.channels().list(part='statistics,snippet', id=",".join(c_ids)).execute()
        channels = {c['id']: c for c in c_res.get('items', [])}

        for i, v in enumerate(v_res.get('items', [])):
            v_id = v['id']
            stats = v['statistics']
            snippet = v['snippet']
            c_id = snippet['channelId']
            
            # Math
            pub_date = datetime.strptime(snippet['publishedAt'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            days_old = max((datetime.now(timezone.utc) - pub_date).total_seconds() / 86400, 0.05)
            views = int(stats.get('viewCount', 0))
            velocity = round(views / days_old, 2)

            c_data = channels.get(c_id, {})
            subs = int(c_data.get('statistics', {}).get('subscriberCount', 1))
            score = round(views / max(subs, 1), 2)

            # Upsert (Update if exists, Insert if new)
            payload = {
                "id": v_id,
                "title": snippet.get('title'),
                "views": views,
                "outlier_score": score,
                "velocity": velocity,
                "thumbnail_url": snippet['thumbnails']['high']['url'],
                "channel_name": c_data.get('snippet', {}).get('title', 'Unknown'),
                "channel_id": c_id,
                "channel_subs": subs,
                "tags": ",".join(snippet.get('tags', []))[:500]
            }
            
            supabase.table("videos").upsert(payload).execute()
            
            # Progress update
            percent = 30 + int(((i + 1) / len(v_ids)) * 70)
            update_status(keyword, f"Auto-Syncing {i+1}/{len(v_ids)}", percent, snippet.get('title'))

        update_status(keyword, "Idle (Auto-Hunt Finished)", 100)
        print("--- AUTO-HUNT COMPLETE ---")

    except Exception as e:
        update_status(keyword, f"Bot Error: {str(e)}", 0)

if __name__ == "__main__":
    # If running manually via 'python scraper.py "niche name"'
    target = sys.argv[1] if len(sys.argv) > 1 else "trending"
    run_automated_hunt(target)