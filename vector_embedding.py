import json
import re
import time
from datetime import datetime
import openai
import requests

# ========= Utility Functions =========

def sanitize(text):
    return re.sub(r'[\\/*?:"<>|\']+', '', str(text))[:100]

def chunk_text(text, chunk_size, chunk_overlap):
    chunks = []
    start = 0
    while start < len(text):
        end = min(start + chunk_size, len(text))
        chunks.append(text[start:end])
        start += chunk_size - chunk_overlap
    return chunks

def format_metadata_value(key, value):
    if key.lower() == "url" and value:
        return f"https://www.youtube.com/watch?v={value}"
    return value or ""

def batch_embed_text(chunks, openai_key, app, max_batch_size=980, max_retries=5):
    openai.api_key = openai_key
    embeddings = []

    for i in range(0, len(chunks), max_batch_size):
        batch = chunks[i:i + max_batch_size]
        attempt = 0

        while attempt <= max_retries:
            try:
                response = openai.embeddings.create(
                    input=batch,
                    model="text-embedding-3-small"
                )
                batch_embeddings = [e.embedding for e in response.data]
                embeddings.extend(batch_embeddings)
                break
            except openai.error.RateLimitError as e:
                app.logger.warning(f"[OpenAI Retry {attempt+1}] RateLimit: {str(e)}")
                time.sleep(10)
                attempt += 1
            except Exception as e:
                app.logger.error(f"[OpenAI Retry {attempt+1}] Failed: {str(e)}", exc_info=True)
                time.sleep(10)
                attempt += 1

        if attempt > max_retries:
            app.logger.error(f"[OpenAI] Gave up after {max_retries} retries on batch {i}")

    return embeddings

def insert_into_supabase(supabase_url, supabase_key, table, records, app, batch_size=100, max_retries=5):
    url = f"{supabase_url}/rest/v1/{table}"
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }

    for i in range(0, len(records), batch_size):
        sub_records = records[i:i + batch_size]
        attempt = 0
        while attempt <= max_retries:
            try:
                response = requests.post(url, headers=headers, json=sub_records)
                if response.ok:
                    break
                else:
                    app.logger.warning(f"[Supabase Retry {attempt+1}] Failed: {response.text}")
            except Exception as e:
                app.logger.warning(f"[Supabase Retry {attempt+1}] Exception: {str(e)}")
            time.sleep(10)
            attempt += 1

        if attempt > max_retries:
            app.logger.error(f"[Supabase] Gave up after {max_retries} retries on batch {i}")

def download_json_from_url(file_url):
    try:
        response = requests.get(file_url)
        response.raise_for_status()
        return response.content.decode('utf-8')
    except Exception as e:
        raise RuntimeError(f"Failed to download file: {e}")

# ========= Background Task Handler =========

def process_upload_task(app, data):
    with app.app_context():
        try:
            openai_key = data["openai_key"]
            supabase_url = data["supabase_url"]
            supabase_key = data["supabase_key"]
            file_url = data["file_url"]
            content_field = data.get("content_field", "captions")
            chunk_size = int(data.get("chunk_size", 500))
            chunk_overlap = int(data.get("chunk_overlap", 50))
            supabase_table = data.get("supabase_table", "documents")
            metadata_map = data.get("metadata_map", {})

            json_str = download_json_from_url(file_url)
            json_data = json.loads(json_str)

            max_batch_size = 980
            supabase_batch_size = 100
            batch_chunks = []
            batch_metadata = []
            total_uploaded = 0

            for item in json_data:
                raw = item.get(content_field)
                content = raw.strip() if isinstance(raw, str) else ''
                metadata = {
                    k: format_metadata_value(k, item.get(v))
                    for k, v in metadata_map.items()
                } if metadata_map else {}

                if not content:
                    record = {
                        "content": None,
                        "metadata": metadata,
                        "embedding": None,
                        "created_at": datetime.utcnow().isoformat()
                    }
                    insert_into_supabase(supabase_url, supabase_key, supabase_table, [record], app)
                    total_uploaded += 1
                    continue

                chunks = chunk_text(content, chunk_size, chunk_overlap)

                for chunk in chunks:
                    batch_chunks.append(chunk)
                    batch_metadata.append(metadata)

                    if len(batch_chunks) >= max_batch_size:
                        embeddings = batch_embed_text(batch_chunks, openai_key, app)
                        records = [
                            {
                                "content": batch_chunks[i],
                                "metadata": batch_metadata[i],
                                "embedding": embeddings[i],
                                "created_at": datetime.utcnow().isoformat()
                            }
                            for i in range(len(batch_chunks))
                        ]
                        insert_into_supabase(supabase_url, supabase_key, supabase_table, records, app, supabase_batch_size)
                        total_uploaded += len(records)
                        batch_chunks, batch_metadata = [], []

            if batch_chunks:
                embeddings = batch_embed_text(batch_chunks, openai_key, app)
                records = [
                    {
                        "content": batch_chunks[i],
                        "metadata": batch_metadata[i],
                        "embedding": embeddings[i],
                        "created_at": datetime.utcnow().isoformat()
                    }
                    for i in range(len(batch_chunks))
                ]
                insert_into_supabase(supabase_url, supabase_key, supabase_table, records, app, supabase_batch_size)
                total_uploaded += len(records)

            app.logger.info(f"[UPLOAD DONE] ✅ {total_uploaded} records uploaded.")

        except Exception as e:
            app.logger.error(f"[TASK ERROR] {str(e)}", exc_info=True)
