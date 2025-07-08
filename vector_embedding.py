import json
import re
import time
from datetime import datetime
import openai
import requests
from flask import jsonify

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

def batch_embed_text(chunks, openai_key, max_batch_size=980, max_retries=5):
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
                error_message = str(e)
                print(f"Rate limit hit on batch {i}: {error_message}")
                wait_time = 10
                if 'try again in' in error_message:
                    try:
                        wait_time = float(error_message.split('try again in')[1].split('s')[0].strip())
                    except:
                        wait_time = 10
                time.sleep(max(wait_time, 10))
                attempt += 1
            except Exception as e:
                raise RuntimeError(f"OpenAI batch embedding failed at batch index {i}: {str(e)}")

        if attempt > max_retries:
            raise RuntimeError(f"Failed after {max_retries} retries on batch {i}")

    return embeddings

def insert_into_supabase(supabase_url, supabase_key, table, records):
    url = f"{supabase_url}/rest/v1/{table}"
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }
    response = requests.post(url, headers=headers, json=records)
    if not response.ok:
        raise RuntimeError(f"Supabase insert failed: {response.text}")

# ========= Main Handler =========

def process_upload_request(request):
    try:
        # Step 1: Headers
        openai_key = request.headers.get('x-openai-api-key')
        supabase_url = request.headers.get('x-supabase-url')
        supabase_key = request.headers.get('x-supabase-key')
        if not all([openai_key, supabase_url, supabase_key]):
            return jsonify({'error': 'Missing required headers'}), 400

        # Step 2: Binary JSON
        if 'json_data' not in request.files:
            return jsonify({'error': 'Missing binary file field: json_data'}), 400

        try:
            json_file = request.files['json_data']
            json_str = json_file.read().decode('utf-8')
            json_data = json.loads(json_str)
        except Exception as e:
            return jsonify({'error': 'Invalid JSON file', 'details': str(e)}), 400

        if not isinstance(json_data, list) or len(json_data) == 0:
            return jsonify({'error': 'Uploaded JSON must be a non-empty list'}), 400

        # Step 3: Optional Inputs
        content_field = request.form.get('content_field', 'captions')
        chunk_size = int(request.form.get('chunk_size', 500) or 500)
        chunk_overlap = int(request.form.get('chunk_overlap', 50) or 50)
        supabase_table = request.form.get('supabase_table', 'documents')

        metadata_map = {}
        metadata_raw = request.form.get('metadata')
        if metadata_raw:
            try:
                metadata_map = json.loads(metadata_raw)
            except Exception as e:
                return jsonify({'error': 'Invalid metadata JSON', 'details': str(e)}), 400

        # Step 4: Batch loop
        max_batch_size = 980
        batch_chunks = []
        batch_metadata = []
        total_uploaded = 0

        for item in json_data:
            raw = item.get(content_field)
            content = raw.strip() if isinstance(raw, str) else ''
            if not content:
                continue

            chunks = chunk_text(content, chunk_size, chunk_overlap)
            metadata = {
                k: format_metadata_value(k, item.get(v))
                for k, v in metadata_map.items()
            } if metadata_map else {}

            for chunk in chunks:
                batch_chunks.append(chunk)
                batch_metadata.append(metadata)

                if len(batch_chunks) >= max_batch_size:
                    embeddings = batch_embed_text(batch_chunks, openai_key)
                    records = [
                        {
                            "content": batch_chunks[i],
                            "metadata": batch_metadata[i],
                            "embedding": embeddings[i],
                            "created_at": datetime.utcnow().isoformat()
                        }
                        for i in range(len(batch_chunks))
                    ]
                    insert_into_supabase(supabase_url, supabase_key, supabase_table, records)
                    total_uploaded += len(records)
                    batch_chunks, batch_metadata = [], []

        # Final leftover batch
        if batch_chunks:
            embeddings = batch_embed_text(batch_chunks, openai_key)
            records = [
                {
                    "content": batch_chunks[i],
                    "metadata": batch_metadata[i],
                    "embedding": embeddings[i],
                    "created_at": datetime.utcnow().isoformat()
                }
                for i in range(len(batch_chunks))
            ]
            insert_into_supabase(supabase_url, supabase_key, supabase_table, records)
            total_uploaded += len(records)

        return jsonify({"status": "success", "records_uploaded": total_uploaded}), 200

    except Exception as e:
        print("ERROR:", e)
        return jsonify({"error": "Unexpected failure", "details": str(e)}), 500
