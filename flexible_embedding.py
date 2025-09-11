# flexible_embedding.py
import json
import re
import time
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

import requests
from openai import OpenAI
import openai

# ==================== Helpers & Utilities ====================

def _parse_key_list(raw: Optional[str]) -> set:
    """
    Accepts either a JSON array string '["caption","description"]'
    or a comma-separated string 'caption, description'
    Returns a lower-cased trimmed set of keys.
    """
    if not raw:
        return set()
    raw = raw.strip()
    try:
        data = json.loads(raw)
        if isinstance(data, list):
            return {str(x).strip().lower() for x in data if str(x).strip()}
    except Exception:
        pass
    # CSV fallback
    return {x.strip().lower() for x in raw.split(",") if x.strip()}

def sanitize(text):
    return re.sub(r'[\\/*?:"<>|\']+', '', str(text))[:100]

def chunk_text(text: str, chunk_size: int, chunk_overlap: int) -> List[str]:
    chunk_size = int(chunk_size)
    chunk_overlap = int(chunk_overlap)
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    if chunk_overlap < 0:
        raise ValueError("chunk_overlap must be >= 0")
    if chunk_overlap >= chunk_size:
        # guard: keep at most 10% overlap if misconfigured
        chunk_overlap = max(0, chunk_size // 10)

    chunks = []
    start = 0
    n = len(text)
    step = chunk_size - chunk_overlap
    while start < n:
        end = min(start + chunk_size, n)
        piece = text[start:end]
        if piece and piece.strip():  # skip empty/whitespace-only chunks
            chunks.append(piece)
        if end == n:
            break
        start += step
    return chunks

_YT_ID_RE = re.compile(r'^[A-Za-z0-9_\-]{11}$')

def format_metadata_value(key: str, value: Any):
    if value is None:
        return ""
    # Example: if someone maps "youtube_id" we auto-expand to URL
    if key.lower() in {"youtube_id", "video_id"} and isinstance(value, str) and _YT_ID_RE.match(value):
        return f"https://www.youtube.com/watch?v={value}"
    return value

def _mk_openai_client(api_key: str) -> OpenAI:
    return OpenAI(api_key=api_key)

def batch_embed_text(
    chunks: List[str],
    openai_key: str,
    app,
    max_batch_size: int = 128,
    max_retries: int = 5,
    model: str = "text-embedding-3-small",
) -> List[List[float]]:
    client = _mk_openai_client(openai_key)
    embeddings: List[List[float]] = []

    for i in range(0, len(chunks), max_batch_size):
        batch = chunks[i:i + max_batch_size]
        attempt = 0
        backoff = 4
        while True:
            try:
                resp = client.embeddings.create(model=model, input=batch)
                embeddings.extend([d.embedding for d in resp.data])
                break
            except (openai.RateLimitError, openai.APIError, openai.APIConnectionError, openai.InternalServerError) as e:
                if attempt >= max_retries:
                    app.logger.error(f"[OpenAI] Gave up after {max_retries} retries on batch starting {i}: {e}")
                    # keep alignment (1536 dims for text-embedding-3-small)
                    embeddings.extend([[0.0] * 1536] * len(batch))
                    break
                app.logger.warning(f"[OpenAI Retry {attempt+1}] {type(e).__name__}: {str(e)}")
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                attempt += 1
            except Exception as e:
                app.logger.error(f"[OpenAI Unexpected] {e}", exc_info=True)
                embeddings.extend([[0.0] * 1536] * len(batch))
                break

    return embeddings

def insert_into_supabase(
    supabase_url: str,
    supabase_key: str,
    table: str,
    records: List[Dict[str, Any]],
    app,
    batch_size: int = 100,
    max_retries: int = 5,
    session: Optional[requests.Session] = None,
):
    if not records:
        return
    url = f"{supabase_url.rstrip('/')}/rest/v1/{table}"
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }
    sess = session or requests.Session()

    for i in range(0, len(records), batch_size):
        sub_records = records[i:i + batch_size]
        attempt = 0
        backoff = 3
        while True:
            try:
                resp = sess.post(url, headers=headers, json=sub_records, timeout=30)
                if 200 <= resp.status_code < 300:
                    break
                app.logger.warning(f"[Supabase Retry {attempt+1}] {resp.status_code}: {resp.text[:500]}")
            except Exception as e:
                app.logger.warning(f"[Supabase Retry {attempt+1}] Exception: {str(e)}")
            if attempt >= max_retries:
                app.logger.error(f"[Supabase] Gave up after {max_retries} retries on batch starting {i}")
                break
            time.sleep(backoff)
            backoff = min(backoff * 2, 20)
            attempt += 1

def download_json_from_url(file_url: str) -> str:
    sess = requests.Session()
    try:
        resp = sess.get(file_url, timeout=60)
        resp.raise_for_status()
        # keep encoding if present
        return resp.content.decode(resp.encoding or 'utf-8', errors='replace')
    except Exception as e:
        raise RuntimeError(f"Failed to download file: {e}")

# ==================== Core Dynamic Split Logic ====================

def _stringify(v: Any) -> str:
    if isinstance(v, (str, int, float, bool)):
        return str(v)
    if v is None:
        return ""
    # list/dict → JSON string
    try:
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return str(v)

def decide_chunk_or_meta_for_item(
    item: Dict[str, Any],
    threshold: int,
    force_chunk_keys: set,
    force_meta_keys: set,
    exclude_chunk_keys: set,
    exclude_meta_keys: set,
    metadata_map: Dict[str, str],
) -> Tuple[List[Tuple[str, str]], Dict[str, Any]]:
    """
    Returns:
      - chunk_sources: list of (source_key, text) that must be chunked
      - meta: metadata dict

    Precedence (highest → lowest):
      A) EXCLUDES:
         - if key in exclude_chunk_keys → NEVER chunk (even if > threshold or forced)
         - if key in exclude_meta_keys  → NEVER metadata
         - if key in both excludes     → skip entirely (neither chunk nor meta)
      B) FORCES (only applied if not excluded):
         - key in force_chunk_keys → chunk (if non-empty)
         - key in force_meta_keys  → metadata
      C) DEFAULT:
         - len(stringified value) > threshold → chunk
         - else → metadata

    metadata_map applies on top (source-key based). If mapped source_key is in exclude_meta_keys,
    that mapping is skipped.
    """
    chunk_sources: List[Tuple[str, str]] = []
    meta: Dict[str, Any] = {}

    # map lowercase->original for case-insensitive lookups
    lower_item = {str(k).lower(): k for k in item.keys()}

    for orig_key, value in item.items():
        key_lc = str(orig_key).lower()
        text = _stringify(value)

        # ---- EXCLUDES take absolute priority ----
        in_excl_chunk = key_lc in exclude_chunk_keys
        in_excl_meta  = key_lc in exclude_meta_keys

        if in_excl_chunk and in_excl_meta:
            # drop entirely
            continue

        if key_lc in force_chunk_keys and not in_excl_chunk:
            if text.strip():
                chunk_sources.append((orig_key, text))
            continue  # don't also duplicate into metadata

        if key_lc in force_meta_keys and not in_excl_meta:
            meta[orig_key] = text
            continue

        # Default rule by length
        if not in_excl_chunk and len(text) > threshold:
            if text.strip():
                chunk_sources.append((orig_key, text))
        else:
            if not in_excl_meta:
                meta[orig_key] = text
            # else: excluded from metadata → drop

    # Apply metadata_map overrides (only if the SOURCE key is not excluded from metadata)
    if metadata_map:
        for out_key, source_key in metadata_map.items():
            sk_lc = str(source_key).lower()
            src_orig = lower_item.get(sk_lc, source_key)
            if sk_lc in exclude_meta_keys:
                # user explicitly blocked this source from metadata
                continue
            meta[out_key] = format_metadata_value(out_key, item.get(src_orig))

    return chunk_sources, meta

# ==================== Background Task (Public) ====================

def process_flexible_task(app, data):
    """
    Inputs:
      (same required headers & fields as /upload-flexible-smart)
      NEW fields:
        - threshold: int (default 70)
        - force_chunk_keys: CSV or JSON array of keys to ALWAYS chunk
        - force_meta_keys:  CSV or JSON array of keys to ALWAYS metadata
        - exclude_chunk_keys: CSV or JSON array of keys to NEVER chunk (even if > threshold or forced)
        - exclude_meta_keys:  CSV or JSON array of keys to NEVER metadata (drop from meta)

    Behavior:
      - If a field is in both exclude lists → dropped entirely.
      - If an item produces NO non-empty chunks → NO ROW is inserted (no empty rows).
    """
    with app.app_context():
        try:
            openai_key     = data["openai_key"]
            supabase_url   = data["supabase_url"]
            supabase_key   = data["supabase_key"]
            file_url       = data["file_url"]

            # existing knobs
            chunk_size     = int(data.get("chunk_size", 500))
            chunk_overlap  = int(data.get("chunk_overlap", 50))
            supabase_table = data.get("supabase_table", "documents")
            metadata_map   = data.get("metadata_map", {}) or {}

            # dynamic knobs
            threshold         = int(data.get("threshold", 70))
            force_chunk       = _parse_key_list(data.get("force_chunk_keys"))
            force_meta        = _parse_key_list(data.get("force_meta_keys"))
            exclude_chunk     = _parse_key_list(data.get("exclude_chunk_keys"))
            exclude_meta      = _parse_key_list(data.get("exclude_meta_keys"))

            if not openai_key or not supabase_url or not supabase_key or not file_url:
                raise ValueError("Missing required configuration values (keys, supabase, or file_url).")

            json_str  = download_json_from_url(file_url)
            json_data = json.loads(json_str)

            max_batch_size       = 128
            supabase_batch_size  = 100

            batched_chunks: List[str] = []
            batched_meta_list: List[Dict[str, Any]] = []
            total_uploaded = 0
            sess = requests.Session()

            for item in json_data:
                chunk_sources, base_meta = decide_chunk_or_meta_for_item(
                    item, threshold,
                    force_chunk, force_meta,
                    exclude_chunk, exclude_meta,
                    metadata_map
                )

                # If there are no sources to chunk → SKIP (do NOT insert empty row)
                if not chunk_sources:
                    continue

                # For each chosen source field, chunk separately
                for src_key, text in chunk_sources:
                    chunks = chunk_text(text, chunk_size, chunk_overlap)
                    for c in chunks:
                        if not c or not c.strip():
                            continue  # skip empty chunk
                        batched_chunks.append(c)
                        per_chunk_meta = dict(base_meta)
                        per_chunk_meta["source_field"] = src_key
                        batched_meta_list.append(per_chunk_meta)

                        if len(batched_chunks) >= max_batch_size:
                            embeddings = batch_embed_text(
                                batched_chunks, openai_key, app, max_batch_size=max_batch_size
                            )
                            # Only non-empty chunks are here; safe to insert
                            records = [
                                {
                                    "content": batched_chunks[i],
                                    "metadata": batched_meta_list[i],
                                    "embedding": embeddings[i],
                                    "created_at": datetime.utcnow().isoformat()
                                }
                                for i in range(len(batched_chunks))
                            ]
                            insert_into_supabase(
                                supabase_url, supabase_key, supabase_table, records, app,
                                batch_size=supabase_batch_size, session=sess
                            )
                            total_uploaded += len(records)
                            batched_chunks.clear()
                            batched_meta_list.clear()

            # Flush remaining
            if batched_chunks:
                embeddings = batch_embed_text(batched_chunks, openai_key, app, max_batch_size=max_batch_size)
                records = [
                    {
                        "content": batched_chunks[i],
                        "metadata": batched_meta_list[i],
                        "embedding": embeddings[i],
                        "created_at": datetime.utcnow().isoformat()
                    }
                    for i in range(len(batched_chunks))
                ]
                insert_into_supabase(
                    supabase_url, supabase_key, supabase_table, records, app,
                    batch_size=supabase_batch_size, session=sess
                )
                total_uploaded += len(records)

            app.logger.info(f"[FLEX UPLOAD DONE] ✅ {total_uploaded} records uploaded.")

        except Exception as e:
            app.logger.error(f"[FLEX TASK ERROR] {str(e)}", exc_info=True)
