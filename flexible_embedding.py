# flexible_embedding.py
import json
import re
import time
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple, Iterable

import requests
from openai import OpenAI
import openai


# ===================== Parsing helpers =====================

def _parse_key_list(raw: Optional[str]) -> List[str]:
    """
    Accepts:
      - JSON array string: '["caption","metadata.title"]'
      - JSON string: '"caption, metadata.title"'
      - CSV string:  'caption, metadata.title'
    Returns lower-cased, trimmed list of tokens/patterns.
    """
    if not raw:
        return []
    s = str(raw).strip()
    tokens: Iterable[str] = []
    try:
        parsed = json.loads(s)
        if isinstance(parsed, list):
            tokens = parsed
        elif isinstance(parsed, str):
            tokens = parsed.split(",")
        else:
            tokens = [s]
    except Exception:
        tokens = s.split(",")

    out: List[str] = []
    for t in tokens:
        tok = str(t).strip().strip('\'"').lower()
        if tok:
            out.append(tok)
    return out


def _token_is_simple_name(tok: str) -> bool:
    """
    A 'simple name' has no dot, no [index], and no wildcard characters.
    e.g., 'crawl', 'title', 'property' are simple; 'a.b', 'a[*].b', 'a*' are not.
    """
    return all(x not in tok for x in ('.', '[', ']', '*'))


def _compile_patterns(raw_tokens: List[str]) -> List[re.Pattern]:
    """
    Build compiled regex patterns implementing the semantics:

    - If token is a complex path (has '.', '[', or '*'):
        Use as-is with wildcards:
          *   -> .*
          [*] -> [\\d+]
      Match is FULL path (re.fullmatch).

    - If token is a simple name, like 'crawl':
        Interpret as TWO intentions:
          (A) subtree root     -> ^crawl(\\..*|\\[\\d+\\].*)?$
          (B) leaf-name anywhere-> (?:^|.*[.\\]])crawl$
        Both patterns are used (so 'crawl' drops subtree; 'loadedurl' drops leafs named 'loadedUrl' anywhere).

    All matches are case-insensitive (paths are lower-cased before matching).
    """
    compiled: List[re.Pattern] = []
    for tok in raw_tokens:
        if _token_is_simple_name(tok):
            # (A) As subtree root
            subtree_regex = rf"^{re.escape(tok)}(?:\..*|\[\d+\].*)?$"
            compiled.append(re.compile(subtree_regex))
            # (B) As leaf name anywhere (end of path segment or [...].leaf)
            leaf_regex = rf"(?:^|.*[.\]]){re.escape(tok)}$"
            compiled.append(re.compile(leaf_regex))
        else:
            pat = tok
            esc = re.escape(pat)
            # translate wildcards
            esc = esc.replace(r'\[\*\]', r'\[\d+\]')  # [*] => [number]
            esc = esc.replace(r'\*', r'.*')           # *   => .*
            compiled.append(re.compile(rf"{esc}"))
    return compiled


def _match_any(path_lc: str, patterns: List[re.Pattern]) -> bool:
    return any(p.fullmatch(path_lc) for p in patterns)


# ===================== Generic utils =====================

def _stringify(v: Any) -> str:
    if isinstance(v, (str, int, float, bool)):
        return str(v)
    if v is None:
        return ""
    try:
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return str(v)


def chunk_text(text: str, chunk_size: int, chunk_overlap: int) -> List[str]:
    chunk_size = int(chunk_size)
    chunk_overlap = int(chunk_overlap)
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    if chunk_overlap < 0:
        raise ValueError("chunk_overlap must be >= 0")
    if chunk_overlap >= chunk_size:
        chunk_overlap = max(0, chunk_size // 10)

    chunks = []
    n = len(text)
    start = 0
    step = chunk_size - chunk_overlap
    while start < n:
        end = min(start + chunk_size, n)
        piece = text[start:end]
        if piece and piece.strip():
            chunks.append(piece)
        if end == n:
            break
        start += step
    return chunks


_YT_ID_RE = re.compile(r'^[A-Za-z0-9_\-]{11}$')

def format_metadata_value(key: str, value: Any):
    if value is None:
        return ""
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
        return resp.content.decode(resp.encoding or 'utf-8', errors='replace')
    except Exception as e:
        raise RuntimeError(f"Failed to download file: {e}")


# ===================== Flatten to dot-paths =====================

def flatten_item(obj: Any, parent: str = "") -> Dict[str, str]:
    """
    Flattens dicts/lists to dot/idx paths:
      dict:  parent.child
      list:  parent[0], parent[1], ...
    Returns { path: stringified_value } with ORIGINAL path casing preserved.
    """
    out: Dict[str, str] = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = f"{parent}.{k}" if parent else str(k)
            out.update(flatten_item(v, key))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            key = f"{parent}[{i}]"
            out.update(flatten_item(v, key))
    else:
        out[parent] = _stringify(obj)
    return out

# ====================== Path utilities ======================

def _extract_root(path: str) -> str:
    """
    'metadata.title'        -> 'metadata'
    'metadata.openGraph[0]' -> 'metadata'
    'crawl.loadedUrl'       -> 'crawl'
    'url'                   -> 'url'
    """
    if not path:
        return ""
    p = path
    # split on first '.' or '['
    dot = p.find('.')
    brk = p.find('[')
    cut = len(p)
    if dot != -1:
        cut = min(cut, dot)
    if brk != -1:
        cut = min(cut, brk)
    return p[:cut]

def _is_related_to_root(meta_key: str, root: str) -> bool:
    """
    meta_key may be a path (e.g., 'metadata.title') or a mapped out-key ('title').
    If it's a path: related iff it starts under the same root.
    If it's an out-key (no '.' and no '['): we decide via origin map (handled elsewhere).
    Here we only handle path-like keys.
    """
    if not meta_key:
        return False
    if ('.' in meta_key) or ('[' in meta_key):
        mk_lc = meta_key.lower()
        r_lc  = root.lower()
        return mk_lc == r_lc or mk_lc.startswith(r_lc + '.') or mk_lc.startswith(r_lc + '[')
    # non-path keys handled by origin matching in the filter function below
    return False


def _leaf_name(path: str) -> str:
    """
    Convert any path like 'metadata.data[0].robots' -> 'robots'
    """
    p = re.sub(r"\[\d+\]", "", path)  # strip [0], [12], ...
    return p.split(".")[-1].lower()


def _compress_meta_keys(meta: Dict[str, Any],
                        meta_origin: Dict[str, str],
                        mode: str = "leaf") -> Tuple[Dict[str, Any], Dict[str, str]]:
    """
    mode == 'leaf' হলে path-like key ('a.b[0].c') -> শুধু leaf 'c'
    collision হলে suffix (_2, _3 ...) দিয়ে ইউনিক রাখা হয়।
    mapped out-key (metadata_map দিয়ে আসা key) অপরিবর্তিত থাকে।
    """
    if mode != "leaf":
        return meta, meta_origin

    new_meta: Dict[str, Any] = {}
    new_origin: Dict[str, str] = {}

    for k, v in meta.items():
        path_like = ('.' in k) or ('[' in k)
        if path_like:
            leaf = _leaf_name(k)
            nk = leaf
            i = 2
            while nk in new_meta:
                nk = f"{leaf}_{i}"
                i += 1
            new_meta[nk] = v
            new_origin[nk] = meta_origin.get(k, k)
        else:
            # mapped out-keys or simple keys 그대로 রাখি
            new_meta[k] = v
            new_origin[k] = meta_origin.get(k, k)

    return new_meta, new_origin



# ===================== Decision (path-aware) =====================

def decide_chunk_or_meta_for_item_paths(
    item: Dict[str, Any],
    threshold: int,
    force_chunk_patterns: List[re.Pattern],
    force_meta_patterns: List[re.Pattern],
    exclude_chunk_patterns: List[re.Pattern],
    exclude_meta_patterns: List[re.Pattern],
    metadata_map: Dict[str, str],
) -> Tuple[List[Tuple[str, str]], Dict[str, Any], Dict[str, str]]:
    """
    Returns:
      - chunk_sources: list of (path, text)
      - meta:          dict of { meta_key_or_path: text }
      - meta_origin:   dict of { meta_key_or_path: source_path_for_this_meta }
                       (for path-like meta, origin == that path; for mapped out-keys, origin == mapped source path)
    """
    flat = flatten_item(item)
    flat_lc = {k.lower(): (k, v) for k, v in flat.items()}

    chunk_sources: List[Tuple[str, str]] = []
    meta: Dict[str, Any] = {}
    meta_origin: Dict[str, str] = {}

    for path_lc, (path, text) in flat_lc.items():
        ex_chunk = _match_any(path_lc, exclude_chunk_patterns)
        ex_meta  = _match_any(path_lc, exclude_meta_patterns)
        if ex_chunk and ex_meta:
            continue  # drop entirely

        if _match_any(path_lc, force_chunk_patterns) and not ex_chunk:
            if text.strip():
                chunk_sources.append((path, text))
            continue
        if _match_any(path_lc, force_meta_patterns) and not ex_meta:
            meta[path] = text
            meta_origin[path] = path  # path-like meta
            continue

        if not ex_chunk and len(text) > threshold:
            if text.strip():
                chunk_sources.append((path, text))
        else:
            if not ex_meta:
                meta[path] = text
                meta_origin[path] = path

    # metadata_map: out_key -> source_path
    if metadata_map:
        for out_key, source_path in metadata_map.items():
            sp = str(source_path).lower().strip()
            src = flat_lc.get(sp)
            if not src:
                # unique tail fallback
                tail = sp.split(".")[-1]
                candidates = [kv for k, kv in flat_lc.items() if k.endswith("." + tail) or k.endswith("]" + "." + tail)]
                if len(candidates) == 1:
                    src = candidates[0]
            if not src:
                continue
            path, val = src
            if _match_any(path.lower(), exclude_meta_patterns):
                continue
            meta[out_key] = format_metadata_value(out_key, val)
            meta_origin[out_key] = path  # mapped meta remembers its source path

    return chunk_sources, meta, meta_origin



# ===================== Background task (public) =====================

def process_flexible_task(app, data):
    """
    Flexible, nested, wildcard-aware embedding.

    Input fields (from form-data):
      - openai_key (header)       : x-openai-api-key
      - supabase_url (header)     : x-supabase-url
      - supabase_key (header)     : x-supabase-key
      - file_url                  : json_url
      - supabase_table            : default 'documents'
      - chunk_size                : default 500
      - chunk_overlap             : default 50
      - metadata_map (JSON str)   : e.g. {"title":"metadata.title"}
      - threshold                 : default 70
      - force_chunk_keys          : CSV or JSON array (dot-path/wildcards supported)
      - force_meta_keys           : CSV or JSON array
      - exclude_chunk_keys        : CSV or JSON array
      - exclude_meta_keys         : CSV or JSON array

    Semantics:
      - Simple token 'crawl' => drop subtree (root) + any leaf named 'crawl'
      - Simple token 'loadedUrl' => drop any leaf named 'loadedUrl' anywhere
      - Complex token 'a.b', 'a[*].c', 'a.*' => use as explicit path pattern
      - Priority: exclude > force > threshold
      - No empty rows inserted (if no non-empty chunks)
    """
    with app.app_context():
        try:
            openai_key     = data["openai_key"]
            supabase_url   = data["supabase_url"]
            supabase_key   = data["supabase_key"]
            file_url       = data["file_url"]

            chunk_size     = int(data.get("chunk_size", 500))
            chunk_overlap  = int(data.get("chunk_overlap", 50))
            supabase_table = data.get("supabase_table", "documents")
            metadata_map   = data.get("metadata_map", {}) or {}
            
            meta_key_mode   = data.get("meta_key_mode", "leaf")

            threshold         = int(data.get("threshold", 70))
            force_chunk_raw   = _parse_key_list(data.get("force_chunk_keys"))
            force_meta_raw    = _parse_key_list(data.get("force_meta_keys"))
            exclude_chunk_raw = _parse_key_list(data.get("exclude_chunk_keys"))
            exclude_meta_raw  = _parse_key_list(data.get("exclude_meta_keys"))
            global_meta_raw = _parse_key_list(data.get("global_meta_keys"))

            # Compile matchers once per request
            force_chunk_patterns   = _compile_patterns(force_chunk_raw)
            force_meta_patterns    = _compile_patterns(force_meta_raw)
            exclude_chunk_patterns = _compile_patterns(exclude_chunk_raw)
            exclude_meta_patterns  = _compile_patterns(exclude_meta_raw)

            if not openai_key or not supabase_url or not supabase_key or not file_url:
                raise ValueError("Missing required configuration values (keys, supabase, or file_url).")

            json_str  = download_json_from_url(file_url)
            json_data = json.loads(json_str)

            # Accept array or single object
            if isinstance(json_data, dict):
                json_data = [json_data]
            elif not isinstance(json_data, list):
                raise ValueError("Input JSON must be an object or an array of objects.")

            max_batch_size       = 128
            supabase_batch_size  = 100

            batched_chunks: List[str] = []
            batched_meta_list: List[Dict[str, Any]] = []
            total_uploaded = 0
            sess = requests.Session()

            for item in json_data:
                chunk_sources, base_meta, meta_origin = decide_chunk_or_meta_for_item_paths(
                    item,
                    threshold,
                    force_chunk_patterns, force_meta_patterns,
                    exclude_chunk_patterns, exclude_meta_patterns,
                    metadata_map
                )

                if not chunk_sources:
                    continue  # no empty rows

                for src_path, text in chunk_sources:
                    root = _extract_root(src_path)
                    for c in chunk_text(text, chunk_size, chunk_overlap):
                        if not c or not c.strip():
                            continue

                        # --- keep ALL meta from this item (no root filter) ---
                        related_meta = dict(base_meta)

                        # --- add/override explicit global meta keys if requested (optional, same logic) ---
                        for g in global_meta_raw:
                            gl = g.lower()
                            for bk, bv in base_meta.items():
                                # leaf match or full-path match
                                if _leaf_name(bk) == gl or bk.lower() == gl:
                                    related_meta[g] = bv

                        # Add trace field (ONLY source_field; DO NOT save source_root)
                        related_meta["source_field"] = src_path

                        # compress path-like meta keys to leaf names (e.g., 'metadata.data[0].robots' -> 'robots')
                        related_meta, _ = _compress_meta_keys(related_meta, meta_origin, mode=meta_key_mode)


                        batched_chunks.append(c)
                        batched_meta_list.append(related_meta)

                        if len(batched_chunks) >= max_batch_size:
                            embeddings = batch_embed_text(
                                batched_chunks, openai_key, app, max_batch_size=max_batch_size
                            )
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

            # flush remainder
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


