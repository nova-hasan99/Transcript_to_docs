import requests
import json
from openai import OpenAI

EMBED_MODEL = "text-embedding-3-small"

def embed_text(text: str, api_key: str):
    """Generate OpenAI embedding"""
    client = OpenAI(api_key=api_key)
    resp = client.embeddings.create(model=EMBED_MODEL, input=text)
    return resp.data[0].embedding

def call_supabase_rpc(supabase_url, supabase_key, rpc_name, embedding_vector, match_count):
    """Call Supabase RPC function dynamically"""
    url = f"{supabase_url.rstrip('/')}/rest/v1/rpc/{rpc_name}"
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload = {
        "query_embedding": embedding_vector,
        "match_count": match_count
    }
    r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=60)
    r.raise_for_status()
    return r.json()
