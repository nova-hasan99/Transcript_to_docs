from flask import Flask, request, jsonify
from transcript_to_docs import generate_zip_from_transcript
from error_log import error_log_bp
from vector_embedding import process_upload_task
from flexible_embedding import process_flexible_task
from threading import Thread
import json

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024  # 200MB max upload

app.register_blueprint(error_log_bp)

@app.route("/ping")
def ping():
    return jsonify({"message": "pong"}), 200

@app.route("/upload-flexible", methods=["POST"])
def upload_flexible():
    try:
        data = {
            "openai_key": request.headers.get('x-openai-api-key'),
            "supabase_url": request.headers.get('x-supabase-url'),
            "supabase_key": request.headers.get('x-supabase-key'),
            "file_url": request.form.get('json_url'),
            "content_field": request.form.get('content_field', 'captions'),
            "chunk_size": request.form.get('chunk_size', 500),
            "chunk_overlap": request.form.get('chunk_overlap', 50),
            "supabase_table": request.form.get('supabase_table', 'documents'),
            "metadata_map": json.loads(request.form.get('metadata') or '{}')
        }

        # 👇 app context send as argument
        thread = Thread(target=process_upload_task, args=(app, data))
        thread.start()

        return jsonify({
            "status": "accepted",
            "message": "Embedding process started in background",
            "note": "Check /error-log for updates"
        }), 202

    except Exception as e:
        app.logger.error(f"Upload request failed: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500
    
# ---- ensure this import is present near your other imports ----
@app.route("/upload-flexible-smart", methods=["POST"])
def upload_flexible_smart():
    try:
        missing = []
        if not request.headers.get('x-openai-api-key'): missing.append('x-openai-api-key')
        if not request.headers.get('x-supabase-url'):  missing.append('x-supabase-url')
        if not request.headers.get('x-supabase-key'):  missing.append('x-supabase-key')
        if not request.form.get('json_url'):           missing.append('json_url')
        if missing:
            return jsonify({"error": "Missing required fields", "missing": missing}), 400

        data = {
            "openai_key": request.headers.get('x-openai-api-key'),
            "supabase_url": request.headers.get('x-supabase-url'),
            "supabase_key": request.headers.get('x-supabase-key'),
            "file_url": request.form.get('json_url'),

            # same knobs as before
            "chunk_size": request.form.get('chunk_size', 500),
            "chunk_overlap": request.form.get('chunk_overlap', 50),
            "supabase_table": request.form.get('supabase_table', 'documents'),
            "metadata_map": json.loads(request.form.get('metadata') or '{}'),

            # dynamic knobs
            "threshold": request.form.get('threshold', 70),
            "force_chunk_keys": request.form.get('force_chunk_keys', ''),
            "force_meta_keys": request.form.get('force_meta_keys', ''),
            "global_meta_keys": request.form.get('global_meta_keys', ''),

            # NEW: hard excludes/blocklists
            "exclude_chunk_keys": request.form.get('exclude_chunk_keys', ''),  # keys that must NEVER be chunked
            "exclude_meta_keys": request.form.get('exclude_meta_keys', ''),    # keys that must NEVER be metadata
            
            "meta_key_mode": request.form.get('meta_key_mode', 'leaf'),
        }

        th = Thread(target=process_flexible_task, args=(app, data), name="flex-embed-uploader")
        th.daemon = True
        th.start()

        return jsonify({
            "status": "accepted",
            "message": "Flexible embedding started in background",
            "note": "Check /error-log for updates"
        }), 202

    except Exception as e:
        app.logger.error(f"Upload smart request failed: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500



@app.route("/generate-docs", methods=["POST"])
def generate_docs():
    return generate_zip_from_transcript(request)

@app.route("/force-error")
def force_error():
    app.logger.error("Intentional error raised from /force-error")
    raise ValueError("Forced test error for logging")

if __name__ == '__main__':
    app.run(debug=False, port=5000)
