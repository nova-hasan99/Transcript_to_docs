from flask import Flask, request, jsonify
from transcript_to_docs import generate_zip_from_transcript
from error_log import error_log_bp
from vector_embedding import process_upload_task
from threading import Thread
import json

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024  # 200MB max upload

# === Register Error Log Blueprint ===
app.register_blueprint(error_log_bp)

# === Health Check ===
@app.route("/ping")
def ping():
    return jsonify({"message": "pong"}), 200

# === Upload & Embed JSON into Supabase Vector Store ===
@app.route("/upload-flexible", methods=["POST"])
def upload_flexible():
    try:
        # Step 1: Collect data from headers + form
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

        # Step 2: Start background thread
        thread = Thread(target=process_upload_task, args=(data,))
        thread.start()

        # Step 3: Respond early to avoid Gunicorn timeout
        return jsonify({
            "status": "accepted",
            "message": "Embedding process started in background",
            "note": "Check /error-log for updates"
        }), 202

    except Exception as e:
        app.logger.error(f"Upload request failed: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500

# === DOCX Generator from Transcript JSON ===
@app.route("/generate-docs", methods=["POST"])
def generate_docs():
    return generate_zip_from_transcript(request)

# === Force an error for testing ===
@app.route("/force-error")
def force_error():
    app.logger.error("Intentional error raised from /force-error")
    raise ValueError("Forced test error for logging")

if __name__ == '__main__':
    app.run(debug=False, port=5000)
