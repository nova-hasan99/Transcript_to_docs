from flask import Flask, request, jsonify, send_file
from vector_embedding import process_upload_request
from transcript_to_docs import generate_zip_from_transcript
from error_log import error_log_bp

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024  # 200MB max upload

# === Register Error Log Blueprint ===
app.register_blueprint(error_log_bp)

# === Health Check Endpoint ===
@app.route("/ping")
def ping():
    return jsonify({"message": "pong"}), 200

# === Upload JSON and process embeddings (Flexible + Celery Ready) ===
@app.route("/upload-flexible", methods=["POST"])
def upload_flexible():
    return process_upload_request(request)

# === Transcript to DOCX Generator ===
@app.route("/generate-docs", methods=["POST"])
def generate_docs():
    return generate_zip_from_transcript(request)

# === Force an Error (for testing logs) ===
@app.route("/force-error")
def force_error():
    app.logger.error("Intentional error raised from /force-error")
    raise ValueError("Forced test error for logging")

if __name__ == '__main__':
    app.run(debug=False, port=5000)  # Always keep debug=False in production
