from flask import Flask, request, jsonify, send_file
from vector_embedding import process_upload_request
from transcript_to_docs import generate_zip_from_transcript
from error_log import error_log_bp, logger  # Import error log route and logger

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024  # 200MB max upload size

# Attach the external logger to app
app.logger.handlers = logger.handlers
app.logger.setLevel(logger.level)

# === Register Blueprints ===
app.register_blueprint(error_log_bp)

# === Health Check ===
@app.route("/ping", methods=["GET"])
def ping():
    return jsonify({"message": "API is working!"}), 200

# === Embedding Upload API ===
@app.route("/upload-flexible", methods=["POST"])
def upload_flexible():
    return process_upload_request(request)

# === Transcript to Docs + ZIP API ===
@app.route("/generate-docs", methods=["POST"])
def generate_docs():
    return generate_zip_from_transcript(request)

@app.route("/force-error")
def force_error():
    raise ValueError("Test error logging from /force-error")

# === Run Server ===
if __name__ == '__main__':
    app.run(debug=False, port=5000)
