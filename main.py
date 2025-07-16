from flask import Flask, request, jsonify, send_file
from vector_embedding import process_upload_request
from transcript_to_docs import generate_zip_from_transcript
from error_log import error_log_bp, logger

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024

# Attach custom logger
for handler in logger.handlers:
    app.logger.addHandler(handler)
app.logger.setLevel(logger.level)

app.register_blueprint(error_log_bp)

@app.route("/ping")
def ping():
    return jsonify({"message": "pong"}), 200

@app.route("/upload-flexible", methods=["POST"])
def upload_flexible():
    return process_upload_request(request)

@app.route("/generate-docs", methods=["POST"])
def generate_docs():
    return generate_zip_from_transcript(request)

@app.route("/force-error")
def force_error():
    app.logger.error("Intentional error raised from /force-error")
    raise ValueError("Forced test error for logging")

if __name__ == '__main__':
    app.run(debug=False, port=5000)  # ✅ must be debug=False
