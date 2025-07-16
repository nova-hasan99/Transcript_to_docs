# error_log.py


# ============================================
#  Error Log Endpoint Usage Examples
# ============================================
# | Purpose                     | URL                       |
# |----------------------------|---------------------------|
# | 🔹 Last 20 lines (default) | /error-log                |
# | 🔹 Last 10 lines           | /error-log?lines=10       |
# | 🔹 Last 5 minutes          | /error-log?minutes=5      |
# | 🔹 Last 1 hour             | /error-log?minutes=60     |
# | 🔹 Last 50 lines           | /error-log?lines=50       |
# ============================================



import logging
import os
from flask import Blueprint, jsonify

log_file_path = 'logs/flask.log'
os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

logger = logging.getLogger('flask_error_logger')
logger.setLevel(logging.ERROR)

file_handler = logging.FileHandler(log_file_path)
file_handler.setLevel(logging.ERROR)

formatter = logging.Formatter('[%(asctime)s] %(levelname)s in %(module)s: %(message)s')
file_handler.setFormatter(formatter)

if not logger.hasHandlers():
    logger.addHandler(file_handler)

error_log_bp = Blueprint("error_log", __name__)

@error_log_bp.route('/error-log', methods=["GET"])
def read_error_log():
    try:
        with open(log_file_path, "r") as f:
            lines = f.readlines()
            return jsonify({
                "log_count": len(lines),
                "logs": lines[-50:]  # last 50 lines
            })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
