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


import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
from flask import Blueprint, jsonify, request

error_log_bp = Blueprint('error_log', __name__)
log_dir = 'logs'
log_file_path = os.path.join(log_dir, 'flask.log')

# === Logging Setup ===
if not os.path.exists(log_dir):
    os.mkdir(log_dir)

log_handler = RotatingFileHandler(log_file_path, maxBytes=2 * 1024 * 1024, backupCount=1)
log_handler.setLevel(logging.ERROR)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s in %(module)s: %(message)s')
log_handler.setFormatter(formatter)

# Expose the handler for external use (like in main.py)
logger = logging.getLogger('flask_error_logger')
logger.setLevel(logging.ERROR)
logger.addHandler(log_handler)


# === /error-log endpoint ===
@error_log_bp.route("/error-log", methods=["GET"])
def get_error_log():
    if not os.path.exists(log_file_path):
        return jsonify({"error": "flask.log file not found"}), 404

    lines = request.args.get('lines', default=None, type=int)
    minutes = request.args.get('minutes', default=None, type=int)

    with open(log_file_path, 'r') as f:
        log_lines = f.readlines()

    result = []

    if minutes:
        cutoff = datetime.now() - timedelta(minutes=minutes)
        for line in reversed(log_lines):
            try:
                timestamp_str = line.split(']')[0].strip('[')
                timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S,%f')
                if timestamp >= cutoff:
                    result.insert(0, line.strip())
                else:
                    break
            except:
                continue
    elif lines:
        result = log_lines[-lines:]
    else:
        result = log_lines[-20:]  # default

    return jsonify({
        "log_count": len(result),
        "logs": result
    })

