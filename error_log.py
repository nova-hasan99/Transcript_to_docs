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



from flask import Blueprint, request, jsonify
import subprocess

error_log_bp = Blueprint("error_log", __name__)

@error_log_bp.route('/error-log', methods=['GET'])
def get_error_log():
    try:
        # Optional query params
        lines = request.args.get('lines', default=None, type=int)
        minutes = request.args.get('minutes', default=None, type=int)

        # Build journalctl command
        cmd = ['journalctl', '-u', 'flask_api', '--no-pager']

        if minutes:
            cmd.extend(['--since', f'{minutes} minutes ago'])

        # Run the command
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, error = process.communicate()

        if process.returncode != 0:
            return jsonify({"error": error.decode('utf-8')}), 500

        log_lines = output.decode('utf-8').splitlines()

        # Apply line filter
        if lines:
            log_lines = log_lines[-lines:]
        elif not minutes:
            log_lines = log_lines[-20:]  # Default fallback

        return jsonify({"log": log_lines}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

