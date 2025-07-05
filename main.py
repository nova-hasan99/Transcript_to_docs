from flask import Flask, request, jsonify, send_file
from docx import Document
import os
import re
import io
import zipfile
import json

app = Flask(__name__)

# Function to sanitize file or folder names
def sanitize_filename(name):
    return re.sub(r'[\\/*?:"<>|\']+', '', name).strip()[:50]

# Function to make keys more readable
def format_key(key):
    return key.replace('_', ' ').title()

# Function to convert videoId to URL
def format_value(key, value):
    if key == 'videoId':
        return f"https://www.youtube.com/watch?v={value}"
    return value

@app.route('/generate-docs', methods=['POST'])
def generate_docs():
    try:
        # Step 1: Load JSON from form-data field
        json_str = request.form.get('json_data')
        if not json_str:
            return jsonify({'error': 'Missing json_data field in form-data'}), 400

        data = json.loads(json_str)
        if not isinstance(data, list) or len(data) == 0:
            return jsonify({'error': 'Invalid JSON payload. Must be a non-empty list.'}), 400

        # Step 2: Prepare zip buffer and name
        raw_channel_name = data[0].get('channelName', 'output_docs')
        zip_name = sanitize_filename(raw_channel_name) or 'output_docs'
        zip_buffer = io.BytesIO()

        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for item in data:
                title = item.get('title', 'Untitled')
                safe_title = sanitize_filename(title)

                # Create docx in memory
                doc = Document()
                doc.add_heading(title, level=1)

                for key, value in item.items():
                    doc.add_paragraph(f"{format_key(key)}: {format_value(key, value)}")

                # Save docx to bytes
                doc_bytes = io.BytesIO()
                doc.save(doc_bytes)
                doc_bytes.seek(0)

                zip_file.writestr(f"{safe_title}.docx", doc_bytes.read())

        zip_buffer.seek(0)

        # Step 3: Return zip file as download
        return send_file(
            zip_buffer,
            as_attachment=True,
            download_name=f"{zip_name}.zip",
            mimetype='application/zip'
        )

    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)
