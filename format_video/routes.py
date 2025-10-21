# format_video/routes.py
from flask import Blueprint, request, jsonify, send_from_directory, current_app
import os, ffmpeg, uuid, glob, threading, requests

format_bp = Blueprint("format_bp", __name__)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "outputs")
os.makedirs(OUTPUT_DIR, exist_ok=True)
JOBS = {}


def clear_old_outputs():
    """Delete old formatted videos before new processing"""
    for f in glob.glob(os.path.join(OUTPUT_DIR, "*")):
        try:
            os.remove(f)
        except Exception:
            pass


def download_video_from_url(url, save_path):
    """Download video file from provided URL"""
    response = requests.get(url, stream=True, timeout=60)
    if response.status_code != 200:
        raise Exception(f"Failed to download video. HTTP {response.status_code}")
    with open(save_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)


def process_video(app, base_url, format_job_id, filepath, resize_data):
    """Background processing of video resize"""
    with app.app_context():
        try:
            output_links = []
            for platform, size in resize_data.items():
                width, height = map(int, size.split(","))
                output_filename = f"{platform}.mp4"
                output_path = os.path.join(OUTPUT_DIR, output_filename)

                vf_filter = (
                    f"scale={width}:{height}:force_original_aspect_ratio=increase,"
                    f"crop={width}:{height}"
                )

                (
                    ffmpeg.input(filepath)
                    .output(
                        output_path,
                        vf=vf_filter,
                        vcodec="libx264",
                        acodec="aac",
                        preset="ultrafast",
                        crf=26,
                        threads=2,
                        movflags="+faststart",
                    )
                    .overwrite_output()
                    .run(quiet=True)
                )

                # Build dynamic download link
                download_url = f"{base_url}/download/{output_filename}"
                output_links.append({platform: download_url})

            if os.path.exists(filepath):
                os.remove(filepath)

            JOBS[format_job_id]["status"] = "done"
            JOBS[format_job_id]["download_links"] = output_links

        except Exception as e:
            JOBS[format_job_id]["status"] = "error"
            JOBS[format_job_id]["error"] = str(e)
            if os.path.exists(filepath):
                os.remove(filepath)


@format_bp.route("/format", methods=["POST"])
def format_video():
    """Accepts: form-data => video_url + any number of <platform>=width,height"""
    video_url = request.form.get("video_url")
    resize_data = request.form.to_dict()

    # Remove video_url key from resize_data so only platform info stays
    resize_data.pop("video_url", None)

    if not video_url:
        return jsonify({"error": "Missing video_url"}), 400
    if not resize_data:
        return jsonify({"error": "No resize data provided"}), 400

    clear_old_outputs()
    format_job_id = str(uuid.uuid4())
    JOBS[format_job_id] = {"status": "processing", "download_links": []}

    filename = f"{uuid.uuid4()}.mp4"
    filepath = os.path.join(OUTPUT_DIR, filename)

    try:
        # Download video first
        download_video_from_url(video_url, filepath)
    except Exception as e:
        JOBS[format_job_id]["status"] = "error"
        JOBS[format_job_id]["error"] = str(e)
        return jsonify({"error": f"Failed to download: {str(e)}"}), 400

    base_url = request.host_url.rstrip("/")
    app = current_app._get_current_object()

    thread = threading.Thread(
        target=process_video, args=(app, base_url, format_job_id, filepath, resize_data), daemon=True
    )
    thread.start()

    return jsonify({
        "status": "processing",
        "format_job_id": format_job_id,
        "message": "Video formatting started. Check status using /format_status/<format_job_id>"
    })


@format_bp.route("/format_status/<format_job_id>", methods=["GET"])
def format_status(format_job_id):
    job = JOBS.get(format_job_id)
    if not job:
        return jsonify({"error": "Invalid format_job_id"}), 404
    return jsonify(job)


@format_bp.route("/download/<filename>", methods=["GET"])
def download_file(filename):
    return send_from_directory(OUTPUT_DIR, filename, as_attachment=True)
