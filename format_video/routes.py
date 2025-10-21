# format_video/routes.py
from flask import Blueprint, request, jsonify, send_from_directory, current_app
import os, ffmpeg, uuid, glob, threading

format_bp = Blueprint("format_bp", __name__)

# outputs will live inside this package folder
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "outputs")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# in-memory job store
JOBS = {}


def clear_old_outputs():
    """Delete all old formatted videos before each new request."""
    for f in glob.glob(os.path.join(OUTPUT_DIR, "*")):
        try:
            os.remove(f)
        except Exception:
            pass


def process_video(app, base_url, format_job_id, filepath, resize_data):
    """Background formatter. No url_for() here to avoid SERVER_NAME / context issues."""
    with app.app_context():
        try:
            output_links = []

            for platform, size in resize_data.items():
                try:
                    width, height = map(int, size.split(","))
                except ValueError:
                    JOBS[format_job_id]["status"] = "error"
                    JOBS[format_job_id]["error"] = f"Invalid size format for {platform}. Use 'width,height'"
                    # clean input
                    if os.path.exists(filepath): os.remove(filepath)
                    return

                # output file name exactly as the platform key
                output_filename = f"{platform}.mp4"
                output_path = os.path.join(OUTPUT_DIR, output_filename)

                # scale to fill & crop
                vf_filter = (
                    f"scale={width}:{height}:force_original_aspect_ratio=increase,"
                    f"crop={width}:{height}"
                )

                (
                    ffmpeg
                    .input(filepath)
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

                # ✅ Build URL WITHOUT url_for() to avoid SERVER_NAME / request context issues
                download_path = f"/download/{output_filename}"
                download_url = f"{base_url}{download_path}"
                output_links.append({platform: download_url})

            # delete temp input
            if os.path.exists(filepath):
                os.remove(filepath)

            JOBS[format_job_id]["status"] = "done"
            JOBS[format_job_id]["download_links"] = output_links

        except Exception as e:
            JOBS[format_job_id]["status"] = "error"
            JOBS[format_job_id]["error"] = str(e)
            if os.path.exists(filepath): os.remove(filepath)


@format_bp.route("/format", methods=["POST"])
def format_video():
    """Accepts: form-data -> video + any number of <platform>=width,height pairs."""
    if "video" not in request.files:
        return jsonify({"error": "No video file uploaded"}), 400

    resize_data = request.form.to_dict()
    if not resize_data:
        return jsonify({"error": "No resize data provided"}), 400

    # wipe old formatted files
    clear_old_outputs()

    # new job
    format_job_id = str(uuid.uuid4())
    JOBS[format_job_id] = {"status": "processing", "download_links": []}

    # save temp input
    file = request.files["video"]
    filename = f"{uuid.uuid4()}_{file.filename}"
    filepath = os.path.join(OUTPUT_DIR, filename)
    file.save(filepath)

    # auto-detect base URL (works for localhost, IP, domain, https)
    base_url = request.host_url.rstrip("/")

    # run in background
    app = current_app._get_current_object()
    t = threading.Thread(target=process_video, args=(app, base_url, format_job_id, filepath, resize_data), daemon=True)
    t.start()

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
