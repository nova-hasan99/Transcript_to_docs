from flask import Blueprint, request, jsonify, send_from_directory, current_app
import os, ffmpeg, uuid, glob, threading, requests, time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

format_bp = Blueprint("format_bp", __name__)

# job registry & executor
JOBS = {}
JOBS_LOCK = threading.Lock()
EXECUTOR = ThreadPoolExecutor(max_workers=3)
JOB_TTL_SECS = 6 * 60 * 60  # 6h TTL

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "outputs")
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ------------------------------ UTILITIES ------------------------------

def _gc_jobs():
    now = datetime.utcnow()
    with JOBS_LOCK:
        remove = []
        for jid, meta in JOBS.items():
            fin = meta.get("finished_at")
            if fin:
                dt = datetime.fromisoformat(fin)
                if (now - dt).total_seconds() > JOB_TTL_SECS:
                    remove.append(jid)
        for jid in remove:
            JOBS.pop(jid, None)


def _set_job(job_id: str, *, status: str, result=None, error=None):
    with JOBS_LOCK:
        if job_id in JOBS:
            JOBS[job_id]["status"] = status
            JOBS[job_id]["result"] = result
            JOBS[job_id]["error"] = error
            if status in ("done", "error"):
                JOBS[job_id]["finished_at"] = datetime.utcnow().isoformat()


def _get_job(job_id: str):
    with JOBS_LOCK:
        return JOBS.get(job_id)


def _new_job(payload: dict) -> str:
    job_id = uuid.uuid4().hex
    with JOBS_LOCK:
        JOBS[job_id] = {
            "status": "queued",
            "created_at": datetime.utcnow().isoformat(),
            "result": None,
            "error": None,
        }
    EXECUTOR.submit(_run_format_job, job_id, payload)
    return job_id


# ------------------------------ JOB LOGIC ------------------------------

def _download_video(video_url: str, save_path: str):
    r = requests.get(video_url, stream=True, timeout=60)
    if r.status_code != 200:
        raise Exception(f"Download failed (HTTP {r.status_code})")
    with open(save_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)


def _run_format_job(job_id: str, payload: dict):
    _set_job(job_id, status="running")

    try:
        video_url = payload.get("video_url")
        resize_data = payload.get("resize_data", {})

        if not video_url:
            _set_job(job_id, status="error", error="Missing video_url")
            return
        if not resize_data:
            _set_job(job_id, status="error", error="No resize data provided")
            return

        os.makedirs(OUTPUT_DIR, exist_ok=True)
        for f in glob.glob(os.path.join(OUTPUT_DIR, "*")):
            try:
                os.remove(f)
            except:
                pass

        input_path = os.path.join(OUTPUT_DIR, f"input_{uuid.uuid4().hex}.mp4")
        _download_video(video_url, input_path)

        output_links = []
        base_url = payload.get("base_url", "").rstrip("/")

        for platform, size in resize_data.items():
            width, height = map(int, size.split(","))
            output_name = f"{platform}.mp4"
            output_path = os.path.join(OUTPUT_DIR, output_name)

            vf = f"scale={width}:{height}:force_original_aspect_ratio=increase,crop={width}:{height}"
            (
                ffmpeg.input(input_path)
                .output(
                    output_path,
                    vf=vf,
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

            output_links.append({
                platform: f"{base_url}/download/{output_name}"
            })

        if os.path.exists(input_path):
            os.remove(input_path)

        _set_job(job_id, status="done", result={"download_links": output_links})

    except Exception as e:
        _set_job(job_id, status="error", error=str(e))


# ------------------------------ ROUTES ------------------------------

@format_bp.post("/format")
def format_submit():
    # support form-data or JSON
    data = request.form.to_dict() or (request.get_json(silent=True) or {})
    video_url = data.get("video_url")
    resize_data = {k: v for k, v in data.items() if k != "video_url"}

    payload = {
        "video_url": video_url,
        "resize_data": resize_data,
        "base_url": request.host_url.rstrip("/")
    }

    job_id = _new_job(payload)
    _gc_jobs()

    return jsonify({
        "status": "accepted",
        "format_job_id": job_id,
        "message": "Video formatting started. Check status using /format_status/<format_job_id>"
    }), 202


@format_bp.get("/format_status/<job_id>")
def format_status(job_id):
    job = _get_job(job_id)
    if not job:
        return jsonify({"error": "Invalid format_job_id"}), 404
    return jsonify(job), 200


@format_bp.route("/download/<filename>", methods=["GET"])
def download_file(filename):
    return send_from_directory(OUTPUT_DIR, filename, as_attachment=True)
