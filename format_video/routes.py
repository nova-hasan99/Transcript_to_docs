from flask import Blueprint, request, jsonify, send_from_directory
import os, uuid, glob, threading, requests, json
from datetime import datetime
import ffmpeg

# ---------- SQLAlchemy (SQLite) ----------
from sqlalchemy import create_engine, Column, String, DateTime, Text
from sqlalchemy.orm import sessionmaker, declarative_base

format_bp = Blueprint("format_bp", __name__)

# ---------- Paths ----------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH   = os.path.join(BASE_DIR, "format_jobs.db")
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------- DB setup ----------
engine = create_engine(f"sqlite:///{DB_PATH}", connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class FormatJob(Base):
    __tablename__ = "format_jobs"
    id          = Column(String, primary_key=True)
    status      = Column(String, default="queued")   # queued | processing | done | error
    created_at  = Column(DateTime, default=datetime.utcnow)
    finished_at = Column(DateTime, nullable=True)
    # result = JSON string: [{"platform": "link"}, ...]
    result      = Column(Text, nullable=True)
    error       = Column(Text, nullable=True)
    base_url    = Column(Text, nullable=True)  # host base URL captured at submit time

Base.metadata.create_all(bind=engine)

# ---------- Helpers ----------
def _clear_outputs_folder():
    """নতুন রিকোয়েস্টে আগের সব আউটপুট ডিলিট করবে"""
    for f in glob.glob(os.path.join(OUTPUT_DIR, "*")):
        try:
            os.remove(f)
        except Exception:
            pass

def _keep_last_n_jobs(n=10):
    """ডাটাবেজে শেষ Nটা জব রাখবে; পুরনো গুলো ডিলিট করবে"""
    db = SessionLocal()
    try:
        jobs = db.query(FormatJob).order_by(FormatJob.created_at.desc()).all()
        if len(jobs) > n:
            for job in jobs[n:]:
                db.delete(job)
            db.commit()
    finally:
        db.close()

def _download_to(path: str, url: str, timeout=120):
    r = requests.get(url, stream=True, timeout=timeout)
    r.raise_for_status()
    with open(path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

# ---------- Background worker ----------
def _run_format_job(job_id: str, video_url: str, resize_data: dict):
    db = SessionLocal()
    try:
        job = db.query(FormatJob).filter(FormatJob.id == job_id).first()
        if not job:
            return
        job.status = "processing"
        db.commit()

        # 1) নতুন জব এলে পুরনো আউটপুট ডিলিট
        _clear_outputs_folder()

        # 2) ইনপুট ভিডিও ডাউনলোড (টেম্প ফাইল)
        input_name = f"input_{uuid.uuid4().hex}.mp4"
        input_path = os.path.join(OUTPUT_DIR, input_name)
        _download_to(input_path, video_url)

        # 3) প্রতিটি প্ল্যাটফর্ম আউটপুট বানাও
        download_links = []
        for platform, size in resize_data.items():
            # handle "1920,1080" বা "1920, 1080"
            parts = [p.strip() for p in str(size).split(",")]
            if len(parts) != 2:
                continue
            width, height = int(parts[0]), int(parts[1])

            out_name = f"{platform}.mp4"   # প্ল্যাটফর্ম নামেই ফাইল
            out_path = os.path.join(OUTPUT_DIR, out_name)

            vf = f"scale={width}:{height}:force_original_aspect_ratio=increase,crop={width}:{height}"
            (
                ffmpeg
                .input(input_path)
                .output(
                    out_path,
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

            # 4) বেস URL DB থেকে নিয়ে ফুল লিংক বানাও
            base_url = (job.base_url or "").rstrip("/")
            full_link = f"{base_url}/download/{out_name}"
            download_links.append({platform: full_link})

        # 5) কাজ শেষে ইনপুট ভিডিও ডিলিট
        try:
            if os.path.exists(input_path):
                os.remove(input_path)
        except Exception:
            pass

        # 6) জব আপডেট
        job.status = "done"
        job.result = json.dumps(download_links)  # save as JSON string
        job.finished_at = datetime.utcnow()
        db.commit()

        # 7) পুরনো জব পরিষ্কার (শুধু শেষ 10টা রাখো)
        _keep_last_n_jobs(10)

    except Exception as e:
        try:
            job = db.query(FormatJob).filter(FormatJob.id == job_id).first()
            if job:
                job.status = "error"
                job.error = str(e)
                job.finished_at = datetime.utcnow()
                db.commit()
        finally:
            pass
    finally:
        db.close()

# ---------- Routes ----------
@format_bp.route("/format", methods=["POST"])
def format_submit():
    # JSON + form-data — দুইটাই সাপোর্ট
    data = request.get_json(silent=True) or request.form.to_dict() or {}

    video_url = data.get("video_url")
    if not video_url:
        return jsonify({"error": "Missing video_url"}), 400

    # বাকি সব key গুলো প্ল্যাটফর্ম-সাইজ ধরে নেওয়া হবে
    resize_data = {k: v for k, v in data.items() if k != "video_url"}
    if not resize_data:
        return jsonify({"error": "No resize data provided"}), 400

    # নতুন জব তৈরি
    job_id = uuid.uuid4().hex
    base_url = request.host_url.rstrip("/")

    db = SessionLocal()
    try:
        job = FormatJob(
            id=job_id,
            status="queued",
            created_at=datetime.utcnow(),
            base_url=base_url,
            result=json.dumps(resize_data)  # worker-এ আবার parse করবো
        )
        db.add(job)
        db.commit()
    finally:
        db.close()

    # ব্যাকগ্রাউন্ড থ্রেড শুরু
    t = threading.Thread(
        target=_run_format_job,
        args=(job_id, video_url, resize_data),
        name=f"format-job-{job_id}",
        daemon=True
    )
    t.start()

    return jsonify({
        "status": "accepted",
        "format_job_id": job_id,
        "message": "Video formatting started. Check status using /format_status/<format_job_id>"
    }), 202


@format_bp.route("/format_status/<job_id>", methods=["GET"])
def format_status(job_id):
    db = SessionLocal()
    try:
        job = db.query(FormatJob).filter(FormatJob.id == job_id).first()
        if not job:
            return jsonify({"error": "Invalid format_job_id"}), 404

        if job.status == "done":
            # মিনিমাল ক্লিন রেসপন্স: শুধু download_links + status
            links = []
            try:
                links = json.loads(job.result) if job.result else []
            except Exception:
                links = []
            return jsonify({
                "download_links": links,
                "status": "ok"
            }), 200

        if job.status == "error":
            return jsonify({"status": "error", "error": job.error}), 500

        # queued / processing
        return jsonify({"status": job.status}), 200
    finally:
        db.close()


@format_bp.route("/download/<filename>", methods=["GET"])
def download_file(filename):
    # ফাইল ডাউনলোড সার্ভ করা
    return send_from_directory(OUTPUT_DIR, filename, as_attachment=True)
