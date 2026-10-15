import json
import hashlib
import hmac
import math
import os
import re
import secrets
import shutil
import signal
import sqlite3
import subprocess
import threading
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import psutil
from fastapi import BackgroundTasks, Cookie, Depends, FastAPI, HTTPException, Request, Response, status
from fastapi.responses import FileResponse, JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles


APP_ROOT = Path("/opt/splunk-cloud-export")
DATA_DIR = APP_ROOT / "data"
CONFIG_DIR = APP_ROOT / "configs"
RUN_DIR = APP_ROOT / "runs"
SPLUNK_EXPORT_DIR = APP_ROOT / "splunk-export"
DB_PATH = DATA_DIR / "console.sqlite3"
DEFAULT_ARCHIVE_PATH = Path(os.getenv("ARCHIVE_PATH", "/data/archive"))

APP_USER = os.getenv("APP_USER", "admin")
APP_PASSWORD = os.getenv("APP_PASSWORD", "admin")

security = HTTPBasic(auto_error=False)
app = FastAPI(title="Splunk Cloud Export Console")
app.mount("/static", StaticFiles(directory="static"), name="static")

runtime: dict[int, dict[str, Any]] = {}
runtime_lock = threading.Lock()
INDEX_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
PARTITION_PROGRESS_RE = re.compile(r"Completed Partition (\d+) of (\d+)")
TOTAL_RESULTS_RE = re.compile(r"Total results:\s*(\d+)", re.IGNORECASE)
metric_samples: dict[int, list[tuple[float, int, int]]] = {}
metric_samples_lock = threading.Lock()
network_samples: dict[str, tuple[float, int, int]] = {}
network_samples_lock = threading.Lock()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def session_token() -> str:
    return hmac.new(
        APP_PASSWORD.encode("utf-8"),
        APP_USER.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def credentials_are_valid(username: str, password: str) -> bool:
    return secrets.compare_digest(username, APP_USER) and secrets.compare_digest(password, APP_PASSWORD)


def require_auth(
    credentials: Optional[HTTPBasicCredentials] = Depends(security),
    sce_session: Optional[str] = Cookie(default=None),
) -> str:
    if sce_session and secrets.compare_digest(sce_session, session_token()):
        return APP_USER
    if credentials and credentials_are_valid(credentials.username, credentials.password):
        return credentials.username
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Not authenticated",
        headers={"WWW-Authenticate": "Basic"},
    )


def db() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute(
        """
        create table if not exists jobs (
            id integer primary key autoincrement,
            name text not null,
            splunk_host text not null,
            splunk_port integer not null default 8089,
            auth_token text not null,
            indexes text not null,
            sourcetypes text default '',
            begin_date text not null,
            end_date text default '',
            extra text default '| table _time _raw index source sourcetype',
            partition_units text not null default 'hours',
            partition_interval integer not null default 1,
            parallel_processes integer not null default 4,
            output_format text not null default 'json',
            gzip integer not null default 1,
            resume_mode text not null default 'resume',
            incremental_mode text not null default 'false',
            max_file_size_mb integer not null default 1000,
            archive_path text not null default '/data/archive',
            status text not null default 'idle',
            created_at text not null,
            updated_at text not null,
            last_started_at text,
            last_finished_at text,
            last_exit_code integer
        )
        """
    )
    con.execute(
        """
        create table if not exists runs (
            id integer primary key autoincrement,
            job_id integer not null,
            pid integer,
            status text not null,
            started_at text not null,
            finished_at text,
            exit_code integer,
            bytes_start integer not null default 0,
            bytes_end integer,
            log_path text not null,
            config_path text not null,
            foreign key(job_id) references jobs(id)
        )
        """
    )
    con.commit()
    return con


def recover_orphaned_runs() -> None:
    """Never keep a job marked running when its runner died with the web service."""
    with db() as con:
        runs = con.execute(
            "select id, job_id, pid from runs where status='running'"
        ).fetchall()
        for run in runs:
            if run["pid"] and psutil.pid_exists(run["pid"]):
                continue
            con.execute(
                "update runs set status=?, finished_at=?, exit_code=? where id=?",
                ("interrupted", now_iso(), -15, run["id"]),
            )
            con.execute(
                """
                update jobs set status=?, updated_at=?, last_finished_at=?, last_exit_code=?
                where id=? and (select max(id) from runs where job_id=?)=?
                """,
                ("interrupted", now_iso(), now_iso(), -15, run["job_id"], run["job_id"], run["id"]),
            )
        con.commit()


@app.on_event("startup")
def recover_on_startup() -> None:
    recover_orphaned_runs()


def row_dict(row: Optional[sqlite3.Row]) -> Optional[dict[str, Any]]:
    if row is None:
        return None
    item = dict(row)
    if "auth_token" in item:
        item["auth_token"] = "********"
    return item


def folder_metrics(path: Path) -> dict[str, Any]:
    total = 0
    files = 0
    if path.exists():
        for root, _, names in os.walk(path):
            for name in names:
                try:
                    total += (Path(root) / name).stat().st_size
                    files += 1
                except OSError:
                    pass
    usage_target = path if path.exists() else path.parent
    try:
        disk = shutil.disk_usage(usage_target)
        free = disk.free
        capacity = disk.total
    except OSError:
        free = None
        capacity = None
    return {"bytes": total, "files": files, "disk_free": free, "disk_total": capacity}


def storage_volumes() -> list[dict[str, Any]]:
    """Return usable local filesystems for the authenticated storage picker."""
    ignored_types = {"proc", "sysfs", "tmpfs", "devtmpfs", "cgroup", "cgroup2", "squashfs"}
    seen: set[str] = set()
    volumes: list[dict[str, Any]] = []
    for partition in psutil.disk_partitions(all=False):
        mountpoint = Path(partition.mountpoint).resolve()
        key = str(mountpoint)
        if key in seen or partition.fstype.lower() in ignored_types:
            continue
        seen.add(key)
        try:
            usage = shutil.disk_usage(mountpoint)
        except OSError:
            continue
        volumes.append({"path": key, "device": partition.device or key, "filesystem": partition.fstype or "local", "free": usage.free, "total": usage.total})
    archive_parent = DEFAULT_ARCHIVE_PATH.parent.resolve()
    if str(archive_parent) not in seen:
        usage = shutil.disk_usage(archive_parent)
        volumes.append({"path": str(archive_parent), "device": "Archive", "filesystem": "local", "free": usage.free, "total": usage.total})
    return sorted(volumes, key=lambda item: item["path"])


def storage_path(path_value: str) -> Path:
    try:
        path = Path(path_value).expanduser().resolve()
    except OSError as exc:
        raise HTTPException(400, "Chemin de stockage invalide") from exc
    allowed = [Path(volume["path"]) for volume in storage_volumes()]
    if not any(path == root or root in path.parents for root in allowed):
        raise HTTPException(403, "Chemin hors des volumes de stockage autorisés")
    return path


def partition_progress(log_text: str) -> tuple[int, int]:
    """Return the most recent completed/total partition pair written by the runner."""
    matches = PARTITION_PROGRESS_RE.findall(log_text)
    if not matches:
        return 0, 0
    current, total = matches[-1]
    return int(current), int(total)


def rolling_progress_rate(run_id: int, completed: int, output_bytes: int) -> tuple[float, float]:
    """Calculate 60-second rolling rates from actual runner progress and output."""
    now = time.monotonic()
    with metric_samples_lock:
        samples = metric_samples.setdefault(run_id, [])
        samples.append((now, completed, output_bytes))
        samples[:] = [sample for sample in samples if sample[0] >= now - 60]
        oldest = samples[0]
    seconds = now - oldest[0]
    if seconds < 1:
        return 0.0, 0.0
    return (
        max(completed - oldest[1], 0) / seconds,
        max(output_bytes - oldest[2], 0) / seconds,
    )


def active_network_interface() -> Optional[str]:
    """Return the interface that carries the default route, with a safe fallback."""
    try:
        for line in Path("/proc/net/route").read_text(encoding="utf-8").splitlines()[1:]:
            fields = line.split()
            if len(fields) >= 4 and fields[1] == "00000000" and int(fields[3], 16) & 2:
                return fields[0]
    except (OSError, ValueError):
        pass
    stats = psutil.net_if_stats()
    return next((name for name, state in stats.items() if name != "lo" and state.isup), None)


def network_rate() -> dict[str, Any]:
    interface = active_network_interface()
    counters = psutil.net_io_counters(pernic=True)
    if not interface or interface not in counters:
        return {"interface": interface, "rx_bytes_per_second": 0.0, "tx_bytes_per_second": 0.0}
    current = counters[interface]
    now = time.monotonic()
    with network_samples_lock:
        previous = network_samples.get(interface)
        network_samples[interface] = (now, current.bytes_recv, current.bytes_sent)
    if not previous:
        return {"interface": interface, "rx_bytes_per_second": 0.0, "tx_bytes_per_second": 0.0}
    seconds = now - previous[0]
    if seconds < 0.1:
        return {"interface": interface, "rx_bytes_per_second": 0.0, "tx_bytes_per_second": 0.0}
    return {
        "interface": interface,
        "rx_bytes_per_second": max(current.bytes_recv - previous[1], 0) / seconds,
        "tx_bytes_per_second": max(current.bytes_sent - previous[2], 0) / seconds,
    }


def job_output_path(job: sqlite3.Row) -> Path:
    return Path(job["archive_path"]).expanduser().resolve() / safe_name(job["name"])


def safe_name(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in "-_." else "-" for ch in value.strip())
    return cleaned.strip("-") or "splunk-export-job"


def parse_dt(value: Optional[str]) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    cleaned = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(cleaned)
    except ValueError as exc:
        raise HTTPException(400, f"Invalid date: {value}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_indexes(value: str) -> list[str]:
    indexes = [item.strip() for item in value.split(",") if item.strip()]
    if not indexes:
        raise HTTPException(400, "At least one index is required")
    invalid = [item for item in indexes if not INDEX_RE.match(item)]
    if invalid:
        raise HTTPException(400, f"Invalid index name: {', '.join(invalid)}")
    return indexes


def splunk_time(value: Optional[str]) -> str:
    return parse_dt(value).strftime("%Y-%m-%dT%H:%M:%S")


def recommend_partition(total_events: int, begin_date: str, end_date: Optional[str]) -> dict[str, Any]:
    begin = parse_dt(begin_date)
    end = parse_dt(end_date)
    hours = max((end - begin).total_seconds() / 3600, 1)
    days = max(hours / 24, 1)

    if total_events <= 0:
        target_partitions = min(max(math.ceil(days / 7), 1), 60)
    elif total_events < 50_000:
        target_partitions = min(max(math.ceil(days / 7), 1), 90)
    elif total_events < 2_000_000:
        target_partitions = min(max(math.ceil(total_events / 100_000), 10), 180)
    else:
        target_partitions = min(max(math.ceil(total_events / 250_000), 30), 300)

    if days <= 2 and total_events > 250_000:
        units = "hours"
        interval = max(1, math.ceil(hours / target_partitions))
        estimated_partitions = math.ceil(hours / interval)
    else:
        units = "days"
        interval = max(1, math.ceil(days / target_partitions))
        estimated_partitions = math.ceil(days / interval)

    if estimated_partitions <= 1:
        workers = 1
    elif total_events < 500_000:
        workers = 2
    else:
        workers = 3

    reason = (
        "Partitionnement large pour eviter des milliers de recherches vides."
        if total_events < 50_000
        else "Partitionnement calcule pour garder des tranches raisonnables sans heurter la limite Splunk Cloud."
    )
    return {
        "partition_units": units,
        "partition_interval": interval,
        "parallel_processes": workers,
        "estimated_partitions": estimated_partitions,
        "range_hours": round(hours, 2),
        "reason": reason,
    }


def write_config(job: sqlite3.Row, config_path: Path) -> None:
    config = f"""[splunk_source]
SPLUNK_HOST={job['splunk_host']}
SPLUNK_PORT={job['splunk_port']}
SPLUNK_AUTH_TOKEN=%SPLUNK_AUTH_TOKEN%

[search]
indexes={job['indexes']}
sourcetypes={job['sourcetypes'] or ''}
begin_date={job['begin_date']}
end_date={job['end_date'] or ''}
extra={job['extra'] or ''}

[export]
log_level=INFO
parition_units={job['partition_units']}
partition_interval={job['partition_interval']}
directory={job['archive_path']}
job_location={job['archive_path']}
job_name={safe_name(job['name'])}
parallel_processes={job['parallel_processes']}
output_destination=file
gzip={'true' if job['gzip'] else 'false'}
output_format={job['output_format']}
resume_mode={job['resume_mode']}
incremental_mode={job['incremental_mode']}
max_file_size_mb={job['max_file_size_mb']}
log_format=json
"""
    config_path.write_text(config, encoding="utf-8")
    os.chmod(config_path, 0o600)


def run_monitor(job_id: int, run_id: int, process: subprocess.Popen, log_handle) -> None:
    exit_code = process.wait()
    log_handle.close()
    with db() as con:
        job = con.execute("select * from jobs where id=?", (job_id,)).fetchone()
        end_bytes = folder_metrics(job_output_path(job))["bytes"] if job else None
        status_value = "completed" if exit_code == 0 else "failed"
        con.execute(
            "update runs set status=?, finished_at=?, exit_code=?, bytes_end=? where id=?",
            (status_value, now_iso(), exit_code, end_bytes, run_id),
        )
        con.execute(
            "update jobs set status=?, updated_at=?, last_finished_at=?, last_exit_code=? where id=?",
            (status_value, now_iso(), now_iso(), exit_code, job_id),
        )
        con.commit()
    with runtime_lock:
        runtime.pop(job_id, None)


def latest_run(con: sqlite3.Connection, job_id: int) -> Optional[sqlite3.Row]:
    return con.execute(
        "select * from runs where job_id=? order by id desc limit 1", (job_id,)
    ).fetchone()


def read_tail(path: str, limit: int = 16000) -> str:
    try:
        with open(path, "rb") as fh:
            fh.seek(0, os.SEEK_END)
            size = fh.tell()
            fh.seek(max(size - limit, 0))
            return fh.read().decode("utf-8", errors="replace")
    except OSError:
        return ""


def job_files(job: sqlite3.Row) -> list[dict[str, Any]]:
    root = job_output_path(job)
    if not root.exists():
        return []
    files: list[dict[str, Any]] = []
    for item in root.rglob("*"):
        try:
            if item.is_file() and not item.is_symlink():
                stat = item.stat()
                files.append({
                    "path": str(item.relative_to(root)),
                    "size": stat.st_size,
                    "modified_at": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
                })
        except OSError:
            continue
    return sorted(files, key=lambda item: item["path"])


def selected_job_file(job: sqlite3.Row, relative_path: str) -> Path:
    root = job_output_path(job).resolve()
    candidate = (root / relative_path).resolve()
    if root not in candidate.parents or not candidate.is_file() or candidate.is_symlink():
        raise HTTPException(404, "Fichier d’export introuvable")
    return candidate


def total_results(log_path: str) -> Optional[int]:
    values = TOTAL_RESULTS_RE.findall(read_tail(log_path, 128_000))
    return int(values[-1]) if values else None


@app.on_event("startup")
def startup() -> None:
    for path in (DATA_DIR, CONFIG_DIR, RUN_DIR, DEFAULT_ARCHIVE_PATH):
        path.mkdir(parents=True, exist_ok=True)
    db().close()


@app.get("/")
def index() -> FileResponse:
    return FileResponse("static/index.html")


@app.post("/api/login")
async def login(request: Request, response: Response) -> dict[str, Any]:
    payload = await request.json()
    if not credentials_are_valid(str(payload.get("username", "")), str(payload.get("password", ""))):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid username or password")
    response.set_cookie(
        "sce_session",
        session_token(),
        httponly=True,
        samesite="lax",
        max_age=60 * 60 * 12,
    )
    return {"ok": True, "user": APP_USER}


@app.post("/api/logout", dependencies=[Depends(require_auth)])
def logout(response: Response) -> dict[str, Any]:
    response.delete_cookie("sce_session")
    return {"ok": True}


@app.get("/api/me", dependencies=[Depends(require_auth)])
def me() -> dict[str, Any]:
    return {"user": APP_USER}


@app.get("/api/health")
def health() -> dict[str, Any]:
    return {"ok": True, "time": now_iso()}


@app.get("/api/jobs", dependencies=[Depends(require_auth)])
def list_jobs() -> list[dict[str, Any]]:
    with db() as con:
        rows = con.execute("select * from jobs order by updated_at desc").fetchall()
        return [row_dict(row) for row in rows]


@app.post("/api/analyze", dependencies=[Depends(require_auth)])
async def analyze_source(request: Request) -> dict[str, Any]:
    payload = await request.json()
    required = ["splunk_host", "auth_token", "indexes", "begin_date"]
    missing = [key for key in required if not str(payload.get(key, "")).strip()]
    if missing:
        raise HTTPException(400, f"Missing required fields: {', '.join(missing)}")

    token = str(payload["auth_token"])
    if token == "********" and payload.get("id"):
        with db() as con:
            old = con.execute("select auth_token from jobs where id=?", (payload["id"],)).fetchone()
            if old:
                token = old["auth_token"]
    indexes = parse_indexes(str(payload["indexes"]))
    where_clause = " OR ".join(f"index={index}" for index in indexes)
    begin = str(payload["begin_date"]).strip()
    end = str(payload.get("end_date", "")).strip()
    splunk_host = str(payload["splunk_host"]).strip().replace("https://", "").replace("http://", "").strip("/")
    splunk_port = int(payload.get("splunk_port") or 8089)

    try:
        import splunklib.client as client
        import splunklib.results as results

        service = client.connect(
            host=splunk_host,
            port=splunk_port,
            scheme="https",
            token=token,
        )
        query = f"| tstats count where {where_clause} by _time span=1d"
        stream = service.jobs.export(
            query,
            earliest_time=splunk_time(begin),
            latest_time=splunk_time(end) if end else "now",
            output_mode="json",
        )
        buckets: list[dict[str, Any]] = []
        total = 0
        for item in results.JSONResultsReader(stream):
            if not isinstance(item, dict):
                continue
            count = int(float(item.get("count") or 0))
            total += count
            buckets.append({"time": item.get("_time"), "count": count})
    except Exception as exc:
        raise HTTPException(502, f"Splunk analysis failed: {exc}") from exc

    recommendation = recommend_partition(total, begin, end)
    non_empty_days = sum(1 for item in buckets if item["count"] > 0)
    max_day = max((item["count"] for item in buckets), default=0)
    return {
        "indexes": indexes,
        "begin_date": begin,
        "end_date": end or "now",
        "event_count": total,
        "bucket_count": len(buckets),
        "non_empty_days": non_empty_days,
        "max_day_count": max_day,
        "recommendation": recommendation,
        "sample": buckets[:14],
    }


@app.post("/api/jobs", dependencies=[Depends(require_auth)])
async def save_job(request: Request) -> dict[str, Any]:
    payload = await request.json()
    required = ["name", "splunk_host", "auth_token", "indexes", "begin_date", "archive_path"]
    missing = [key for key in required if not str(payload.get(key, "")).strip()]
    if missing:
        raise HTTPException(400, f"Missing required fields: {', '.join(missing)}")
    job_id = payload.get("id")
    values = {
        "name": payload["name"].strip(),
        "splunk_host": payload["splunk_host"].strip(),
        "splunk_port": int(payload.get("splunk_port") or 8089),
        "auth_token": payload["auth_token"],
        "indexes": payload["indexes"].strip(),
        "sourcetypes": payload.get("sourcetypes", "").strip(),
        "begin_date": payload["begin_date"].strip(),
        "end_date": payload.get("end_date", "").strip(),
        "extra": payload.get("extra", "| table _time _raw index source sourcetype").strip(),
        "partition_units": payload.get("partition_units", "hours"),
        "partition_interval": int(payload.get("partition_interval") or 1),
        "parallel_processes": int(payload.get("parallel_processes") or 4),
        "output_format": payload.get("output_format", "json"),
        "gzip": 1 if payload.get("gzip", True) else 0,
        "resume_mode": payload.get("resume_mode", "resume"),
        "incremental_mode": payload.get("incremental_mode", "false"),
        "max_file_size_mb": int(payload.get("max_file_size_mb") or 1000),
        "archive_path": payload["archive_path"].strip(),
        "updated_at": now_iso(),
    }
    Path(values["archive_path"]).mkdir(parents=True, exist_ok=True)
    with db() as con:
        if job_id:
            old = con.execute("select * from jobs where id=?", (job_id,)).fetchone()
            if not old:
                raise HTTPException(404, "Job not found")
            if payload["auth_token"] == "********":
                values["auth_token"] = old["auth_token"]
            assignments = ", ".join(f"{key}=?" for key in values)
            con.execute(
                f"update jobs set {assignments} where id=?",
                (*values.values(), job_id),
            )
        else:
            values["created_at"] = now_iso()
            values["status"] = "idle"
            columns = ", ".join(values)
            placeholders = ", ".join("?" for _ in values)
            cur = con.execute(
                f"insert into jobs ({columns}) values ({placeholders})", tuple(values.values())
            )
            job_id = cur.lastrowid
        con.commit()
        row = con.execute("select * from jobs where id=?", (job_id,)).fetchone()
        return row_dict(row)


@app.post("/api/jobs/{job_id}/start", dependencies=[Depends(require_auth)])
def start_job(job_id: int) -> dict[str, Any]:
    with runtime_lock:
        if job_id in runtime:
            raise HTTPException(409, "Job already running")
    with db() as con:
        job = con.execute("select * from jobs where id=?", (job_id,)).fetchone()
        if not job:
            raise HTTPException(404, "Job not found")
        output_path = job_output_path(job)
        output_path.mkdir(parents=True, exist_ok=True)
        run_stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        config_path = CONFIG_DIR / f"job-{job_id}-{run_stamp}.conf"
        log_path = RUN_DIR / f"job-{job_id}-{run_stamp}.log"
        write_config(job, config_path)
        bytes_start = folder_metrics(output_path)["bytes"]
        cur = con.execute(
            "insert into runs (job_id,status,started_at,bytes_start,log_path,config_path) values (?,?,?,?,?,?)",
            (job_id, "running", now_iso(), bytes_start, str(log_path), str(config_path)),
        )
        run_id = cur.lastrowid
        con.execute(
            "update jobs set status=?, updated_at=?, last_started_at=?, last_exit_code=null where id=?",
            ("running", now_iso(), now_iso(), job_id),
        )
        con.commit()
    env = os.environ.copy()
    env["SPLUNK_AUTH_TOKEN"] = job["auth_token"]
    command = [
        "/opt/splunk-cloud-export/venv/bin/python",
        str(SPLUNK_EXPORT_DIR / "splunk_export_parallel.py"),
        "--my-config",
        str(config_path),
    ]
    log_handle = open(log_path, "ab", buffering=0)
    process = subprocess.Popen(
        command,
        cwd=str(SPLUNK_EXPORT_DIR),
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        env=env,
        preexec_fn=os.setsid,
    )
    with db() as con:
        con.execute("update runs set pid=? where id=?", (process.pid, run_id))
        con.commit()
    with runtime_lock:
        runtime[job_id] = {"process": process, "run_id": run_id, "started": time.time()}
    thread = threading.Thread(target=run_monitor, args=(job_id, run_id, process, log_handle), daemon=True)
    thread.start()
    return {"job_id": job_id, "run_id": run_id, "pid": process.pid, "status": "running"}


@app.post("/api/jobs/{job_id}/stop", dependencies=[Depends(require_auth)])
def stop_job(job_id: int) -> dict[str, Any]:
    with runtime_lock:
        entry = runtime.get(job_id)
    if not entry:
        raise HTTPException(409, "Job is not running in this console process")
    process = entry["process"]
    try:
        os.killpg(os.getpgid(process.pid), signal.SIGTERM)
    except ProcessLookupError:
        pass
    with db() as con:
        con.execute("update jobs set status=?, updated_at=? where id=?", ("interrupted", now_iso(), job_id))
        con.execute("update runs set status=?, finished_at=? where id=?", ("interrupted", now_iso(), entry["run_id"]))
        con.commit()
    return {"job_id": job_id, "status": "interrupted"}


def remove_run_artifact(raw_path: str, base_dir: Path) -> bool:
    """Delete only a regular run artifact located under the expected app directory."""
    try:
        path = Path(raw_path).resolve()
        root = base_dir.resolve()
    except OSError:
        return False
    if root not in path.parents or not path.is_file():
        return False
    try:
        path.unlink()
        return True
    except OSError:
        return False


@app.delete("/api/jobs/{job_id}/history", dependencies=[Depends(require_auth)])
def clear_job_history(job_id: int) -> dict[str, Any]:
    """Clear run metadata and runner artifacts while deliberately preserving exports."""
    with runtime_lock:
        if job_id in runtime:
            raise HTTPException(409, "Arrête le job en cours avant d'effacer son historique")

    with db() as con:
        job = con.execute("select id from jobs where id=?", (job_id,)).fetchone()
        if not job:
            raise HTTPException(404, "Job not found")
        active_run = con.execute(
            "select pid from runs where job_id=? and status='running' order by id desc limit 1",
            (job_id,),
        ).fetchone()
        if active_run and active_run["pid"] and psutil.pid_exists(active_run["pid"]):
            raise HTTPException(409, "Arrête le job en cours avant d'effacer son historique")
        runs = con.execute(
            "select log_path, config_path from runs where job_id=?", (job_id,)
        ).fetchall()
        con.execute("delete from runs where job_id=?", (job_id,))
        con.execute(
            """
            update jobs
            set status='idle', updated_at=?, last_started_at=null,
                last_finished_at=null, last_exit_code=null
            where id=?
            """,
            (now_iso(), job_id),
        )
        con.commit()

    deleted_artifacts = 0
    for run in runs:
        deleted_artifacts += int(remove_run_artifact(run["log_path"], RUN_DIR))
        deleted_artifacts += int(remove_run_artifact(run["config_path"], CONFIG_DIR))
    with metric_samples_lock:
        metric_samples.clear()
    return {
        "job_id": job_id,
        "cleared_runs": len(runs),
        "deleted_artifacts": deleted_artifacts,
        "exports_preserved": True,
    }


@app.get("/api/jobs/{job_id}/metrics", dependencies=[Depends(require_auth)])
def job_metrics(job_id: int) -> dict[str, Any]:
    with db() as con:
        job = con.execute("select * from jobs where id=?", (job_id,)).fetchone()
        if not job:
            raise HTTPException(404, "Job not found")
        run = latest_run(con, job_id)
    output_path = job_output_path(job)
    files = folder_metrics(output_path)
    run_info = row_dict(run) if run else None
    elapsed = 0.0
    throughput = 0.0
    pid = run["pid"] if run else None
    alive = False
    process_threads = 0
    active_workers = 0
    cpu_percent = 0.0
    memory_rss = 0
    log_tail = read_tail(run["log_path"]) if run else ""
    completed_partitions, total_partitions = partition_progress(log_tail)
    partition_rate = 0.0
    rolling_throughput = 0.0
    if run and run["started_at"]:
        started = datetime.fromisoformat(run["started_at"])
        end = datetime.fromisoformat(run["finished_at"]) if run["finished_at"] else datetime.now(timezone.utc)
        elapsed = max((end - started).total_seconds(), 0.0)
        delta = max(files["bytes"] - run["bytes_start"], 0)
        throughput = delta / elapsed if elapsed else 0.0
        partition_rate, rolling_throughput = rolling_progress_rate(
            run["id"], completed_partitions, files["bytes"]
        )
    if pid and psutil.pid_exists(pid):
        try:
            proc = psutil.Process(pid)
            alive = proc.is_running()
            process_threads = proc.num_threads()
            # splunk_export_parallel.py is the coordinator.  Its real export
            # workers are child processes, not threads of this coordinator.
            active_workers = sum(
                1
                for child in proc.children(recursive=True)
                if child.is_running() and child.status() != psutil.STATUS_ZOMBIE
            )
            cpu_percent = proc.cpu_percent(interval=0.0)
            memory_rss = proc.memory_info().rss
        except psutil.Error:
            pass
    return {
        "job": row_dict(job),
        "run": run_info,
        "output_path": str(output_path),
        "output": files,
        "elapsed_seconds": elapsed,
        "bytes_per_second": throughput,
        "rolling_bytes_per_second": rolling_throughput,
        "partitions_completed": completed_partitions,
        "partitions_total": total_partitions,
        "partitions_per_second": partition_rate,
        "alive": alive,
        "pid": pid,
        "configured_workers": job["parallel_processes"],
        "active_workers": active_workers,
        "process_threads": process_threads,
        "process_cpu_percent": cpu_percent,
        "process_memory_rss": memory_rss,
        "system": {
            "cpu_percent": psutil.cpu_percent(interval=0.0),
            "memory": psutil.virtual_memory()._asdict(),
            "loadavg": os.getloadavg(),
        },
        "network": network_rate(),
        "log_tail": log_tail,
    }


@app.get("/api/jobs/{job_id}/results", dependencies=[Depends(require_auth)])
def job_results(job_id: int) -> dict[str, Any]:
    with db() as con:
        job = con.execute("select * from jobs where id=?", (job_id,)).fetchone()
        if not job:
            raise HTTPException(404, "Job not found")
        run = latest_run(con, job_id)
    output = folder_metrics(job_output_path(job))
    elapsed = 0.0
    if run and run["started_at"]:
        start = datetime.fromisoformat(run["started_at"])
        end = datetime.fromisoformat(run["finished_at"]) if run["finished_at"] else datetime.now(timezone.utc)
        elapsed = max((end - start).total_seconds(), 0.0)
    exported_bytes = max(output["bytes"] - (run["bytes_start"] if run else 0), 0)
    return {
        "job": row_dict(job),
        "run": row_dict(run),
        "summary": {
            "elapsed_seconds": elapsed,
            "bytes": exported_bytes,
            "bytes_per_second": exported_bytes / elapsed if elapsed else 0.0,
            "event_count": total_results(run["log_path"]) if run else None,
            "file_count": len(job_files(job)),
        },
        "files": job_files(job),
    }


@app.get("/api/jobs/{job_id}/files/download", dependencies=[Depends(require_auth)])
def download_job_file(job_id: int, file: str) -> FileResponse:
    with db() as con:
        job = con.execute("select * from jobs where id=?", (job_id,)).fetchone()
    if not job:
        raise HTTPException(404, "Job not found")
    target = selected_job_file(job, file)
    return FileResponse(target, filename=target.name, media_type="application/octet-stream")


@app.post("/api/jobs/{job_id}/files/archive", dependencies=[Depends(require_auth)])
async def download_job_files(job_id: int, request: Request, background_tasks: BackgroundTasks) -> FileResponse:
    payload = await request.json()
    requested = payload.get("files", [])
    if not isinstance(requested, list) or not requested:
        raise HTTPException(400, "Sélectionne au moins un fichier")
    with db() as con:
        job = con.execute("select * from jobs where id=?", (job_id,)).fetchone()
    if not job:
        raise HTTPException(404, "Job not found")
    selected = [selected_job_file(job, str(item)) for item in requested]
    archive_path = RUN_DIR / f"download-{job_id}-{secrets.token_urlsafe(10)}.zip"
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for item in selected:
            archive.write(item, arcname=item.relative_to(job_output_path(job)))
    background_tasks.add_task(archive_path.unlink, missing_ok=True)
    return FileResponse(archive_path, filename=f"{safe_name(job['name'])}-export.zip", media_type="application/zip", background=background_tasks)


@app.delete("/api/jobs/{job_id}/files", dependencies=[Depends(require_auth)])
async def delete_job_files(job_id: int, request: Request) -> dict[str, Any]:
    payload = await request.json()
    requested = payload.get("files", [])
    if not isinstance(requested, list) or not requested:
        raise HTTPException(400, "Sélectionne au moins un fichier")
    with db() as con:
        job = con.execute("select * from jobs where id=?", (job_id,)).fetchone()
    if not job:
        raise HTTPException(404, "Job not found")
    selected = [selected_job_file(job, str(item)) for item in requested]
    deleted = 0
    freed = 0
    for item in selected:
        try:
            freed += item.stat().st_size
            item.unlink()
            deleted += 1
        except OSError as exc:
            raise HTTPException(500, f"Suppression impossible : {item.name}") from exc
    return {"deleted": deleted, "freed_bytes": freed}


@app.get("/api/overview", dependencies=[Depends(require_auth)])
def overview() -> dict[str, Any]:
    archive = folder_metrics(DEFAULT_ARCHIVE_PATH)
    with db() as con:
        jobs = con.execute("select status, count(*) as count from jobs group by status").fetchall()
    return {
        "archive_path": str(DEFAULT_ARCHIVE_PATH),
        "archive": archive,
        "jobs_by_status": {row["status"]: row["count"] for row in jobs},
        "system": {
            "cpu_percent": psutil.cpu_percent(interval=0.0),
            "memory": psutil.virtual_memory()._asdict(),
            "boot_time": psutil.boot_time(),
        },
    }


@app.get("/api/storage/volumes", dependencies=[Depends(require_auth)])
def list_storage_volumes() -> dict[str, Any]:
    return {"volumes": storage_volumes(), "default_path": str(DEFAULT_ARCHIVE_PATH)}


@app.get("/api/storage/browse", dependencies=[Depends(require_auth)])
def browse_storage(path: str = "") -> dict[str, Any]:
    target = storage_path(path or str(DEFAULT_ARCHIVE_PATH.parent))
    if not target.exists() or not target.is_dir():
        raise HTTPException(404, "Dossier de stockage introuvable")
    folders: list[dict[str, str]] = []
    try:
        for child in sorted(target.iterdir(), key=lambda item: item.name.lower()):
            if child.is_dir() and not child.is_symlink():
                folders.append({"name": child.name, "path": str(child.resolve())})
    except OSError as exc:
        raise HTTPException(403, "Dossier non accessible") from exc
    parent = target.parent
    allowed = [Path(volume["path"]) for volume in storage_volumes()]
    parent_value = str(parent) if any(parent == root or root in parent.parents for root in allowed) else None
    return {"path": str(target), "parent": parent_value, "folders": folders[:250]}


@app.post("/api/storage/mkdir", dependencies=[Depends(require_auth)])
async def create_storage_folder(request: Request) -> dict[str, str]:
    payload = await request.json()
    parent = storage_path(str(payload.get("parent", "")))
    name = str(payload.get("name", "")).strip()
    if not name or name in {".", ".."} or "/" in name or "\\" in name:
        raise HTTPException(400, "Nom de dossier invalide")
    target = (parent / name).resolve()
    if parent not in target.parents:
        raise HTTPException(400, "Chemin de dossier invalide")
    try:
        target.mkdir(parents=False, exist_ok=False)
    except FileExistsError as exc:
        raise HTTPException(409, "Ce dossier existe déjà") from exc
    except OSError as exc:
        raise HTTPException(500, "Création du dossier impossible") from exc
    return {"path": str(target)}


@app.exception_handler(Exception)
async def generic_exception_handler(_: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(status_code=500, content={"detail": str(exc)})
