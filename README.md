# Advanced Splunk Export Console

Authenticated FastAPI console for supervising parallel exports from Splunk Cloud through [`tmuth/splunk-export`](https://github.com/tmuth/splunk-export).

## Features

- Configured export jobs with masked Splunk tokens
- Pre-export analysis and partition recommendation
- Runner supervision: state, logs, CPU/RAM, real network RX/TX, rolling throughput and partition progress
- Export result dashboard with duration, average throughput, event/file counts, individual or ZIP downloads, and confirmed deletion of selected output files
- Graphical storage-volume and folder picker
- Orphan-run recovery after a console restart

## Deployment outline

The console expects this layout:

```text
/opt/splunk-cloud-export/
  app/                 # this repository
  splunk-export/       # upstream tmuth/splunk-export checkout
  venv/
  data/ configs/ runs/
```

Install the Python dependencies, clone the upstream runner into `splunk-export/`, then install `systemd/splunk-cloud-export-8082.service`.

The systemd environment file `/etc/splunk-cloud-export/app.env` must contain:

```ini
APP_USER=...
APP_PASSWORD=...
ARCHIVE_PATH=/data/archive
```

Never commit Splunk tokens, the environment file, the SQLite database, logs, or exported archives.
