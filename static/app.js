const $ = (id) => document.getElementById(id);
let jobs = [];
let selectedJob = null;
let authenticated = false;
let selectedStoragePath = "/data/archive";

function bytes(value) {
  if (!value) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB", "PB"];
  let n = Number(value);
  let i = 0;
  while (n >= 1024 && i < units.length - 1) { n /= 1024; i += 1; }
  return `${n.toFixed(n >= 10 || i === 0 ? 0 : 1)} ${units[i]}`;
}

function seconds(value) {
  value = Math.floor(value || 0);
  const h = Math.floor(value / 3600);
  const m = Math.floor((value % 3600) / 60);
  const s = value % 60;
  if (h) return `${h}h ${m}m`;
  if (m) return `${m}m ${s}s`;
  return `${s}s`;
}

function number(value) {
  return new Intl.NumberFormat("fr-FR").format(value || 0);
}

async function api(path, options = {}) {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    credentials: "same-origin",
    ...options,
  });
  if (res.status === 401) {
    showLogin();
  }
  if (!res.ok) throw new Error((await res.json()).detail || res.statusText);
  return res.json();
}

function showLogin(message = "") {
  authenticated = false;
  $("loginScreen").classList.remove("hidden");
  $("loginError").textContent = message;
}

function hideLogin() {
  authenticated = true;
  $("loginScreen").classList.add("hidden");
  $("loginError").textContent = "";
}

function setView(name) {
  document.querySelectorAll(".view").forEach((v) => v.classList.toggle("hidden", v.id !== name));
  document.querySelectorAll("nav button").forEach((b) => b.classList.toggle("active", b.dataset.view === name));
}

function fillForm(job = {}) {
  selectedJob = job.id || null;
  const fields = ["id", "name", "splunk_host", "splunk_port", "auth_token", "indexes", "sourcetypes", "begin_date", "end_date", "partition_units", "partition_interval", "parallel_processes", "output_format", "resume_mode", "max_file_size_mb", "archive_path", "extra"];
  $("jobId").value = job.id || "";
  fields.filter((f) => f !== "id").forEach((f) => {
    if ($(f)) $(f).value = job[f] ?? $(f).defaultValue ?? "";
  });
  $("gzip").checked = job.gzip === undefined ? true : Boolean(job.gzip);
  $("analysisTitle").textContent = "Aucune analyse lancee";
  $("analysisText").textContent = "Lance l'analyse apres avoir renseigne l'index et la periode. Les champs techniques seront ajustes automatiquement.";
}

function formPayload() {
  return {
    id: $("jobId").value ? Number($("jobId").value) : null,
    name: $("name").value,
    splunk_host: $("splunk_host").value,
    splunk_port: Number($("splunk_port").value || 8089),
    auth_token: $("auth_token").value,
    indexes: $("indexes").value,
    sourcetypes: $("sourcetypes").value,
    begin_date: $("begin_date").value,
    end_date: $("end_date").value,
    partition_units: $("partition_units").value,
    partition_interval: Number($("partition_interval").value || 1),
    parallel_processes: Number($("parallel_processes").value || 4),
    output_format: $("output_format").value,
    gzip: $("gzip").checked,
    resume_mode: $("resume_mode").value,
    max_file_size_mb: Number($("max_file_size_mb").value || 1000),
    archive_path: $("archive_path").value,
    extra: $("extra").value,
  };
}

function applyAnalysis(data) {
  const rec = data.recommendation;
  $("partition_units").value = rec.partition_units;
  $("partition_interval").value = rec.partition_interval;
  $("parallel_processes").value = rec.parallel_processes;
  $("analysisTitle").textContent = `${number(data.event_count)} events trouves`;
  $("analysisText").textContent = [
    `${number(data.non_empty_days)} jours avec donnees sur ${number(data.bucket_count)} jours analyses.`,
    `Pic journalier: ${number(data.max_day_count)} events.`,
    `Recommendation: ${rec.estimated_partitions} partitions en ${rec.partition_units} par ${rec.partition_interval}, ${rec.parallel_processes} workers.`,
    rec.reason,
  ].join(" ");
}

function renderJobs() {
  const selectedRunner = $("jobSelect").value;
  const selectedResults = $("resultJobSelect").value;
  $("jobList").innerHTML = jobs.map((job) => `
    <div class="job-card">
      <div>
        <strong>${job.name}</strong>
        <small>${job.indexes} -> ${job.archive_path}</small>
      </div>
      <button data-edit="${job.id}">${job.status}</button>
    </div>
  `).join("") || "<small>Aucun job configuré.</small>";
  $("jobSelect").innerHTML = jobs.map((job) => `<option value="${job.id}">${job.name}</option>`).join("");
  $("resultJobSelect").innerHTML = jobs.map((job) => `<option value="${job.id}">${job.name}</option>`).join("");
  if (jobs.some((job) => String(job.id) === selectedRunner)) $("jobSelect").value = selectedRunner;
  if (jobs.some((job) => String(job.id) === selectedResults)) $("resultJobSelect").value = selectedResults;
  document.querySelectorAll("[data-edit]").forEach((btn) => btn.addEventListener("click", () => {
    fillForm(jobs.find((j) => j.id === Number(btn.dataset.edit)));
    setView("jobs");
  }));
}

async function refreshOverview() {
  const data = await api("/api/overview");
  $("archivePath").textContent = data.archive_path;
  $("archiveFree").textContent = `${bytes(data.archive.disk_free)} libres`;
  $("metricVolume").textContent = bytes(data.archive.bytes);
  $("metricFree").textContent = bytes(data.archive.disk_free);
  $("metricCpu").textContent = `${Math.round(data.system.cpu_percent)}%`;
  $("metricRam").textContent = `${Math.round(data.system.memory.percent)}%`;
}

async function refreshJobs() {
  jobs = await api("/api/jobs");
  renderJobs();
}

async function refreshMetrics() {
  const id = $("jobSelect").value || selectedJob || (jobs[0] && jobs[0].id);
  if (!id) return;
  const data = await api(`/api/jobs/${id}/metrics`);
  $("runStatus").textContent = data.job.status;
  $("runRate").textContent = data.alive
    ? `${Number(data.partitions_per_second || 0).toFixed(1)} part./s`
    : "Terminé";
  const dataRate = data.alive ? data.rolling_bytes_per_second : data.bytes_per_second;
  $("runDataRate").textContent = `${bytes(dataRate)}/s`;
  $("runDuration").textContent = seconds(data.elapsed_seconds);
  $("runWorkers").textContent = `${data.configured_workers} configured / ${data.process_threads} proc`;
  $("runPid").textContent = data.pid || "-";
  $("runFiles").textContent = `${number(data.partitions_completed)} / ${number(data.partitions_total)}`;
  $("runNetworkDown").textContent = `${bytes(data.network?.rx_bytes_per_second)}/s`;
  $("runNetworkUp").textContent = `${bytes(data.network?.tx_bytes_per_second)}/s`;
  $("logTail").textContent = data.log_tail || "Pas encore de logs pour ce job.";
  $("globalStatus").textContent = data.alive ? "Runner actif" : "Ready";
}

function selectedResultFiles() {
  return [...document.querySelectorAll("[data-result-file]:checked")].map((input) => input.value);
}

async function refreshResults() {
  const id = $("resultJobSelect").value || selectedJob || (jobs[0] && jobs[0].id);
  if (!id) return;
  const data = await api(`/api/jobs/${id}/results`);
  $("resultDuration").textContent = seconds(data.summary.elapsed_seconds);
  $("resultRate").textContent = `${bytes(data.summary.bytes_per_second)}/s`;
  $("resultEvents").textContent = data.summary.event_count === null ? "En attente" : number(data.summary.event_count);
  $("resultFileCount").textContent = number(data.summary.file_count);
  $("resultVolume").textContent = bytes(data.summary.bytes);
  $("resultStatus").textContent = data.run ? `Dernière exécution : ${data.run.status}` : "Ce job n’a pas encore été lancé.";
  $("resultFiles").innerHTML = data.files.length ? data.files.map((file) => `
    <label class="file-row"><input type="checkbox" data-result-file value="${file.path}" /><code>${file.path}</code><small>${bytes(file.size)}</small><a href="/api/jobs/${id}/files/download?file=${encodeURIComponent(file.path)}">Télécharger</a></label>
  `).join("") : "<div class=\"empty-files\">Aucun fichier exporté pour ce job.</div>";
}

async function downloadSelectedFiles() {
  const id = $("resultJobSelect").value;
  const files = selectedResultFiles();
  if (!files.length) return alert("Sélectionne au moins un fichier.");
  const response = await fetch(`/api/jobs/${id}/files/archive`, { method: "POST", headers: { "Content-Type": "application/json" }, credentials: "same-origin", body: JSON.stringify({ files }) });
  if (!response.ok) throw new Error((await response.json()).detail || "Téléchargement impossible");
  const blob = await response.blob();
  const link = document.createElement("a");
  link.href = URL.createObjectURL(blob);
  link.download = "splunk-export.zip";
  link.click();
  URL.revokeObjectURL(link.href);
}

async function deleteSelectedFiles() {
  const id = $("resultJobSelect").value;
  const files = selectedResultFiles();
  if (!files.length) return alert("Sélectionne au moins un fichier.");
  if (!window.confirm(`Supprimer définitivement ${files.length} fichier(s) d’export ?`)) return;
  const result = await api(`/api/jobs/${id}/files`, { method: "DELETE", body: JSON.stringify({ files }) });
  await refreshResults();
  await refreshOverview();
  alert(`${result.deleted} fichier(s) supprimé(s), ${bytes(result.freed_bytes)} libérés.`);
}

document.querySelectorAll("nav button").forEach((b) => b.addEventListener("click", () => setView(b.dataset.view)));
$("newJobBtn").addEventListener("click", () => { fillForm({ archive_path: "/data/archive" }); setView("jobs"); });
$("jobForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  const job = await api("/api/jobs", { method: "POST", body: JSON.stringify(formPayload()) });
  fillForm(job);
  await refreshJobs();
  setView("dashboard");
});
$("analyzeBtn").addEventListener("click", async () => {
  $("analysisTitle").textContent = "Analyse en cours...";
  $("analysisText").textContent = "Connexion Splunk Cloud et comptage tstats par jour.";
  $("analyzeBtn").disabled = true;
  try {
    const data = await api("/api/analyze", { method: "POST", body: JSON.stringify(formPayload()) });
    applyAnalysis(data);
  } catch (err) {
    $("analysisTitle").textContent = "Analyse impossible";
    $("analysisText").textContent = err.message || "Erreur inconnue";
  } finally {
    $("analyzeBtn").disabled = false;
  }
});
$("startBtn").addEventListener("click", async () => {
  const id = $("jobId").value;
  if (!id) throw new Error("Enregistre le job avant de le lancer.");
  await api(`/api/jobs/${id}/start`, { method: "POST" });
  await refreshJobs();
  setView("logs");
});
$("stopBtn").addEventListener("click", async () => {
  const id = $("jobId").value || $("jobSelect").value;
  if (id) await api(`/api/jobs/${id}/stop`, { method: "POST" });
});
$("jobSelect").addEventListener("change", refreshMetrics);
$("resultJobSelect").addEventListener("change", refreshResults);
$("selectAllFilesBtn").addEventListener("click", () => document.querySelectorAll("[data-result-file]").forEach((input) => { input.checked = true; }));
$("clearFilesBtn").addEventListener("click", () => document.querySelectorAll("[data-result-file]").forEach((input) => { input.checked = false; }));
$("downloadSelectedBtn").addEventListener("click", () => downloadSelectedFiles().catch((err) => alert(err.message)));
$("deleteSelectedBtn").addEventListener("click", () => deleteSelectedFiles().catch((err) => alert(err.message)));
async function browseStorage(path) {
  const data = await api(`/api/storage/browse?path=${encodeURIComponent(path)}`);
  selectedStoragePath = data.path;
  $("storagePath").textContent = data.path;
  const parent = data.parent ? `<button class="folder-choice" data-folder="${data.parent}">.. (dossier parent)</button>` : "";
  $("storageFolders").innerHTML = parent + data.folders.map((folder) => `<button class="folder-choice" data-folder="${folder.path}">${folder.name}</button>`).join("") || "<small>Aucun sous-dossier.</small>";
  document.querySelectorAll("[data-folder]").forEach((button) => button.addEventListener("click", () => browseStorage(button.dataset.folder)));
}

async function openStoragePicker() {
  const data = await api("/api/storage/volumes");
  $("storageVolumes").innerHTML = data.volumes.map((volume) => `<button class="volume-choice" data-volume="${volume.path}"><strong>${volume.path}</strong><small>${volume.filesystem} · ${bytes(volume.free)} libres / ${bytes(volume.total)}</small></button>`).join("");
  document.querySelectorAll("[data-volume]").forEach((button) => button.addEventListener("click", () => browseStorage(button.dataset.volume)));
  $("storageModal").classList.remove("hidden");
  await browseStorage($("archive_path").value || data.default_path);
}

$("browseStorageBtn").addEventListener("click", () => openStoragePicker().catch((err) => alert(err.message)));
$("closeStorageBtn").addEventListener("click", () => $("storageModal").classList.add("hidden"));
$("useStorageBtn").addEventListener("click", () => { $("archive_path").value = selectedStoragePath; $("storageModal").classList.add("hidden"); });
$("newFolderBtn").addEventListener("click", () => {
  const name = window.prompt("Nom du nouveau dossier de stockage :");
  if (!name) return;
  api("/api/storage/mkdir", { method: "POST", body: JSON.stringify({ parent: selectedStoragePath, name }) })
    .then((data) => browseStorage(data.path))
    .catch((err) => alert(err.message));
});
$("loginForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  $("loginError").textContent = "";
  try {
    await api("/api/login", {
      method: "POST",
      body: JSON.stringify({
        username: $("loginUser").value,
        password: $("loginPassword").value,
      }),
    });
    hideLogin();
    await boot();
  } catch (err) {
    showLogin(err.message || "Login impossible");
  }
});

async function boot() {
  await api("/api/me");
  hideLogin();
  await refreshOverview();
  await refreshJobs();
  if (jobs[0]) fillForm(jobs[0]);
  await refreshMetrics();
  await refreshResults();
}

boot().catch((err) => {
  showLogin(err.message === "Not authenticated" ? "" : err.message);
});
setInterval(() => { if (authenticated) refreshOverview().catch(() => {}); }, 5000);
setInterval(() => { if (authenticated) refreshJobs().catch(() => {}); }, 7000);
setInterval(() => { if (authenticated) refreshMetrics().catch(() => {}); }, 2000);
setInterval(() => { if (authenticated && !$("results").classList.contains("hidden")) refreshResults().catch(() => {}); }, 5000);
