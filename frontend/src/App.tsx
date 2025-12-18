import React, { useEffect, useMemo, useRef, useState } from "react"
import { apiClient } from "./lib/api"
import "./app.css"

type NavKey = "dashboard" | "uploads" | "jobRuns" | "incidents" | "reports" | "jobs"
type PillTone = "live" | "pipeline" | "execution" | "rca" | "workflow" | "exports"
type QueuedUploadFile = { id: string; file: File }

const NAV: Array<{
  key: NavKey
  label: string
  icon: string
  pill: { text: string; tone: PillTone }
}> = [
  { key: "dashboard", label: "Dashboard", icon: "D", pill: { text: "LIVE", tone: "live" } },
  { key: "uploads", label: "Uploads", icon: "U", pill: { text: "PIPELINE", tone: "pipeline" } },
  { key: "jobRuns", label: "Job Runs", icon: "J", pill: { text: "EXECUTION", tone: "execution" } },
  { key: "incidents", label: "Incidents", icon: "I", pill: { text: "RCA", tone: "rca" } },
  { key: "reports", label: "Reports", icon: "R", pill: { text: "EXPORTS", tone: "exports" } },
  { key: "jobs", label: "Jobs", icon: "S", pill: { text: "CRON", tone: "workflow" } },
]

const MAX_UPLOAD_FILES = 5
const LAST_NAV_KEY = "flowboard:lastNav"
const LAST_UPLOAD_KEY = "flowboard:lastUpload"
const UPLOAD_QUEUE_DB = "flowboardUploadQueue"
const UPLOAD_QUEUE_STORE = "files"

function cx(...xs: Array<string | false | null | undefined>) {
  return xs.filter(Boolean).join(" ")
}

function Pill({ tone, children }: { tone: PillTone; children: React.ReactNode }) {
  return <span className={cx("pill", `pill-${tone}`)}>{children}</span>
}

type DRFPage<T> = { results?: T[]; count?: number; next?: string | null; previous?: string | null } | T[]

/** ✅ Works whether apiClient returns AxiosResponse (.data) OR already-unwrapped json */
function unwrapList<T = any>(res: any): T[] {
  const payload: DRFPage<T> | undefined = res?.data ?? res
  if (!payload) return []
  // @ts-ignore
  return (payload as any).results ?? payload ?? []
}

function baseURL() {
  return (import.meta as any).env?.VITE_API_BASE_URL || "http://localhost:8000"
}

async function postJSON(path: string, body: any) {
  const token = localStorage.getItem("jwt_token")
  const res = await fetch(`${baseURL()}${path}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    body: JSON.stringify(body ?? {}),
  })
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
  return res.json().catch(() => ({}))
}

async function getJSON(path: string) {
  const token = localStorage.getItem("jwt_token")
  const res = await fetch(`${baseURL()}${path}`, {
    method: "GET",
    headers: {
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
  })
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
  return res.json()
}

function Segments({
  value,
  onChange,
  options,
}: {
  value: string
  onChange: (v: string) => void
  options: Array<{ value: string; label: string }>
}) {
  return (
    <div className="segments">
      {options.map((opt) => (
        <button
          key={opt.value}
          className={cx("seg", value === opt.value && "seg-active")}
          onClick={() => onChange(opt.value)}
          type="button"
        >
          {opt.label}
        </button>
      ))}
    </div>
  )
}

function BadgeRow({
  items,
}: {
  items: Array<{ label: string; value: number; tone: "muted" | "blue" | "green" | "red" | "amber" }>
}) {
  return (
    <div className="badgeRow">
      {items.map((it) => (
        <span key={it.label} className={cx("badge", `badge-${it.tone}`)}>
          <span className="badgeLabel">{it.label}:</span> {it.value}
        </span>
      ))}
    </div>
  )
}

function Card({
  children,
  className,
  style,
}: {
  children: React.ReactNode
  className?: string
  style?: React.CSSProperties
}) {
  return (
    <div className={cx("card", className)} style={style}>
      {children}
    </div>
  )
}

function CardTitle({ title, right }: { title: string; right?: React.ReactNode }) {
  return (
    <div className="cardTitle">
      <div className="cardTitleText">{title}</div>
      <div className="cardTitleRight">{right}</div>
    </div>
  )
}

function DetailModal({
  title,
  subtitle,
  onClose,
  children,
}: {
  title: string
  subtitle?: React.ReactNode
  onClose: () => void
  children: React.ReactNode
}) {
  return (
    <div className="modalBackdrop" onClick={onClose}>
      <div className="modal detailModal" onClick={(e) => e.stopPropagation()}>
        <div className="modalHeader">
          <div>
            <div className="modalTitle">{title}</div>
            {subtitle ? <div className="modalSubtitle">{subtitle}</div> : null}
          </div>
          <button className="modalClose" type="button" onClick={onClose}>
            &times;
          </button>
        </div>
        <div className="modalBody">{children}</div>
      </div>
    </div>
  )
}

function KpiCard({ label, value, hint }: { label: string; value: React.ReactNode; hint: string }) {
  return (
    <div className="kpi">
      <div className="kpiLabel">{label}</div>
      <div className="kpiValue">{value}</div>
      <div className="kpiHint">{hint}</div>
    </div>
  )
}

const INDIAN_LOCALE = "en-IN"
const INDIAN_DATE_FORMATTER = new Intl.DateTimeFormat(INDIAN_LOCALE, {
  hour12: true,
  year: "numeric",
  month: "short",
  day: "numeric",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
})
const IST_OFFSET_MINUTES = 330

function parseDateInput(input?: string): Date | null {
  if (!input) return null
  let value = String(input).trim()
  const hasTime = /[tT]/.test(value)
  if (!hasTime && /^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}/.test(value)) {
    value = value.replace(/\s+/, "T")
  }
  const hasZone = /([zZ]|[+-]\d{2}:?\d{2})$/.test(value)
  if (!hasZone) {
    value = `${value}Z`
  }
  const parsed = new Date(value)
  if (!Number.isNaN(parsed.getTime())) return parsed
  const fallback = new Date(input)
  return Number.isNaN(fallback.getTime()) ? null : fallback
}

function convertToIST(date: Date) {
  return new Date(date.getTime() + (IST_OFFSET_MINUTES + 30) * 60000)
}

function fmtDate(s?: string) {
  if (!s) return "—"
  try {
    const d = parseDateInput(s)
    if (!d || Number.isNaN(d.getTime())) return String(s)
    const istDate = convertToIST(d)
    return INDIAN_DATE_FORMATTER.format(istDate)
  } catch {
    return String(s)
  }
}

function clampText(s: any, n = 120) {
  const t = String(s ?? "")
  if (t.length <= n) return t
  return t.slice(0, n) + "…"
}

function formatFileSize(bytes?: number | null) {
  if (typeof bytes !== "number" || Number.isNaN(bytes) || bytes <= 0) return "—"
  const units = ["B", "KB", "MB", "GB", "TB"]
  let size = bytes
  let unitIdx = 0
  while (size >= 1024 && unitIdx < units.length - 1) {
    size /= 1024
    unitIdx += 1
  }
  const precision = size >= 10 || unitIdx === 0 ? 0 : 1
  return `${size.toFixed(precision)} ${units[unitIdx]}`
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function triggerCsvDownload(data: BlobPart, uploadId: string) {
  const blob = new Blob([data], { type: "text/csv" })
  const link = document.createElement("a")
  link.href = window.URL.createObjectURL(blob)
  link.download = `summary-${uploadId}.csv`
  document.body.appendChild(link)
  link.click()
  document.body.removeChild(link)
}

function supportsIndexedDB() {
  return typeof window !== "undefined" && typeof window.indexedDB !== "undefined"
}

function openUploadQueueDB(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    if (!supportsIndexedDB()) {
      reject(new Error("IndexedDB not supported"))
      return
    }
    const req = window.indexedDB.open(UPLOAD_QUEUE_DB, 1)
    req.onerror = () => reject(req.error ?? new Error("Failed to open upload queue DB"))
    req.onupgradeneeded = () => {
      const db = req.result
      if (!db.objectStoreNames.contains(UPLOAD_QUEUE_STORE)) {
        db.createObjectStore(UPLOAD_QUEUE_STORE, { keyPath: "id" })
      }
    }
    req.onsuccess = () => resolve(req.result)
  })
}

async function persistQueuedFiles(list: QueuedUploadFile[]) {
  if (!supportsIndexedDB()) return
  const db = await openUploadQueueDB()
  await new Promise<void>((resolve, reject) => {
    const tx = db.transaction(UPLOAD_QUEUE_STORE, "readwrite")
    const store = tx.objectStore(UPLOAD_QUEUE_STORE)
    const clearReq = store.clear()
    clearReq.onerror = () => reject(clearReq.error ?? new Error("Failed clearing queue"))
    clearReq.onsuccess = () => {
      list.forEach(({ id, file }) => {
        const record = {
          id,
          name: file.name,
          type: file.type,
          size: file.size,
          lastModified: file.lastModified,
          file,
        }
        store.put(record)
      })
    }
    tx.oncomplete = () => resolve()
    tx.onerror = () => reject(tx.error ?? new Error("Failed saving queue"))
  })
  db.close()
}

async function loadQueuedFiles(): Promise<QueuedUploadFile[]> {
  if (!supportsIndexedDB()) return []
  const db = await openUploadQueueDB()
  const records = await new Promise<any[]>((resolve, reject) => {
    const tx = db.transaction(UPLOAD_QUEUE_STORE, "readonly")
    const store = tx.objectStore(UPLOAD_QUEUE_STORE)
    const req = store.getAll()
    req.onerror = () => reject(req.error ?? new Error("Failed loading queue"))
    req.onsuccess = () => resolve(req.result ?? [])
  })
  db.close()
  return records.map((record) => {
    const blob: Blob | undefined = record?.file
    const blobName = blob ? (blob as any).name : undefined
    const name = record?.name || blobName || "upload"
    const type = record?.type || blob?.type || ""
    const lastModified = record?.lastModified ?? Date.now()
    let file: File
    if (blob instanceof Blob) {
      file = new File([blob], name, { type, lastModified })
    } else {
      file = new File([], name, { type, lastModified })
    }
    return { id: record?.id || crypto.randomUUID?.() || String(Math.random()), file }
  })
}

function makeQueueId() {
  if (typeof crypto !== "undefined" && crypto.randomUUID) return crypto.randomUUID()
  return `${Date.now()}-${Math.random().toString(16).slice(2)}`
}

async function ensureReportDownloaded(uploadId: string, tries = 4) {
  try {
    const uploadData = await apiClient.uploads.get(uploadId)
    if (uploadData?.report_csv) {
      triggerCsvDownload(uploadData.report_csv, uploadId)
      return
    }
  } catch {
    // ignore and fall back to API polling
  }

  const token = localStorage.getItem("jwt_token")
  for (let attempt = 0; attempt < tries; attempt += 1) {
    const res = await fetch(`${baseURL()}/api/reports/summary/?upload_id=${encodeURIComponent(uploadId)}`, {
      headers: token ? { Authorization: `Bearer ${token}` } : undefined,
    })
    if (res.ok) {
      const blob = await res.blob()
      triggerCsvDownload(blob, uploadId)
      return
    }
    if (res.status === 404 || res.status === 409) {
      await sleep(1500)
      continue
    }
    if (res.status === 400) {
      const body = await res.json().catch(() => ({}))
      throw new Error(body?.error || "Report not available yet.")
    }
    throw new Error(`Download failed (${res.status}).`)
  }
  throw new Error("Report still generating. Try again shortly.")
}

function formatStepName(name?: string) {
  const text = String(name ?? "").replace(/_/g, " ").trim()
  if (!text) return "—"
  return text
    .split(" ")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ")
}

function summarizeSteps(details: any) {
  const steps = details?.steps
  if (!Array.isArray(steps) || steps.length === 0) return "—"
  return steps
    .map((step: any) => `${formatStepName(step.name)}: ${step.status}`)
    .join(" · ")
}

/** ---------------- PAGES ---------------- */

function DashboardPage() {
  const [metricsWarning, setMetricsWarning] = useState<string | null>(null)

  const [todayUploads, setTodayUploads] = useState<number>(0)
  const [openIncidents, setOpenIncidents] = useState<number>(0)
  const [openIncidentTasks, setOpenIncidentTasks] = useState<number>(0)
  const [lastMttr, setLastMttr] = useState<string>("—")

  const [health, setHealth] = useState<Record<string, string> | null>(null)

  useEffect(() => {
    let mounted = true

    async function load() {
      // KPIs from existing endpoints
      try {
        const [uploadsRes, incidentsRes] = await Promise.all([
          apiClient.uploads.list({ ordering: "-received_at" }),
          apiClient.incidents.list({ state: "open", ordering: "-created_at" }),
        ])
        if (!mounted) return

        const uploads = unwrapList<any>(uploadsRes)
        const incidents = unwrapList<any>(incidentsRes)

        const today = new Date()
        const yyyy = today.getFullYear()
        const mm = String(today.getMonth() + 1).padStart(2, "0")
        const dd = String(today.getDate()).padStart(2, "0")
        const prefix = `${yyyy}-${mm}-${dd}`

        setTodayUploads(uploads.filter((u) => String(u.received_at ?? "").startsWith(prefix)).length)
        setOpenIncidents(incidents.length)
        setOpenIncidentTasks(incidents.length)

        // last MTTR: latest resolved ticket (best-effort)
        const resolved = [] as any[] // MTTR from incidents could be added later
        if (resolved.length > 0) {
          resolved.sort((a, b) => String(b.resolved_at).localeCompare(String(a.resolved_at)))
          const t = resolved[0]
          const start = new Date(t.created_at).getTime()
          const end = new Date(t.resolved_at).getTime()
          const mins = Math.max(0, Math.round((end - start) / 60000))
          setLastMttr(`${mins} min`)
        } else {
          setLastMttr("—")
        }
      } catch {
        // ignore
      }

      // metrics warning: your backend /api/metrics is Prometheus text, not JSON
      // so we intentionally show the same warning you wanted in the UI
      try {
        const res = await fetch(`${baseURL()}/api/metrics`, { method: "GET" })
        const text = await res.text()
        try {
          JSON.parse(text)
          if (!mounted) return
          setMetricsWarning(null)
        } catch {
          if (!mounted) return
          setMetricsWarning("Bad JSON from /api/metrics")
        }
      } catch {
        if (!mounted) return
        setMetricsWarning("Bad JSON from /api/metrics")
      }

      // optional: /api/health/ (if you add later, this auto-fills)
      try {
        const data = await getJSON(`/api/health/`)
        if (!mounted) return
        setHealth(data)
      } catch {
        if (!mounted) return
        setHealth(null)
      }
    }

    load()
    const t = setInterval(load, 4000)
    return () => {
      mounted = false
      clearInterval(t)
    }
  }, [])

  return (
    <div className="page">
      <div className="pageHeader">
        <div>
          <div className="pageTitle">Control room</div>
          <div className="pageSub">At-a-glance view of uploads, incidents, tickets and system health.</div>
          {metricsWarning ? <div className="warnPill">● {metricsWarning}</div> : null}
        </div>

        <div className="rightHeader">
          <div className="pipelinePill">
            <span className="dot" /> <span className="muted">Pipeline idle</span>
            <span className="muted">Waiting for next upload</span>
          </div>
        </div>
      </div>

      <div className="kpiGrid">
        <Card>
          <KpiCard label="TODAY’S UPLOADS" value={todayUploads} hint="Files ingested today" />
        </Card>
        <Card>
          <KpiCard label="OPEN INCIDENTS" value={openIncidents} hint="Need attention" />
        </Card>
        <Card>
          <KpiCard label="OPEN INCIDENT TASKS" value={openIncidentTasks} hint="System + manual incidents" />
        </Card>
        <Card>
          <KpiCard label="LAST RUN MTTR" value={lastMttr} hint="Last incident" />
        </Card>
      </div>

      <div className="grid2">
        <Card>
          <CardTitle title="Pipeline overview" right={<span className="miniPill">Standardize → Validate → Transform → Publish</span>} />
          <div className="pipelineBoxes">
            <div className="pipelineBox">
              <div className="pipelineBoxTitle">Ingest</div>
              <div className="pipelineBoxText">Files land in staging and are queued.</div>
            </div>
            <div className="pipelineBox">
              <div className="pipelineBoxTitle">Validation</div>
              <div className="pipelineBoxText">Rules, type checks and transforms run via workers.</div>
            </div>
            <div className="pipelineBox">
              <div className="pipelineBoxTitle">Output</div>
              <div className="pipelineBoxText">Clean tables and exports are written for consumers.</div>
            </div>
          </div>
        </Card>

        <Card>
          <CardTitle title="System health" right={<span className="muted">Status from /api/health</span>} />
          {!health ? (
            <div className="muted">No health data received.</div>
          ) : (
            <div className="healthList">
              {Object.entries(health).map(([k, v]) => (
                <div key={k} className="healthRow">
                  <span className="healthName">{k}</span>
                  <span className={cx("healthStatus", String(v).toLowerCase().includes("healthy") ? "ok" : "bad")}>
                    {String(v)}
                  </span>
                </div>
              ))}
            </div>
          )}
        </Card>
      </div>

      <Card style={{ marginTop: 16 }}>
        <div className="sectionTitle">School-friendly automation ideas</div>
        <ul className="plainList">
          <li><span className="mono">core.automation.tasks.send_attendance_reminders</span> – weekday attendance nudges.</li>
          <li><span className="mono">core.automation.tasks.send_system_status_digest</span> – daily system health email.</li>
          <li><span className="mono">core.automation.tasks.run_web_scrape</span> – scrape exam/admissions portals.</li>
          <li><span className="mono">core.automation.tasks.schedule_file_ingest</span> – nightly ingest per department.</li>
          <li><span className="mono">core.automation.tasks.purge_old_records</span> – Sunday cleanup & archiving.</li>
          <li><span className="mono">core.automation.tasks.run_daily_backup</span> – off-peak storage backups.</li>
        </ul>
      </Card>
    </div>
  )
}

function UploadsPage() {
  const [department, setDepartment] = useState("Examination")
  const [notes, setNotes] = useState("")
  const [queuedFiles, setQueuedFiles] = useState<QueuedUploadFile[]>([])
  const [fileError, setFileError] = useState<string | null>(null)
  const fileInputRef = useRef<HTMLInputElement | null>(null)
  const [submitting, setSubmitting] = useState(false)
  const filesAtLimit = queuedFiles.length >= MAX_UPLOAD_FILES

  const [currentUpload, setCurrentUpload] = useState<any | null>(null)
  const [uploadError, setUploadError] = useState<string | null>(null)
  const [reportHint, setReportHint] = useState<string | null>(null)
  const [reportLoading, setReportLoading] = useState(false)

  const [currentRuns, setCurrentRuns] = useState<any[]>([])
  const [currentIncident, setCurrentIncident] = useState<any | null>(null)
  const hasStoredUploadRef = useRef(false)

  useEffect(() => {
    if (currentUpload) return
    if (typeof window === "undefined") return
    const savedId = localStorage.getItem(LAST_UPLOAD_KEY)
    if (!savedId) return
    let mounted = true
    async function restore() {
      try {
        const uploadData = await getJSON(`/api/uploads/${savedId}/`)
        if (!mounted) return
        setCurrentUpload(uploadData)
      } catch {
        if (mounted) {
          try {
            localStorage.removeItem(LAST_UPLOAD_KEY)
          } catch {
            // ignore
          }
        }
      }
    }
    restore()
    return () => {
      mounted = false
    }
  }, [currentUpload]) // run once on mount or until current upload restored

  useEffect(() => {
    if (typeof window === "undefined") return
    const id = currentUpload?.upload_id
    if (id) {
      try {
        localStorage.setItem(LAST_UPLOAD_KEY, String(id))
        hasStoredUploadRef.current = true
      } catch {
        // ignore storage failures
      }
    } else if (hasStoredUploadRef.current) {
      try {
        localStorage.removeItem(LAST_UPLOAD_KEY)
        hasStoredUploadRef.current = false
      } catch {
        // ignore storage failures
      }
    }
  }, [currentUpload?.upload_id])

  // Poll current upload status (so your “Start pipeline” feels real)
  useEffect(() => {
    if (!currentUpload?.upload_id) return
    let mounted = true

    async function poll() {
      try {
        const id = currentUpload.upload_id
        const [uploadData, runsData, incidentsData] = await Promise.all([
          getJSON(`/api/uploads/${id}/`),
          getJSON(`/api/job-runs/?upload_id=${id}&ordering=started_at`),
          getJSON(`/api/incidents/?upload_id=${id}&ordering=-created_at`),
        ])
        if (!mounted) return
        setCurrentUpload(uploadData)

        const runs = unwrapList<any>(runsData)
        setCurrentRuns(runs)

        const incidents = unwrapList<any>(incidentsData)
        setCurrentIncident(incidents[0] ?? null)
      } catch {
        // ignore
      }
    }

    poll()
    const t = setInterval(poll, 2500)
    return () => {
      mounted = false
      clearInterval(t)
    }
  }, [currentUpload?.upload_id])

  useEffect(() => {
    let cancelled = false
    async function restoreQueue() {
      try {
        const restored = await loadQueuedFiles()
        if (!cancelled && restored.length > 0) {
          setQueuedFiles(restored.slice(0, MAX_UPLOAD_FILES))
        }
      } catch {
        // ignore
      }
    }
    restoreQueue()
    return () => {
      cancelled = true
    }
  }, [])

  function handleFileSelect(e: React.ChangeEvent<HTMLInputElement>) {
    const picked = Array.from(e.target.files ?? [])
    if (picked.length === 0) return
    setQueuedFiles((prev) => {
      const remaining = Math.max(0, MAX_UPLOAD_FILES - prev.length)
      const accepted = picked.slice(0, remaining).map((file) => ({ id: makeQueueId(), file }))
      if (accepted.length === 0) {
        setFileError(`You can upload up to ${MAX_UPLOAD_FILES} files at once. Remove one to add another.`)
        return prev
      }
      const next = [...prev, ...accepted]
      if (picked.length > remaining) {
        setFileError(`Only the first ${remaining} file(s) were added (queue limit ${MAX_UPLOAD_FILES}).`)
      } else {
        setFileError(null)
      }
      persistQueuedFiles(next).catch(() => {})
      return next
    })
    if (fileInputRef.current) {
      fileInputRef.current.value = ""
    }
  }

  function removeFile(index: number) {
    setQueuedFiles((prev) => {
      const next = prev.filter((_, idx) => idx !== index)
      persistQueuedFiles(next).catch(() => {})
      return next
    })
    setFileError(null)
  }

  async function startPipeline() {
    if (queuedFiles.length === 0) return
    setUploadError(null)
    setSubmitting(true)
    try {
      const uploadOne = async (f: File) => {
        const fd = new FormData()
        fd.append("file", f)
        fd.append("department", department)
        fd.append("notes", notes)
        const res: any = await apiClient.uploads.upload(fd)
        return res?.data ?? res
      }

      const results = await Promise.allSettled(queuedFiles.map((entry) => uploadOne(entry.file)))
      const successes = results
        .filter((r): r is PromiseFulfilledResult<any> => r.status === "fulfilled")
        .map((r) => r.value)

      if (successes.length > 0) {
        setCurrentUpload(successes[successes.length - 1])
        if (successes.length > 1) {
          alert(`Queued ${successes.length} uploads. Monitor their progress in Job Runs.`)
        }
      }

      const failures = results.filter((r) => r.status === "rejected")
      if (failures.length > 0 && successes.length === 0) {
        setUploadError(
          "All uploads failed. If backend logs show missing packages (like simplejwt) or migrations not run, fix backend first."
        )
      } else if (failures.length > 0) {
        setUploadError(`${failures.length} upload(s) failed. Check backend logs for details.`)
      }
      setQueuedFiles([])
      persistQueuedFiles([]).catch(() => {})
      setFileError(null)
      if (fileInputRef.current) {
        fileInputRef.current.value = ""
      }
    } catch (e: any) {
      setUploadError(
        "Upload failed. If backend logs show missing packages (like simplejwt) or migrations not run, fix backend first."
      )
    } finally {
      setSubmitting(false)
    }
  }

  async function downloadReport() {
    if (!currentUpload?.upload_id) return
    if (currentUpload.status !== "published") {
      setReportHint("Report only becomes available once status is published.")
      return
    }
    setReportHint(null)
    setReportLoading(true)
    try {
      await ensureReportDownloaded(currentUpload.upload_id)
    } catch (e: any) {
      setReportHint(typeof e?.message === "string" ? e.message : "Download failed. Try again shortly.")
    } finally {
      setReportLoading(false)
    }
  }

  return (
    <div className="page">
      <div className="pageHeader">
        <div>
          <div className="pageTitle">Uploads</div>
          <div className="pageSub">Upload a results file and watch it move through each processing stage in real time.</div>
        </div>
      </div>

      <div className="grid2">
        <Card>
          <div className="formHeader">
            <div className="formTitle">New upload</div>
            <div className="muted">CSV · Excel · PDF · &lt; 50 MB</div>
          </div>

          <div className="field">
            <label className="label">Department</label>
            <input className="input" value={department} onChange={(e) => setDepartment(e.target.value)} />
          </div>

          <div className="field">
            <label className="label">Notes (optional)</label>
            <textarea className="textarea" value={notes} onChange={(e) => setNotes(e.target.value)} />
          </div>

          <div className="field">
            <label className="label">Files</label>
            <div className="fileBox">
              <input
                ref={fileInputRef}
                className="fileInput"
                type="file"
                accept=".csv,.xlsx,.xls,.pdf"
                multiple
                disabled={filesAtLimit}
                onChange={handleFileSelect}
              />
              <div className="muted">
                {queuedFiles.length > 0
                  ? `Queued ${queuedFiles.length}/${MAX_UPLOAD_FILES} files${
                      filesAtLimit ? " (limit reached, remove one to add another)" : ""
                    }. They'll run in parallel once started.`
                  : `Choose up to ${MAX_UPLOAD_FILES} files. They'll run in parallel once you start the pipeline.`}
              </div>
            </div>
            {fileError ? <div className="errorLine">{fileError}</div> : null}
            {queuedFiles.length > 0 ? (
              <div className="fileList">
                <div className="fileListHead">
                  <div>File</div>
                  <div>Type</div>
                  <div>Size</div>
                  <div />
                </div>
                {queuedFiles.map((entry, idx) => (
                  <div className="fileListRow" key={entry.id}>
                    <div className="fileNameCell">
                      <div className="fileNameMain">{entry.file.name}</div>
                    </div>
                    <div className="fileMeta">{entry.file.type || "—"}</div>
                    <div className="fileMeta">{formatFileSize(entry.file.size)}</div>
                    <div className="fileAction">
                      <button className="ghostBtn ghostBtnSmall" type="button" onClick={() => removeFile(idx)}>
                        Remove
                      </button>
                    </div>
                  </div>
                ))}
              </div>
            ) : null}
          </div>

          {uploadError ? <div className="errorLine">{uploadError}</div> : null}

          <button className="primaryBtn" disabled={queuedFiles.length === 0 || submitting} onClick={startPipeline} type="button">
            {submitting
              ? "Starting..."
              : queuedFiles.length > 1
              ? `Start ${queuedFiles.length} pipelines`
              : "Start pipeline"}
          </button>
        </Card>

        <Card>
          <div className="formHeader">
            <div className="formTitle">Current upload</div>
            {currentUpload?.upload_id ? (
              <button className="ghostBtn" type="button" onClick={downloadReport} disabled={reportLoading}>
                {reportLoading ? "Fetching..." : "Download report"}
              </button>
            ) : null}
          </div>
          {reportHint ? <div className="errorLine">{reportHint}</div> : null}

          {!currentUpload ? (
            <div className="muted">After you upload a file, its status and report link will appear here.</div>
          ) : (
            <div className="currentUpload">
              <div className="row">
                <span className="muted">Upload ID</span>
                <span className="mono">{currentUpload.upload_id}</span>
              </div>
              <div className="row">
                <span className="muted">Status</span>
                  <span
                    className={cx(
                      "statusChip",
                      currentUpload.status === "failed" ? "bad" : currentUpload.status === "published" ? "ok" : "",
                    )}
                  >
                    {currentUpload.status}
                  </span>
              </div>
              <div className="row">
                <span className="muted">Department</span>
                <span>{currentUpload.department}</span>
              </div>
              <div className="row">
                <span className="muted">Filename</span>
                <span>{currentUpload.filename}</span>
              </div>
              <div className="row">
                <span className="muted">Received</span>
                <span>{fmtDate(currentUpload.received_at)}</span>
              </div>
                {currentRuns.length > 0 ? (
                  <div className="row">
                    <span className="muted">Pipeline</span>
                    <span className="mono">
                      {["standardize_results", "validate_results", "transform_gradebook", "generate_summary", "publish_results"].map(
                        (step) => {
                          const r = currentRuns.find((run) => run.job?.name === step || run.job_name === step)
                          const s = r?.status ?? "pending"
                          return `${step.replace("_", " ")}: ${s}`
                        },
                      ).join(" · ")}
                    </span>
                  </div>
                ) : null}
                {currentIncident ? (
                  <div className="row">
                    <span className="muted">Last incident</span>
                    <span title={String(currentIncident.error ?? "")}>
                      {currentIncident.state === "resolved" ? "Resolved" : "Open"} ·{" "}
                      {currentIncident.is_known ? "Known pattern" : "Unknown"} ·{" "}
                      {clampText(currentIncident.error ?? "", 80) || "—"}
                    </span>
                  </div>
                ) : null}
            </div>
          )}
        </Card>
      </div>
    </div>
  )
}

function JobRunsPage() {
  const [filter, setFilter] = useState("all")
  const [counts, setCounts] = useState({ total: 0, queued: 0, running: 0, success: 0, failed: 0, retrying: 0 })
  const [rows, setRows] = useState<any[]>([])
  const [err, setErr] = useState<string | null>(null)
  const [refreshTick, setRefreshTick] = useState(0)

  const [selectedRun, setSelectedRun] = useState<any | null>(null)
  const [selectedIncident, setSelectedIncident] = useState<any | null>(null)
  const [detailError, setDetailError] = useState<string | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const selectedSteps = selectedRun && Array.isArray(selectedRun.details?.steps) ? selectedRun.details.steps : []
  const selectedUploadId =
    selectedRun && (selectedRun.upload?.upload_id ?? selectedRun.upload_id ?? (typeof selectedRun.upload === "string" ? selectedRun.upload : null))

  useEffect(() => {
    let mounted = true
    async function load() {
      try {
        const res = await apiClient.jobRuns.list({ ordering: "-started_at" })
        const all = unwrapList<any>(res)
        const c = {
          total: all.length,
          queued: all.filter((r) => r.status === "queued").length,
          running: all.filter((r) => r.status === "running").length,
          success: all.filter((r) => r.status === "success").length,
          failed: all.filter((r) => r.status === "failed").length,
          retrying: all.filter((r) => r.status === "retrying").length,
        }
        const filtered = filter === "all" ? all : all.filter((r) => r.status === filter)
        if (!mounted) return
        setCounts(c)
        setRows(filtered.slice(0, 30))
        setErr(null)
      } catch {
        if (!mounted) return
        setCounts({ total: 0, queued: 0, running: 0, success: 0, failed: 0, retrying: 0 })
        setRows([])
        setErr("Failed to load job runs. Check /api/job-runs/")
      }
    }
    load()
    const t = setInterval(load, 4000)
    return () => {
      mounted = false
      clearInterval(t)
    }
  }, [filter, refreshTick])

  async function loadDetails(run: any) {
    setSelectedRun(run)
    setSelectedIncident(null)
    setDetailError(null)
    setDetailLoading(true)
    try {
      const runId = run.run_id ?? run.id
      if (!runId) return
      const res = await apiClient.incidents.list({ job_run: runId, ordering: "-created_at" })
      const list = unwrapList<any>(res)
      setSelectedIncident(list[0] ?? null)
    } catch {
      setDetailError("Failed to load incident details for this run.")
    } finally {
      setDetailLoading(false)
    }
  }

  function closeRunDetail() {
    setSelectedRun(null)
    setSelectedIncident(null)
    setDetailError(null)
    setDetailLoading(false)
  }

  return (
    <div className="page pageTall">
      <div className="pageHeader">
        <div>
          <div className="pageTitle">Job runs</div>
          <div className="pageSub">Live view of each stage for every upload.</div>
        </div>
        <button className="ghostBtn" type="button" onClick={() => setRefreshTick((x) => x + 1)}>
          Refresh
        </button>
      </div>

      <Card>
        <BadgeRow
          items={[
            { label: "Total", value: counts.total, tone: "muted" },
            { label: "Queued", value: counts.queued, tone: "blue" },
            { label: "Running", value: counts.running, tone: "blue" },
            { label: "Success", value: counts.success, tone: "green" },
            { label: "Failed", value: counts.failed, tone: "red" },
            { label: "Retrying", value: counts.retrying, tone: "amber" },
          ]}
        />
        <div className="divider" />
        <Segments
          value={filter}
          onChange={setFilter}
          options={[
            { value: "all", label: "All" },
            { value: "queued", label: "Queued" },
            { value: "running", label: "Running" },
            { value: "success", label: "Success" },
            { value: "failed", label: "Failed" },
            { value: "retrying", label: "Retrying" },
          ]}
        />
      </Card>

      <Card>
        <div className="tableHeader">
          <div className="muted">Latest job runs</div>
          <div className="muted">Auto-updating</div>
        </div>
        {err ? <div className="errorLine">{err}</div> : null}

        <div className="tableWrapper">
          <div className="table">
            <div className="tHead">
              <div>Run</div>
              <div>Job</div>
              <div>Status</div>
              <div>Upload ID</div>
              <div>Steps</div>
              <div>Started</div>
              <div>Finished</div>
              <div>Duration</div>
              <div>Exit</div>
            </div>

            {rows.length === 0 ? (
              <div className="muted" style={{ paddingTop: 10 }}>
                No job runs yet.
              </div>
            ) : (
              rows.map((r) => {
                const uploadId =
                  r.upload?.upload_id ??
                  r.upload_id ??
                  (typeof r.upload === "string" ? r.upload : null)
                return (
                  <div className="tRow" key={r.run_id ?? r.id ?? Math.random()} onClick={() => loadDetails(r)}>
                    <div className="mono">{String(r.run_id ?? "—").slice(0, 8)}</div>
                    <div>{r.job?.name ?? r.job_name ?? "—"}</div>
                    <div>
                      <span className={cx("statusChip", r.status === "failed" ? "bad" : r.status === "success" ? "ok" : "")}>
                        {r.status}
                      </span>
                    </div>
                    <div className="mono" title={uploadId ?? "Unknown"}>
                      {uploadId ? String(uploadId).slice(0, 8) : "—"}
                    </div>
                    <div title={summarizeSteps(r.details)}>{clampText(summarizeSteps(r.details), 60)}</div>
                    <div>{fmtDate(r.started_at)}</div>
                    <div>{fmtDate(r.finished_at)}</div>
                    <div className="mono">{r.duration_ms ? `${r.duration_ms} ms` : "—"}</div>
                    <div className="mono">{typeof r.exit_code === "number" ? r.exit_code : "—"}</div>
                  </div>
                )
              })
            )}
          </div>
        </div>
      </Card>

      {selectedRun ? (
        <DetailModal
          title="Run details"
          subtitle={
            <span className="muted">
              {selectedRun.job?.name ?? selectedRun.job_name ?? "—"} ·{" "}
              {selectedUploadId ? String(selectedUploadId).slice(0, 8) : "no upload"}
            </span>
          }
          onClose={closeRunDetail}
        >
          {detailError ? <div className="errorLine">{detailError}</div> : null}
          <div className="detailGrid">
            <div>
              <div className="muted">Run ID</div>
              <div className="mono">{String(selectedRun.run_id ?? "—")}</div>
            </div>
            <div>
              <div className="muted">Upload ID</div>
              <div className="mono">{selectedUploadId ? String(selectedUploadId) : "—"}</div>
            </div>
            <div>
              <div className="muted">Status</div>
              <span
                className={cx(
                  "statusChip",
                  selectedRun.status === "failed" ? "bad" : selectedRun.status === "success" ? "ok" : "",
                )}
              >
                {selectedRun.status}
              </span>
            </div>
            <div>
              <div className="muted">Started</div>
              <div>{fmtDate(selectedRun.started_at)}</div>
            </div>
            <div>
              <div className="muted">Finished</div>
              <div>{fmtDate(selectedRun.finished_at)}</div>
            </div>
            <div>
              <div className="muted">Duration</div>
              <div className="mono">{selectedRun.duration_ms ? `${selectedRun.duration_ms} ms` : "—"}</div>
            </div>
            <div>
              <div className="muted">Exit code</div>
              <div className="mono">{typeof selectedRun.exit_code === "number" ? selectedRun.exit_code : "—"}</div>
            </div>
          </div>

          <div className="detailSection">
            <div className="detailTitle">Pipeline steps</div>
            {selectedSteps.length === 0 ? (
              <div className="muted">No step data recorded for this run.</div>
            ) : (
              <div className="stepList">
                {selectedSteps.map((step: any) => (
                  <div className="stepRow" key={`${selectedRun.run_id}-${step.name}-${step.started_at}`}>
                    <div>
                      <div className="stepName">{formatStepName(step.name)}</div>
                      <div className="mono">
                        {fmtDate(step.started_at)} → {fmtDate(step.finished_at)}
                      </div>
                    </div>
                    <span
                      className={cx(
                        "statusChip",
                        step.status === "failed" ? "bad" : step.status === "success" ? "ok" : "",
                      )}
                    >
                      {step.status}
                    </span>
                  </div>
                ))}
              </div>
            )}
          </div>

          <div className="detailSection">
            <div className="detailTitle">Logs</div>
            <pre className="detailLogs">{String(selectedRun.logs ?? "No logs captured for this run.")}</pre>
          </div>

          <div className="detailSection">
            <div className="detailTitle">Incident</div>
            {detailLoading ? (
              <div className="muted">Loading related incident...</div>
            ) : !selectedIncident ? (
              <div className="muted">No incident recorded for this run yet.</div>
            ) : (
              <div className="incidentDetail">
                <div className="detailRow">
                  <span className="muted">Incident ID</span>
                  <span className="mono">
                    {selectedIncident.incident_id
                      ? String(selectedIncident.incident_id).slice(0, 8)
                      : String(selectedIncident.id ?? "—").slice(0, 8)}
                  </span>
                </div>
                <div className="detailRow">
                  <span className="muted">State</span>
                  <span
                    className={cx(
                      "statusChip",
                      selectedIncident.state === "open" ? "bad" : selectedIncident.state === "resolved" ? "ok" : "",
                    )}
                  >
                    {selectedIncident.state}
                  </span>
                </div>
                <div className="detailRow">
                  <span className="muted">Known error</span>
                  <span>
                    {selectedIncident.is_known
                      ? selectedIncident.matched_known_error_name || "Known pattern"
                      : "Unknown"}
                  </span>
                </div>
                <div className="detailRow">
                  <span className="muted">Error</span>
                  <span>{clampText(selectedIncident.error ?? "", 160) || "—"}</span>
                </div>
                <div className="detailRow">
                  <span className="muted">Root cause</span>
                  <span>{selectedIncident.root_cause || "—"}</span>
                </div>
                <div className="detailRow">
                  <span className="muted">Corrective action</span>
                  <span>{selectedIncident.corrective_action || "—"}</span>
                </div>
                {selectedIncident.suggested_fix ? (
                  <div className="detailRow">
                    <span className="muted">Suggested fix</span>
                    <span>{JSON.stringify(selectedIncident.suggested_fix)}</span>
                  </div>
                ) : null}
              </div>
            )}
          </div>
        </DetailModal>
      ) : null}
    </div>
  )
}

function IncidentsPage() {
  const [filter, setFilter] = useState("all")
  const [counts, setCounts] = useState({ total: 0, open: 0, in_progress: 0, resolved: 0 })
  const [rows, setRows] = useState<any[]>([])
  const [loadError, setLoadError] = useState<string | null>(null)
  const [refreshTick, setRefreshTick] = useState(0)

  const [showCreate, setShowCreate] = useState(false)
  const [newUploadId, setNewUploadId] = useState("")
  const [newErrorText, setNewErrorText] = useState("")
  const [newSeverity, setNewSeverity] = useState("medium")
  const [newCategory, setNewCategory] = useState("")
  const [newImpactSummary, setNewImpactSummary] = useState("")
  const [newAnalysisNotes, setNewAnalysisNotes] = useState("")
  const [actionBusy, setActionBusy] = useState<string | null>(null)
  const [inspectingIncident, setInspectingIncident] = useState<any | null>(null)
  const [incidentDetailError, setIncidentDetailError] = useState<string | null>(null)
  const [incidentDetailLoading, setIncidentDetailLoading] = useState(false)

  useEffect(() => {
    let mounted = true
    async function load() {
      try {
        const res = await apiClient.incidents.list({ ordering: "-created_at" })
        const all = unwrapList<any>(res)
        const c = {
          total: all.length,
          open: all.filter((r) => r.state === "open").length,
          in_progress: all.filter((r) => r.state === "in_progress").length,
          resolved: all.filter((r) => r.state === "resolved").length,
        }
        const filtered = filter === "all" ? all : all.filter((r) => r.state === filter)
        if (!mounted) return
        setCounts(c)
        setRows(filtered.slice(0, 30))
        setLoadError(null)
      } catch {
        if (!mounted) return
        setCounts({ total: 0, open: 0, in_progress: 0, resolved: 0 })
        setRows([])
        setLoadError("Failed to load incidents. Check /api/incidents/")
      }
    }
    load()
    const t = setInterval(load, 5000)
    return () => {
      mounted = false
      clearInterval(t)
    }
  }, [filter, refreshTick])

  async function createIncident() {
    if (!newUploadId.trim()) {
      alert("Upload ID is required to create an incident.")
      return
    }
    const payload: any = {
      upload: newUploadId.trim(),
      error: newErrorText.trim() || "Manual incident created from dashboard.",
      severity: newSeverity,
      category: newCategory.trim() || undefined,
      impact_summary: newImpactSummary.trim() || undefined,
      analysis_notes: newAnalysisNotes.trim() || undefined,
    }
    try {
      setActionBusy("create")
      await apiClient.incidents.create(payload)
      setShowCreate(false)
      setNewUploadId("")
      setNewErrorText("")
      setNewSeverity("medium")
      setNewCategory("")
      setNewImpactSummary("")
      setNewAnalysisNotes("")
      setRefreshTick((x) => x + 1)
    } catch {
      alert("Create incident failed. Ensure backend IncidentSerializer accepts upload and error.")
    } finally {
      setActionBusy(null)
    }
  }

  async function assignIncident(incidentId?: string) {
    if (!incidentId) {
      alert("Missing incident ID.")
      return
    }
    const assignee = prompt("Assign to (username):")
    if (!assignee) return
    try {
      setActionBusy(incidentId)
      await postJSON(`/api/incidents/${incidentId}/assign/`, { assignee })
      setRefreshTick((x) => x + 1)
    } catch {
      alert("Assign failed. Backend must have /api/incidents/<id>/assign/ action.")
    } finally {
      setActionBusy(null)
    }
  }

  async function resolveIncident(incidentId?: string) {
    if (!incidentId) {
      alert("Missing incident ID.")
      return
    }
    const root = prompt("Root cause (optional):") ?? ""
    const action = prompt("Corrective action (optional):") ?? ""
    const report = prompt("Resolution report (optional):") ?? ""
    try {
      setActionBusy(incidentId)
      await postJSON(`/api/incidents/${incidentId}/resolve/`, {
        root_cause: root,
        corrective_action: action,
        resolution_report: report,
      })
      setRefreshTick((x) => x + 1)
    } catch {
      alert("Resolve failed. Backend must have /api/incidents/<id>/resolve/ action.")
    } finally {
      setActionBusy(null)
    }
  }

  async function analyzeIncident(incidentId?: string) {
    if (!incidentId) {
      alert("Missing incident ID.")
      return
    }
    const severity = prompt("Severity (low/medium/high/critical):", "medium") || "medium"
    const impact = prompt("Impact summary (optional):") || ""
    const analysis = prompt("Analysis notes:", "") || ""
    try {
      setActionBusy(incidentId)
      await postJSON(`/api/incidents/${incidentId}/analyze/`, {
        severity: severity.toLowerCase(),
        impact_summary: impact,
        analysis_notes: analysis,
      })
      setRefreshTick((x) => x + 1)
    } catch {
      alert("Failed to update analysis. Please try again.")
    } finally {
      setActionBusy(null)
    }
  }

  async function retryIncident(incidentId?: string) {
    if (!incidentId) {
      alert("Missing incident ID.")
      return
    }
    const notes = prompt("Retry notes (optional):") || ""
    try {
      setActionBusy(incidentId)
      await postJSON(`/api/incidents/${incidentId}/retry/`, { notes })
      setRefreshTick((x) => x + 1)
    } catch {
      alert("Retry failed. Check backend logs for details.")
    } finally {
      setActionBusy(null)
    }
  }

  async function archiveIncident(incidentId?: string) {
    if (!incidentId) {
      alert("Missing incident ID.")
      return
    }
    const confirmArchive = confirm("Archive this incident? This will mark it resolved and hide it from the active list.")
    if (!confirmArchive) return
    try {
      setActionBusy(incidentId)
      await postJSON(`/api/incidents/${incidentId}/archive/`, {})
      setRefreshTick((x) => x + 1)
    } catch {
      alert("Archive failed. Please try again.")
    } finally {
      setActionBusy(null)
    }
  }

  async function openIncidentDetail(incident: any) {
    setInspectingIncident(incident)
    setIncidentDetailError(null)
    const id = incident?.incident_id ?? incident?.id
    if (!id) {
      setIncidentDetailLoading(false)
      return
    }
    setIncidentDetailLoading(true)
    try {
      const detail = await apiClient.incidents.get(String(id))
      setInspectingIncident(detail)
    } catch {
      setIncidentDetailError("Failed to load latest incident details.")
    } finally {
      setIncidentDetailLoading(false)
    }
  }

  function closeIncidentDetail() {
    setInspectingIncident(null)
    setIncidentDetailError(null)
    setIncidentDetailLoading(false)
  }

  return (
    <div className="page pageTall">
      <div className="pageHeader">
        <div>
          <div className="pageTitle">Incidents</div>
          <div className="pageSub">Failures create incidents. Track RCA and resolution.</div>
        </div>
        <div style={{ display: "flex", gap: 10 }}>
          <button className="ghostBtn" type="button" onClick={() => setRefreshTick((x) => x + 1)}>
            Refresh
          </button>
          <button className="primaryBtnSmall" type="button" onClick={() => setShowCreate(true)}>
            + Create incident
          </button>
        </div>
      </div>

      <Card>
        <BadgeRow
          items={[
            { label: "Total", value: counts.total, tone: "muted" },
            { label: "Open", value: counts.open, tone: "red" },
            { label: "In progress", value: counts.in_progress, tone: "amber" },
            { label: "Resolved", value: counts.resolved, tone: "green" },
          ]}
        />
        <div className="divider" />
        <Segments
          value={filter}
          onChange={setFilter}
          options={[
            { value: "all", label: "All" },
            { value: "open", label: "Open" },
            { value: "in_progress", label: "In progress" },
            { value: "resolved", label: "Resolved" },
          ]}
        />
      </Card>

      <Card>
        <div className="tableHeader">
          <div className="muted">Latest incidents</div>
          <div className="muted">Auto-updating</div>
        </div>
        {loadError ? <div className="errorLine">{loadError}</div> : null}

        <div className="tableWrapper">
          <div className="table">
            <div className="tHead">
              <div>Incident</div>
              <div>State</div>
              <div>Severity</div>
              <div>Created</div>
              <div>Upload</div>
              <div>Error</div>
              <div>Assignee</div>
              <div>Known?</div>
              <div>Actions</div>
            </div>

            {rows.length === 0 ? (
              <div className="muted" style={{ paddingTop: 10 }}>
                No incidents yet.
              </div>
            ) : (
              rows.map((r) => {
                const incidentKey = r.incident_id ?? r.id ?? Math.random()
                const incidentId = r.incident_id ?? r.id
                return (
                  <div className="tRow" key={incidentKey} onClick={() => openIncidentDetail(r)}>
                    <div className="mono">{String(r.incident_id ?? "—").slice(0, 8)}</div>
                    <div>
                      <span className={cx("statusChip", r.state === "open" ? "bad" : r.state === "resolved" ? "ok" : "")}>
                        {r.state}
                      </span>
                    </div>
                    <div>
                      <span className={cx("severityPill", r.severity || "medium")}>{r.severity ?? "medium"}</span>
                    </div>
                    <div>{fmtDate(r.created_at)}</div>
                    <div className="mono">{r.upload?.upload_id ? String(r.upload.upload_id).slice(0, 8) : "—"}</div>
                    <div title={String(r.error ?? "")}>{clampText(r.error, 60) || "—"}</div>
                <div>{r.assignee ?? "—"}</div>
                <div>{r.is_known ? "Known" : "Unknown"}</div>
                <div style={{ display: "flex", gap: 8 }}>
                  <button
                    className="ghostBtn"
                        type="button"
                        disabled={actionBusy === incidentId}
                        onClick={(e) => {
                          e.stopPropagation()
                          assignIncident(incidentId ? String(incidentId) : undefined)
                        }}
                      >
                        Assign
                      </button>
                      <button
                        className="ghostBtn"
                        type="button"
                        disabled={actionBusy === incidentId || r.state === "resolved"}
                        onClick={(e) => {
                          e.stopPropagation()
                          resolveIncident(incidentId ? String(incidentId) : undefined)
                        }}
                      >
                        Resolve
                      </button>
                    </div>
                  </div>
                )
              })
            )}
          </div>
        </div>
      </Card>

      {inspectingIncident ? (
        <DetailModal
          title="Incident details"
          subtitle={
            inspectingIncident.upload?.upload_id ? (
              <span className="muted">
                Upload {String(inspectingIncident.upload.upload_id).slice(0, 8)}
              </span>
            ) : (
              <span className="muted">{fmtDate(inspectingIncident.created_at)}</span>
            )
          }
          onClose={closeIncidentDetail}
        >
          <div className="detailActionRow">
            <button
              className="ghostBtn"
              type="button"
              onClick={() => analyzeIncident(String(inspectingIncident.incident_id))}
            >
              Add analysis
            </button>
            <button
              className="ghostBtn"
              type="button"
              onClick={() => retryIncident(String(inspectingIncident.incident_id))}
            >
              Retry pipeline
            </button>
            <button
              className="ghostBtn"
              type="button"
              onClick={() => archiveIncident(String(inspectingIncident.incident_id))}
            >
              Archive
            </button>
          </div>
          {incidentDetailError ? <div className="errorLine">{incidentDetailError}</div> : null}
          {incidentDetailLoading ? <div className="muted">Refreshing incident data...</div> : null}
          <div className="detailGrid">
            <div>
              <div className="muted">Incident ID</div>
              <div className="mono">
                {String(inspectingIncident.incident_id ?? inspectingIncident.id ?? "—").slice(0, 8)}
              </div>
            </div>
            <div>
              <div className="muted">State</div>
              <span
                className={cx(
                  "statusChip",
                  inspectingIncident.state === "open" ? "bad" : inspectingIncident.state === "resolved" ? "ok" : "",
                )}
              >
                {inspectingIncident.state}
              </span>
            </div>
            <div>
              <div className="muted">Severity</div>
              <span className={cx("severityPill", inspectingIncident.severity || "medium")}>
                {inspectingIncident.severity ?? "medium"}
              </span>
            </div>
            <div>
              <div className="muted">Assignee</div>
              <div>{inspectingIncident.assignee ?? "Unassigned"}</div>
            </div>
            <div>
              <div className="muted">Upload</div>
              <div className="mono">
                {inspectingIncident.upload?.upload_id
                  ? String(inspectingIncident.upload.upload_id).slice(0, 8)
                  : "—"}
              </div>
            </div>
            <div>
              <div className="muted">Job run</div>
              <div className="mono">
                {inspectingIncident.job_run?.run_id
                  ? String(inspectingIncident.job_run.run_id).slice(0, 8)
                  : inspectingIncident.job_run_id
                  ? String(inspectingIncident.job_run_id).slice(0, 8)
                  : "—"}
              </div>
            </div>
            <div>
              <div className="muted">Known pattern</div>
              <div>
                {inspectingIncident.is_known
                  ? inspectingIncident.matched_known_error_name || "Known pattern"
                  : "Unknown"}
              </div>
            </div>
            <div>
              <div className="muted">Created</div>
              <div>{fmtDate(inspectingIncident.created_at)}</div>
            </div>
            <div>
              <div className="muted">Resolved</div>
              <div>{fmtDate(inspectingIncident.resolved_at)}</div>
            </div>
            <div>
              <div className="muted">Retries</div>
              <div>
                {inspectingIncident.auto_retry_count ?? 0}/{inspectingIncident.max_auto_retries ?? 0}
              </div>
            </div>
            <div>
              <div className="muted">Detection</div>
              <div>{inspectingIncident.detection_source || "engine"}</div>
            </div>
            <div>
              <div className="muted">Archived</div>
              <div>{inspectingIncident.archived_at ? fmtDate(inspectingIncident.archived_at) : "Not archived"}</div>
            </div>
          </div>

          <div className="detailSection">
            <div className="detailTitle">Error message</div>
            <div className="detailTextBlock">{inspectingIncident.error || "No error captured."}</div>
          </div>

          <div className="detailSection">
            <div className="detailTitle">Impact summary</div>
            <div className="detailTextBlock">{inspectingIncident.impact_summary || "Not recorded yet."}</div>
          </div>

          <div className="detailSection">
            <div className="detailTitle">Root cause</div>
            <div className="detailTextBlock">{inspectingIncident.root_cause || "Not recorded yet."}</div>
          </div>

          <div className="detailSection">
            <div className="detailTitle">Corrective action</div>
            <div className="detailTextBlock">{inspectingIncident.corrective_action || "Not recorded yet."}</div>
          </div>

          <div className="detailSection">
            <div className="detailTitle">Analysis notes</div>
            <div className="detailTextBlock">{inspectingIncident.analysis_notes || "Not recorded yet."}</div>
          </div>

  ***

          {inspectingIncident.suggested_fix ? (
            <div className="detailSection">
              <div className="detailTitle">Suggested fix</div>
              <div className="detailTextBlock">
                {typeof inspectingIncident.suggested_fix === "string"
                  ? inspectingIncident.suggested_fix
                  : JSON.stringify(inspectingIncident.suggested_fix, null, 2)}
              </div>
            </div>
          ) : null}

          <div className="detailSection">
            <div className="detailTitle">Resolution report</div>
            <div className="detailTextBlock">
              {inspectingIncident.resolution_report || "No resolution summary captured yet."}
            </div>
          </div>

          <div className="detailSection">
            <div className="detailTitle">Timeline</div>
            {Array.isArray(inspectingIncident.timeline) && inspectingIncident.timeline.length > 0 ? (
              <div className="timelineList">
                {inspectingIncident.timeline.map((ev: any, idx: number) => (
                  <div className="timelineItem" key={`${ev.timestamp}-${idx}`}>
                    <div className="timelineTime">{fmtDate(ev.timestamp)}</div>
                    <div>
                      <div className="timelineEvent">{ev.event}</div>
                      <div className="muted">{ev.actor || "engine"}</div>
                      {ev.notes ? <div className="timelineNotes">{ev.notes}</div> : null}
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <div className="muted">Timeline will populate as the incident progresses.</div>
            )}
          </div>
        </DetailModal>
      ) : null}

      {showCreate ? (
        <div className="modalBackdrop" onClick={() => setShowCreate(false)}>
          <div className="modal" onClick={(e) => e.stopPropagation()}>
            <div className="modalTitle">Create incident</div>

            <div className="field">
              <label className="label">Upload ID</label>
              <input
                className="input"
                value={newUploadId}
                onChange={(e) => setNewUploadId(e.target.value)}
                placeholder="UUID of the related upload"
              />
            </div>

            <div className="field">
              <label className="label">Error / description</label>
              <textarea
                className="textarea"
                value={newErrorText}
                onChange={(e) => setNewErrorText(e.target.value)}
              />
            </div>

            <div className="field">
              <label className="label">Severity</label>
              <select className="input" value={newSeverity} onChange={(e) => setNewSeverity(e.target.value)}>
                <option value="low">Low</option>
                <option value="medium">Medium</option>
                <option value="high">High</option>
                <option value="critical">Critical</option>
              </select>
            </div>

            <div className="field">
              <label className="label">Category (optional)</label>
              <input
                className="input"
                value={newCategory}
                onChange={(e) => setNewCategory(e.target.value)}
                placeholder="e.g. validation, ingest"
              />
            </div>

            <div className="field">
              <label className="label">Impact summary (optional)</label>
              <textarea
                className="textarea"
                value={newImpactSummary}
                onChange={(e) => setNewImpactSummary(e.target.value)}
                placeholder="Describe the blast radius"
              />
            </div>

            <div className="field">
              <label className="label">Initial analysis (optional)</label>
              <textarea
                className="textarea"
                value={newAnalysisNotes}
                onChange={(e) => setNewAnalysisNotes(e.target.value)}
                placeholder="Any immediate clues or leads?"
              />
            </div>

            <div style={{ display: "flex", gap: 10, justifyContent: "flex-end" }}>
              <button className="ghostBtn" type="button" onClick={() => setShowCreate(false)}>
                Cancel
              </button>
              <button
                className="primaryBtnSmall"
                type="button"
                disabled={actionBusy === "create" || !newUploadId.trim()}
                onClick={createIncident}
              >
                {actionBusy === "create" ? "Creating..." : "Create"}
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  )
}

function TicketsPage() {
  const [filter, setFilter] = useState("all")
  const [rows, setRows] = useState<any[]>([])
  const [counts, setCounts] = useState({ total: 0, open: 0, in_progress: 0, resolved: 0, closed: 0 })
  const [err, setErr] = useState<string | null>(null)
  const [refreshTick, setRefreshTick] = useState(0)

  const [showCreate, setShowCreate] = useState(false)
  const [newTitle, setNewTitle] = useState("")
  const [newDesc, setNewDesc] = useState("")
  const [newIncidentId, setNewIncidentId] = useState("")

  const [actionBusy, setActionBusy] = useState<string | null>(null)

  async function loadTickets() {
    const res = await apiClient.tickets.list({ ordering: "-created_at" })
    const all = unwrapList<any>(res)

    const c = {
      total: all.length,
      open: all.filter((t) => t.status === "open").length,
      in_progress: all.filter((t) => t.status === "in_progress").length,
      resolved: all.filter((t) => t.status === "resolved").length,
      closed: all.filter((t) => t.status === "closed").length,
    }

    const filtered = filter === "all" ? all : all.filter((t) => t.status === filter)
    setCounts(c)
    setRows(filtered.slice(0, 40))
  }

  useEffect(() => {
    let mounted = true
    async function run() {
      try {
        await loadTickets()
        if (!mounted) return
        setErr(null)
      } catch {
        if (!mounted) return
        setRows([])
        setCounts({ total: 0, open: 0, in_progress: 0, resolved: 0, closed: 0 })
        setErr("Failed to load tickets. Check /api/tickets/")
      }
    }
    run()
    const t = setInterval(run, 5000)
    return () => {
      mounted = false
      clearInterval(t)
    }
  }, [filter, refreshTick])

  async function createTicket() {
    try {
      setActionBusy("create")
      const payload: any = {
        title: newTitle.trim(),
        description: newDesc.trim(),
      }
      if (newIncidentId.trim()) payload.incident = newIncidentId.trim()
      await apiClient.tickets.create(payload)
      setShowCreate(false)
      setNewTitle("")
      setNewDesc("")
      setNewIncidentId("")
      setRefreshTick((x) => x + 1)
    } catch (e) {
      alert("Create ticket failed. Make sure backend TicketSerializer accepts incident field as UUID or nested relation.")
    } finally {
      setActionBusy(null)
    }
  }

  async function assignTicket(ticketId: string) {
    const assignee = prompt("Assign to (username):")
    if (!assignee) return
    try {
      setActionBusy(ticketId)
      await postJSON(`/api/tickets/${ticketId}/assign/`, { assignee })
      setRefreshTick((x) => x + 1)
    } catch {
      alert("Assign failed. Backend must have /api/tickets/<id>/assign/ action.")
    } finally {
      setActionBusy(null)
    }
  }

  async function resolveTicket(ticketId: string) {
    const notes = prompt("Resolution notes (optional):") ?? ""
    try {
      setActionBusy(ticketId)
      await postJSON(`/api/tickets/${ticketId}/resolve/`, { resolution_type: "manual", resolution_notes: notes })
      setRefreshTick((x) => x + 1)
    } catch {
      alert("Resolve failed. Backend must have /api/tickets/<id>/resolve/ action.")
    } finally {
      setActionBusy(null)
    }
  }

  async function analyzeLinkedIncident(incidentId?: string) {
    if (!incidentId) {
      alert("Ticket is not linked to an incident.")
      return
    }
    const severity = prompt("Severity (low/medium/high/critical):", "medium") || "medium"
    const impact = prompt("Impact summary (optional):") || ""
    const analysis = prompt("Analysis notes:", "") || ""
    try {
      setActionBusy(incidentId)
      await postJSON(`/api/incidents/${incidentId}/analyze/`, {
        severity: severity.toLowerCase(),
        impact_summary: impact,
        analysis_notes: analysis,
      })
      setRefreshTick((x) => x + 1)
    } catch {
      alert("Failed to update analysis. Please try again.")
    } finally {
      setActionBusy(null)
    }
  }

  async function retryLinkedIncident(incidentId?: string) {
    if (!incidentId) {
      alert("Ticket is not linked to an incident.")
      return
    }
    const notes = prompt("Retry notes (optional):") || ""
    try {
      setActionBusy(incidentId)
      await postJSON(`/api/incidents/${incidentId}/retry/`, { notes })
      setRefreshTick((x) => x + 1)
    } catch {
      alert("Retry failed. Check backend logs for details.")
    } finally {
      setActionBusy(null)
    }
  }

  return (
    <div className="page">
      <div className="pageHeader">
        <div>
          <div className="pageTitle">Tickets</div>
          <div className="pageSub">System and manual tickets tied to incidents. Assign, track and resolve issues from here.</div>
        </div>

        <div style={{ display: "flex", gap: 10 }}>
          <button className="ghostBtn" type="button" onClick={() => setRefreshTick((x) => x + 1)}>
            Refresh
          </button>
          <button className="primaryBtnSmall" type="button" onClick={() => setShowCreate(true)}>
            + Create Ticket
          </button>
        </div>
      </div>

      <Card>
        <BadgeRow
          items={[
            { label: "Total", value: counts.total, tone: "muted" },
            { label: "Open", value: counts.open, tone: "red" },
            { label: "In Progress", value: counts.in_progress, tone: "amber" },
            { label: "Resolved", value: counts.resolved, tone: "green" },
            { label: "Closed", value: counts.closed, tone: "muted" },
          ]}
        />
        <div className="divider" />
        <Segments
          value={filter}
          onChange={setFilter}
          options={[
            { value: "all", label: "All" },
            { value: "open", label: "Open" },
            { value: "in_progress", label: "In progress" },
            { value: "resolved", label: "Resolved" },
            { value: "closed", label: "Closed" },
          ]}
        />
        {err ? <div className="errorLine">{err}</div> : null}
      </Card>

      <Card>
        <div className="tableHeader">
          <div className="muted">Latest tickets</div>
          <div className="muted">Auto-updating</div>
        </div>

        <div className="table">
          <div className="tHead">
            <div>Ticket</div>
            <div>Status</div>
            <div>Source</div>
            <div>Assignee</div>
            <div>Incident</div>
            <div>Title</div>
            <div>Actions</div>
          </div>

          {rows.length === 0 ? (
            <div className="muted" style={{ paddingTop: 10 }}>
              No tickets yet.
            </div>
          ) : (
            rows.map((t) => {
              const ticketIncidentId = t.incident ?? t.incident_id
              const incidentRef = ticketIncidentId ? String(ticketIncidentId) : undefined
              return (
                <div className="tRow" key={t.ticket_id ?? Math.random()}>
                  <div className="mono">{String(t.ticket_id ?? "—").slice(0, 8)}</div>
                  <div>
                    <span className={cx("statusChip", t.status === "open" ? "bad" : t.status === "resolved" ? "ok" : "")}>
                      {t.status}
                    </span>
                  </div>
                  <div>{t.source ?? "—"}</div>
                  <div>{t.assignee ?? "—"}</div>
                  <div className="mono">
                    {t.incident ? String(t.incident).slice(0, 8) : t.incident_id ? String(t.incident_id).slice(0, 8) : "—"}
                  </div>
                  <div title={String(t.title ?? "")}>{clampText(t.title ?? t.description ?? "—", 50)}</div>
                  <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                    <button
                      className="ghostBtn"
                      type="button"
                      disabled={actionBusy === t.ticket_id}
                      onClick={() => assignTicket(String(t.ticket_id))}
                    >
                      Assign
                    </button>
                    <button
                      className="ghostBtn"
                      type="button"
                      disabled={actionBusy === t.ticket_id || t.status === "resolved"}
                      onClick={() => resolveTicket(String(t.ticket_id))}
                    >
                      Resolve
                    </button>
                    <button
                      className="ghostBtn"
                      type="button"
                    disabled={actionBusy === ticketIncidentId || !incidentRef}
                    onClick={(e) => {
                      e.stopPropagation()
                      if (!incidentRef) {
                        alert("Ticket is not linked to an incident.")
                        return
                      }
                      analyzeLinkedIncident(incidentRef)
                    }}
                  >
                    Analyze
                    </button>
                    <button
                      className="ghostBtn"
                      type="button"
                    disabled={actionBusy === ticketIncidentId || !incidentRef}
                    onClick={(e) => {
                      e.stopPropagation()
                      if (!incidentRef) {
                        alert("Ticket is not linked to an incident.")
                        return
                      }
                      retryLinkedIncident(incidentRef)
                    }}
                  >
                    Retry
                    </button>
                  </div>
                </div>
              )
            })
          )}
        </div>
      </Card>

      {showCreate ? (
        <div className="modalBackdrop" onClick={() => setShowCreate(false)}>
          <div className="modal" onClick={(e) => e.stopPropagation()}>
            <div className="modalTitle">Create ticket</div>

            <div className="field">
              <label className="label">Incident ID (optional)</label>
              <input className="input" value={newIncidentId} onChange={(e) => setNewIncidentId(e.target.value)} placeholder="UUID" />
            </div>

            <div className="field">
              <label className="label">Title</label>
              <input className="input" value={newTitle} onChange={(e) => setNewTitle(e.target.value)} />
            </div>

            <div className="field">
              <label className="label">Description</label>
              <textarea className="textarea" value={newDesc} onChange={(e) => setNewDesc(e.target.value)} />
            </div>

            <div style={{ display: "flex", gap: 10, justifyContent: "flex-end" }}>
              <button className="ghostBtn" type="button" onClick={() => setShowCreate(false)}>
                Cancel
              </button>
              <button className="primaryBtnSmall" type="button" disabled={actionBusy === "create" || !newTitle.trim()} onClick={createTicket}>
                {actionBusy === "create" ? "Creating..." : "Create"}
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  )
}

function ReportsPage() {
  const [jobRunId, setJobRunId] = useState("")
  const [hint, setHint] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)

  async function download() {
    const id = jobRunId.trim()
    if (!id) {
      setHint("Enter a job_run_id (UUID) first.")
      return
    }
    setHint(null)
    setLoading(true)
    try {
      const run = await apiClient.jobRuns.get(id)
      const uploadId = run?.upload?.upload_id || run?.upload_id || run?.upload
      if (!uploadId) {
        setHint("That run has no upload associated with it yet.")
        return
      }
      if (run.status !== "success") {
        if (run.status === "failed") {
          setHint("Job run failed. Check Incidents for root cause before downloading.")
          return
        }
        setHint("Job run still in progress. Wait until status is success.")
        return
      }
      await ensureReportDownloaded(uploadId)
      setHint(null)
    } catch (e: any) {
      if (typeof e?.message === "string") {
        setHint(e.message)
      } else if (e?.response?.status === 404) {
        setHint("Job run not found. Use a valid run_id from the Job Runs tab.")
      } else {
        setHint("Failed to download report. Ensure the job run is complete and try again.")
      }
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="page">
      <div className="pageHeader">
        <div>
          <div className="pageTitle">Reports</div>
          <div className="pageSub">Download the CSV summary generated for an upload.</div>
        </div>
      </div>

      <Card>
        <div className="sectionTitle">Download a report</div>
        <div className="muted" style={{ marginTop: 8 }}>
          Paste a <span className="mono">job_run_id</span> and FlowBoard will find the related upload and fetch the CSV.
        </div>

        <div className="field" style={{ marginTop: 12 }}>
          <label className="label">Job Run ID</label>
          <input
            className="input"
            value={jobRunId}
            onChange={(e) => setJobRunId(e.target.value)}
            placeholder="e.g. f4a6c63e-9d1b..."
          />
        </div>

        {hint ? <div className="errorLine">{hint}</div> : null}

        <button className="primaryBtnSmall" type="button" onClick={download} disabled={loading}>
          {loading ? "Fetching..." : "Download report"}
        </button>
      </Card>

      <Card>
        <div className="sectionTitle">Testing the pipeline</div>
        <ol className="steps">
          <li>Go to Uploads.</li>
          <li>Upload a CSV/Excel/PDF.</li>
          <li>Wait until status becomes “published”.</li>
          <li>Use “Download report”.</li>
        </ol>
      </Card>
    </div>
  )
}

function JobsPage() {
  const [jobs, setJobs] = useState<any[]>([])
  const [loading, setLoading] = useState(false)
  const [jobError, setJobError] = useState<string | null>(null)
  const [newJobName, setNewJobName] = useState("")
  const [newJobCallable, setNewJobCallable] = useState("")
  const [newJobCron, setNewJobCron] = useState("")
  const [newJobArgs, setNewJobArgs] = useState("")
  const [scheduleDrafts, setScheduleDrafts] = useState<Record<string, string>>({})
  const [savingJobId, setSavingJobId] = useState<string | null>(null)

  const syncDrafts = (list: any[]) => {
    const next: Record<string, string> = {}
    list.forEach((job) => {
      next[String(job.id)] = job.schedule_cron || ""
    })
    setScheduleDrafts(next)
  }

  async function loadJobs() {
    setLoading(true)
    try {
      const list = await apiClient.jobs.list()
      setJobs(list)
      syncDrafts(list)
      setJobError(null)
    } catch {
      setJobError("Failed to load jobs. Ensure /api/jobs/ is reachable.")
      setJobs([])
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    loadJobs()
  }, [])

  async function createJob() {
    if (!newJobName.trim() || !newJobCallable.trim()) {
      setJobError("Name and callable are required.")
      return
    }
    let args: any[] = []
    if (newJobArgs.trim()) {
      try {
        const parsed = JSON.parse(newJobArgs)
        args = Array.isArray(parsed) ? parsed : [parsed]
      } catch {
        setJobError("Arguments must be valid JSON (e.g. [\"foo\", 123]).")
        return
      }
    }
    const payload: any = {
      name: newJobName.trim(),
      job_type: "python",
      config: {
        callable: newJobCallable.trim(),
        args,
        kwargs: {},
      },
      schedule_cron: newJobCron.trim() || null,
    }
    try {
      setSavingJobId("create")
      await apiClient.jobs.create(payload)
      setNewJobName("")
      setNewJobCallable("")
      setNewJobCron("")
      setNewJobArgs("")
      setJobError(null)
      loadJobs()
    } catch (e: any) {
      setJobError(e?.response?.data?.error || "Create job failed. Check backend logs.")
    } finally {
      setSavingJobId(null)
    }
  }

  async function saveSchedule(jobId: string) {
    const cron = scheduleDrafts[jobId] ?? ""
    try {
      setSavingJobId(jobId)
      await apiClient.jobs.update(jobId, { schedule_cron: cron.trim() || null })
      setJobError(null)
      loadJobs()
    } catch {
      setJobError("Failed to update schedule. Ensure cron expression is valid.")
    } finally {
      setSavingJobId(null)
    }
  }

  async function triggerJob(jobId: string) {
    try {
      setSavingJobId(jobId)
      await apiClient.jobs.trigger(jobId, {})
      setJobError(null)
      alert("Job queued successfully.")
    } catch {
      setJobError("Failed to trigger job.")
    } finally {
      setSavingJobId(null)
    }
  }

  return (
    <div className="page">
      <div className="pageHeader">
        <div>
          <div className="pageTitle">Jobs</div>
          <div className="pageSub">Schedule recurring tasks with cron expressions and run them on demand.</div>
        </div>
      </div>

      <div className="grid2">
        <Card>
          <div className="formHeader">
            <div className="formTitle">New scheduled job</div>
          </div>
          <div className="field">
            <label className="label">Job name</label>
            <input className="input" value={newJobName} onChange={(e) => setNewJobName(e.target.value)} placeholder="e.g. nightly_pipeline" />
          </div>
          <div className="field">
            <label className="label">Python callable</label>
            <input
              className="input"
              value={newJobCallable}
              onChange={(e) => setNewJobCallable(e.target.value)}
              placeholder="module.path:function_name"
            />
          </div>
          <div className="field">
            <label className="label">Arguments (JSON array, optional)</label>
            <textarea
              className="textarea"
              value={newJobArgs}
              onChange={(e) => setNewJobArgs(e.target.value)}
              placeholder='e.g. ["arg1", 42]'
            />
          </div>
          <div className="field">
            <label className="label">Cron expression (optional)</label>
            <input className="input" value={newJobCron} onChange={(e) => setNewJobCron(e.target.value)} placeholder="*/5 * * * *" />
          </div>
          <button className="primaryBtn" type="button" disabled={savingJobId === "create"} onClick={createJob}>
            {savingJobId === "create" ? "Creating..." : "Create job"}
          </button>
        </Card>

        <Card>
          <div className="formHeader">
            <div className="formTitle">Configured jobs</div>
            <button className="ghostBtn" type="button" onClick={loadJobs} disabled={loading}>
              {loading ? "Loading..." : "Refresh"}
            </button>
          </div>
          {jobError ? <div className="errorLine">{jobError}</div> : null}
          {jobs.length === 0 ? (
            <div className="muted">No jobs configured yet.</div>
          ) : (
            <div className="jobList">
              {jobs.map((job: any) => {
                const jobId = String(job.id)
                return (
                  <div className="jobRow" key={jobId}>
                    <div>
                      <div className="muted">Name</div>
                      <div className="mono">{job.name}</div>
                      <div className="muted" style={{ fontSize: 12 }}>{job.job_type}</div>
                    </div>
                    <div>
                      <div className="muted">Callable</div>
                      <div className="mono">{job.config?.callable || "—"}</div>
                    </div>
                    <div>
                      <div className="muted">Schedule</div>
                      <input
                        className="input"
                        value={scheduleDrafts[jobId] ?? ""}
                        onChange={(e) => setScheduleDrafts((draft) => ({ ...draft, [jobId]: e.target.value }))}
                        placeholder="Cron expression"
                      />
                    </div>
                    <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                      <button
                        className="ghostBtn"
                        type="button"
                        disabled={savingJobId === jobId}
                        onClick={() => saveSchedule(jobId)}
                      >
                        Save schedule
                      </button>
                      <button
                        className="ghostBtn"
                        type="button"
                        disabled={savingJobId === jobId}
                        onClick={() => triggerJob(jobId)}
                      >
                        Run now
                      </button>
                    </div>
                  </div>
                )
              })}
            </div>
          )}
        </Card>
      </div>
    </div>
  )
}

/** ---------------- APP SHELL ---------------- */

export default function App() {
  const [active, setActive] = useState<NavKey>(() => {
    if (typeof window === "undefined") return "dashboard"
    const saved = localStorage.getItem(LAST_NAV_KEY)
    return NAV.some((item) => item.key === saved) ? (saved as NavKey) : "dashboard"
  })
  const [systemOnline, setSystemOnline] = useState(true)

  useEffect(() => {
    if (typeof window === "undefined") return
    try {
      localStorage.setItem(LAST_NAV_KEY, active)
    } catch {
      // ignore persistence failures
    }
  }, [active])

  const Page = useMemo(() => {
    switch (active) {
      case "dashboard":
        return <DashboardPage />
      case "uploads":
        return <UploadsPage />
      case "jobRuns":
        return <JobRunsPage />
      case "incidents":
        return <IncidentsPage />
      case "reports":
        return <ReportsPage />
      case "jobs":
        return <JobsPage />
      default:
        return <DashboardPage />
    }
  }, [active])

  return (
    <div className="shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brandMark">FB</div>
          <div>
            <div className="brandName">FlowBoard.io</div>
            <div className="brandSub">Batch Processing</div>
          </div>
        </div>

        <nav className="nav">
          {NAV.map((item) => (
            <button
              key={item.key}
              className={cx("navItem", active === item.key && "navItemActive")}
              onClick={() => setActive(item.key)}
              type="button"
            >
              <span className={cx("navIcon", active === item.key && "navIconActive")}>{item.icon}</span>
              <span className="navLabel">{item.label}</span>
              <Pill tone={item.pill.tone}>{item.pill.text}</Pill>
            </button>
          ))}
        </nav>

        <div className="runtime">
          <div className="runtimeTitle">Runtime Stack · v1</div>
          <div className="runtimeText">Backend · Django · RQ · Postgres · Redis</div>
          <div className="runtimeText">Metrics · Prometheus · Grafana</div>
        </div>
      </aside>

      <main className="main">
        <div className="topbar">
          <div className="topbarLeft" />
          <div className="topbarRight">
            <div className="status">
              <span className={cx("statusDot", systemOnline ? "ok" : "bad")} />
              <span className="statusText">{systemOnline ? "System Online" : "System Offline"}</span>
            </div>
            <label className="switch">
              <input type="checkbox" checked={systemOnline} onChange={(e) => setSystemOnline(e.target.checked)} />
              <span className="slider" />
            </label>
          </div>
        </div>

        <div className="content">{Page}</div>
      </main>
    </div>
  )
}
