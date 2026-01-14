import React, { useEffect, useMemo, useRef, useState } from "react"
import { apiClient, setApiToken } from "./lib/api"
import "./app.css"

type NavKey = "dashboard" | "uploads" | "jobRuns" | "incidents" | "reports" | "jobs"
type PillTone = "live" | "pipeline" | "execution" | "rca" | "workflow" | "exports"
type QueuedUploadFile = { id: string; file: File }
type DownloadFormat = "csv" | "pdf"
type ProcessMode = "transform_gradebook" | "append_record" | "delete_record" | "custom_rules"
type UserRole = "admin" | "moderator" | "user"
type AuthUser = { id?: number; username: string; role: UserRole; is_superuser?: boolean }
type StageDetailPayload = {
  step: any
  run?: any
  upload?: any
  nextStep?: any
}

const NAV: Array<{
  key: NavKey
  label: string
  icon: string
  pill: { text: string; tone: PillTone }
}> = [
  { key: "dashboard", label: "Batch Home", icon: "D", pill: { text: "OVERVIEW", tone: "live" } },
  { key: "uploads", label: "Batch Intake", icon: "U", pill: { text: "INTAKE", tone: "pipeline" } },
  { key: "jobRuns", label: "Batch Runs", icon: "J", pill: { text: "RUNS", tone: "execution" } },
  { key: "incidents", label: "Batch Issues", icon: "I", pill: { text: "ALERTS", tone: "rca" } },
  { key: "reports", label: "Batch Reports", icon: "R", pill: { text: "OUTPUTS", tone: "exports" } },
  { key: "jobs", label: "Batch Schedules", icon: "S", pill: { text: "AUTOMATION", tone: "workflow" } },
]

const MAX_UPLOAD_FILES = 5
const PAGE_STEP = 30
const LAST_NAV_KEY = "batchops:lastNav"
const LAST_UPLOAD_KEY = "batchops:lastUpload"
const UPLOAD_QUEUE_DB = "batchopsUploadQueue"
const UPLOAD_QUEUE_STORE = "files"

type AuthContextValue = {
  user: AuthUser | null
  role: UserRole
  canEditSchedules: boolean
  canRunSchedules: boolean
  canManageIncidents: boolean
  canCreateIncidents: boolean
}

const AuthContext = React.createContext<AuthContextValue | null>(null)

function resolveRole(user: AuthUser | null): UserRole {
  if (!user) return "user"
  if (user.is_superuser) return "admin"
  return user.role || "user"
}

function useAuth() {
  const ctx = React.useContext(AuthContext)
  if (!ctx) {
    return {
      user: null,
      role: "user" as UserRole,
      canEditSchedules: false,
      canRunSchedules: false,
      canManageIncidents: false,
      canCreateIncidents: false,
    }
  }
  return ctx
}

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
      ...(token ? { Authorization: `Token ${token}` } : {}),
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
      ...(token ? { Authorization: `Token ${token}` } : {}),
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
  className,
}: {
  title: string
  subtitle?: React.ReactNode
  onClose: () => void
  children: React.ReactNode
  className?: string
}) {
  return (
    <div className="modalBackdrop" onClick={onClose}>
      <div className={cx("modal", "detailModal", className)} onClick={(e) => e.stopPropagation()}>
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

function buildStageDetail(step: any, steps: any[], run?: any, upload?: any): StageDetailPayload {
  const match = steps.findIndex(
    (candidate) =>
      candidate === step ||
      (candidate?.name === step?.name && candidate?.started_at === step?.started_at && candidate?.status === step?.status),
  )
  const nextStep = match >= 0 && match + 1 < steps.length ? steps[match + 1] : undefined
  return { step, run, upload, nextStep }
}

function StageDetailModal({ stage, onClose }: { stage: StageDetailPayload; onClose: () => void }) {
  const step = stage.step || {}
  const uploadId =
    stage.upload?.upload_id ||
    stage.upload?.id ||
    stage.run?.upload_id ||
    stage.run?.upload?.upload_id ||
    stage.run?.upload
  const uploadName = stage.upload?.filename || stage.run?.upload?.filename || stage.run?.upload_filename
  const runId = stage.run?.run_id || stage.run?.id
  const fileDepartment = stage.upload?.department || stage.run?.upload?.department
  const fileStatus = stage.upload?.status || stage.run?.upload?.status
  const processLabel = formatProcessMode(stage.upload?.process_mode || stage.run?.upload?.process_mode)
  const fileNotes = stage.upload?.notes || stage.run?.upload?.notes
  const insight = stepInsight(step.name)
  const overview = insight?.overview || stepDescription(step.name)
  const checks = insight?.checks
  const output = insight?.output
  const hasFileDetails =
    Boolean(uploadName) || Boolean(fileDepartment) || Boolean(fileStatus) || (processLabel && processLabel !== "-")
  const title = `${formatStepName(step.name)} stage`
  const subtitle = (
    <span className="muted">
      {uploadName ? `${uploadName} | ` : ""}Intake {uploadId ? String(uploadId).slice(0, 8) : "-"}
      {runId ? ` | Run ${String(runId).slice(0, 8)}` : ""}
    </span>
  )
  const durationLabel =
    typeof step.duration_ms === "number" ? formatDuration(step.duration_ms) : formatDuration(undefined)
  return (
    <DetailModal title={title} subtitle={subtitle} onClose={onClose} className="stageDetailModal">
      <div className="detailGrid">
        <div>
          <div className="muted">Status</div>
          <span className={cx("statusChip", step.status === "failed" ? "bad" : step.status === "success" ? "ok" : "")}>
            {step.status || "-"}
          </span>
        </div>
        <div>
          <div className="muted">Started</div>
          <div>{fmtDate(step.started_at)}</div>
        </div>
        <div>
          <div className="muted">Finished</div>
          <div>{fmtDate(step.finished_at)}</div>
        </div>
        <div>
          <div className="muted">Duration</div>
          <div className="mono">{durationLabel}</div>
        </div>
      </div>

      {hasFileDetails ? (
        <div className="detailSection">
          <div className="detailTitle">Intake details</div>
          <div className="detailGrid">
            <div>
              <div className="muted">Intake file name</div>
              <div>{uploadName || "-"}</div>
            </div>
            <div>
              <div className="muted">Department</div>
              <div>{fileDepartment || "-"}</div>
            </div>
            <div>
              <div className="muted">Request type</div>
              <div>{processLabel}</div>
            </div>
            <div>
              <div className="muted">Intake status</div>
              <span className={cx("statusChip", fileStatus === "failed" ? "bad" : fileStatus === "published" ? "ok" : "")}>
                {fileStatus || "-"}
              </span>
            </div>
          </div>
        </div>
      ) : null}

      <div className="detailSection">
        <div className="detailTitle">What this stage does</div>
        <div className="detailTextBlock">{overview}</div>
      </div>

      {Array.isArray(checks) && checks.length > 0 ? (
        <div className="detailSection">
          <div className="detailTitle">What to check</div>
          <ul className="plainList">
            {checks.map((item, idx) => (
              <li key={`${step.name || "step"}-check-${idx}`}>{item}</li>
            ))}
          </ul>
        </div>
      ) : null}

      {output ? (
        <div className="detailSection">
          <div className="detailTitle">Output from this stage</div>
          <div className="detailTextBlock">{output}</div>
        </div>
      ) : null}

      {fileNotes ? (
        <div className="detailSection">
          <div className="detailTitle">Intake notes</div>
          <div className="detailTextBlock">{fileNotes}</div>
        </div>
      ) : null}

      <div className="detailSection">
        <div className="detailTitle">Notes from this stage</div>
        <div className="detailTextBlock">{step.logs || "No notes were recorded for this stage."}</div>
      </div>

      {stage.nextStep ? (
        <div className="detailSection">
          <div className="detailTitle">Next stage</div>
          <div className="detailTextBlock">{formatStepName(stage.nextStep.name)}</div>
        </div>
      ) : null}
    </DetailModal>
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
const IST_DATE_PARTS_FORMATTER = new Intl.DateTimeFormat(INDIAN_LOCALE, {
  timeZone: "Asia/Kolkata",
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
})

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

function getISTDateKeyFromDate(date: Date) {
  const parts = IST_DATE_PARTS_FORMATTER.formatToParts(date)
  let year = ""
  let month = ""
  let day = ""
  for (const part of parts) {
    if (part.type === "year") year = part.value
    if (part.type === "month") month = part.value
    if (part.type === "day") day = part.value
  }
  if (!year || !month || !day) return null
  return `${year}-${month}-${day}`
}

function getISTDateKey(input?: string) {
  const parsed = parseDateInput(input)
  if (!parsed || Number.isNaN(parsed.getTime())) return null
  return getISTDateKeyFromDate(parsed)
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

function parseDateOnly(value?: string) {
  if (!value) return null
  const parts = value.split("-").map((part) => Number(part))
  if (parts.length !== 3) return null
  const [year, month, day] = parts
  if (!year || !month || !day) return null
  return new Date(year, month - 1, day)
}

function parseMonthOnly(value?: string) {
  if (!value) return null
  const parts = value.split("-").map((part) => Number(part))
  if (parts.length !== 2) return null
  const [year, month] = parts
  if (!year || !month) return null
  return { year, month }
}

function getLocalDate(input?: string) {
  const parsed = parseDateInput(input)
  if (!parsed || Number.isNaN(parsed.getTime())) return null
  const istDate = convertToIST(parsed)
  return new Date(istDate.getFullYear(), istDate.getMonth(), istDate.getDate())
}

function getLocalDateParts(input?: string) {
  const localDate = getLocalDate(input)
  if (!localDate) return null
  return {
    year: localDate.getFullYear(),
    month: localDate.getMonth() + 1,
    day: localDate.getDate(),
    weekday: localDate.getDay(),
  }
}

function getLocalDateTime(input?: string) {
  const parsed = parseDateInput(input)
  if (!parsed || Number.isNaN(parsed.getTime())) return null
  return convertToIST(parsed)
}

function isWithinDays(input: string | undefined, days: number) {
  const dt = getLocalDateTime(input)
  if (!dt) return false
  const now = convertToIST(new Date())
  return now.getTime() - dt.getTime() <= days * 24 * 60 * 60 * 1000
}

function clampText(s: any, n = 120) {
  const t = String(s ?? "")
  if (t.length <= n) return t
  return t.slice(0, n) + "…"
}

function formatDuration(ms?: number | null) {
  if (typeof ms !== "number" || Number.isNaN(ms)) return "-"
  if (ms < 1000) return `${ms} ms`
  return `${(ms / 1000).toFixed(1)} sec`
}

function formatDurationShort(ms?: number | null) {
  if (typeof ms !== "number" || Number.isNaN(ms)) return "-"
  if (ms < 1000) return `${ms} ms`
  if (ms < 60000) return `${(ms / 1000).toFixed(1)} sec`
  return `${(ms / 60000).toFixed(1)} min`
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

function triggerCsvDownload(data: BlobPart, uploadId: string, mode?: string, mime = "text/csv") {
  const normalized = (mode || "transform_gradebook").toLowerCase()
  const prefix = normalized === "transform_gradebook" ? "summary" : "processed"
  const extension = mime === "application/pdf" ? "pdf" : "csv"
  const blob = new Blob([data], { type: mime })
  const link = document.createElement("a")
  link.href = window.URL.createObjectURL(blob)
  link.download = `${prefix}-${uploadId}.${extension}`
  document.body.appendChild(link)
  link.click()
  document.body.removeChild(link)
}

function base64ToArrayBuffer(encoded: string): ArrayBuffer {
  try {
    const binary = atob(encoded)
    const len = binary.length
    const buffer = new ArrayBuffer(len)
    const bytes = new Uint8Array(buffer)
    for (let i = 0; i < len; i += 1) {
      bytes[i] = binary.charCodeAt(i)
    }
    return buffer
  } catch {
    return new ArrayBuffer(0)
  }
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

async function ensureReportDownloaded(
  uploadId: string,
  opts?: { processMode?: ProcessMode; format?: DownloadFormat },
  tries = 4,
) {
  let knownMode: ProcessMode | undefined = opts?.processMode
  const format: DownloadFormat = opts?.format || "csv"
  try {
    const uploadData = await apiClient.uploads.get(uploadId)
    if (!knownMode && typeof uploadData?.process_mode === "string") {
      knownMode = uploadData.process_mode as ProcessMode
    }
    if (format === "csv" && uploadData?.report_csv) {
      triggerCsvDownload(uploadData.report_csv, uploadId, knownMode, "text/csv")
      return
    }
    if (format === "pdf" && uploadData?.report_pdf) {
      const pdfBlob =
        typeof uploadData.report_pdf === "string"
          ? new Blob([base64ToArrayBuffer(uploadData.report_pdf)], { type: "application/pdf" })
          : (uploadData.report_pdf as Blob)
      triggerCsvDownload(pdfBlob, uploadId, knownMode, "application/pdf")
      return
    }
  } catch {
    // ignore and fall back to API polling
  }

  const token = localStorage.getItem("jwt_token")
  for (let attempt = 0; attempt < tries; attempt += 1) {
    const url = new URL(`${baseURL()}/api/reports/summary/`)
    url.searchParams.set("upload_id", uploadId)
    url.searchParams.set("format", format)
    const res = await fetch(url.toString(), {
      headers: token ? { Authorization: `Token ${token}` } : undefined,
    })
    if (res.ok) {
      const blob = await res.blob()
      const contentType = res.headers.get("Content-Type") || ""
      const guessedMime = format === "pdf" || contentType.includes("pdf") ? "application/pdf" : "text/csv"
      triggerCsvDownload(blob, uploadId, knownMode, guessedMime)
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

const STEP_LABELS: Record<string, string> = {
  standardize_results: "Load file",
  validate_results: "Check data",
  transform_gradebook: "Clean data",
  generate_summary: "Summarize",
  publish_results: "Publish report",
}
const STEP_DESCRIPTIONS: Record<string, string> = {
  standardize_results:
    "Imports the file, detects columns, and normalizes headers so every record follows the same structure.",
  validate_results:
    "Checks that required columns and rows are present, and flags missing or incomplete data before moving on.",
  transform_gradebook:
    "Cleans values, trims spaces, converts numbers, and applies your selected changes (add or remove rows).",
  generate_summary:
    "Builds a short overview of totals and key statistics so the report is easy to understand.",
  publish_results:
    "Creates the final CSV and PDF, saves them, and marks the file as ready to download.",
}
const STEP_DETAILS: Record<string, { overview: string; checks: string[]; output: string }> = {
  standardize_results: {
    overview: "Reads the file and lines up columns so every row follows the same layout.",
    checks: ["File opens without errors.", "Column names look correct.", "Row count looks reasonable."],
    output: "A clean table that is ready for data checks.",
  },
  validate_results: {
    overview: "Checks required fields and flags missing or incomplete data before moving on.",
    checks: ["Required columns like student_id are present.", "Key fields are not blank.", "Unexpected gaps are flagged."],
    output: "A pass or fail decision with notes when something is missing.",
  },
  transform_gradebook: {
    overview: "Applies your selected change like cleaning, adding rows, or removing rows.",
    checks: ["Changes match the request you selected.", "Names and numbers look consistent.", "No unexpected values appear."],
    output: "Updated records ready for a report.",
  },
  generate_summary: {
    overview: "Builds a short summary with totals and quick statistics.",
    checks: ["Totals match the number of rows.", "Columns listed look correct.", "Summary looks complete."],
    output: "A summary table that feeds the final report.",
  },
  publish_results: {
    overview: "Packages the final CSV and PDF and stores them for download.",
    checks: ["Download buttons become active.", "File name matches your upload.", "Data looks correct in the report."],
    output: "Final files saved and ready to share.",
  },
}

const PROCESS_MODE_LABELS: Record<string, string> = {
  transform_gradebook: "Create clean report",
  append_record: "Add rows",
  delete_record: "Remove rows",
  custom_rules: "Custom request",
}

function formatStepName(name?: string) {
  const raw = String(name ?? "").trim()
  if (!raw) return "—"
  const normalized = raw.toLowerCase()
  if (STEP_LABELS[normalized]) return STEP_LABELS[normalized]
  const text = raw.replace(/_/g, " ").trim()
  if (!text) return "—"
  return text
    .split(" ")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ")
}

function stepDescription(name?: string) {
  const raw = String(name ?? "").trim()
  if (!raw) return "No description available."
  const normalized = raw.toLowerCase()
  return STEP_DESCRIPTIONS[normalized] || "This step prepares data for the next stage."
}

function stepInsight(name?: string) {
  const raw = String(name ?? "").trim()
  if (!raw) return null
  const normalized = raw.toLowerCase()
  return STEP_DETAILS[normalized] || null
}

function formatProcessMode(mode?: string) {
  const raw = String(mode ?? "").trim().toLowerCase()
  if (!raw) return "-"
  if (PROCESS_MODE_LABELS[raw]) return PROCESS_MODE_LABELS[raw]
  return raw
    .replace(/_/g, " ")
    .split(" ")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ")
}

function summarizeSteps(details: any) {
  const steps = details?.steps
  if (!Array.isArray(steps) || steps.length === 0) return "—"
  return steps
    .map((step: any) => `${formatStepName(step.name)}: ${step.status}`)
    .join(" | ")
}

function stageProgressLabel(upload: any, summary?: string) {
  if (summary && summary.includes(":")) return summary
  const status = String(upload?.status ?? "").toLowerCase()
  if (status === "processing" || status === "running") return "Running now"
  if (status === "failed") return "Needs attention"
  if (status === "published" || status === "success") return "Complete"
  if (status === "pending" || status === "queued") return "Waiting to start"
  return "Open to view"
}

/** ---------------- PAGES ---------------- */

function DashboardPage() {
  const [metricsWarning, setMetricsWarning] = useState<string | null>(null)

  const [todayUploads, setTodayUploads] = useState<number>(0)
  const [failedRecent, setFailedRecent] = useState<number>(0)
  const [activeRuns, setActiveRuns] = useState<number>(0)
  const [avgRunTime, setAvgRunTime] = useState<string>("—")
  const [activeSchedules, setActiveSchedules] = useState<number>(0)
  const [openIncidents, setOpenIncidents] = useState<number>(0)
  const [lastMttr, setLastMttr] = useState<string>("—")

  const [health, setHealth] = useState<Record<string, string> | null>(null)

  useEffect(() => {
    let mounted = true

    async function load() {
      // KPIs from existing endpoints
      try {
        const [uploadsRes, incidentsRes, runsRes, jobsRes] = await Promise.all([
          apiClient.uploads.list({ ordering: "-received_at" }),
          apiClient.incidents.list({ ordering: "-created_at" }),
          apiClient.jobRuns.list({ ordering: "-started_at" }),
          apiClient.jobs.list(),
        ])
        if (!mounted) return

        const uploads = unwrapList<any>(uploadsRes)
        const incidents = unwrapList<any>(incidentsRes)
        const runs = unwrapList<any>(runsRes)
        const jobs = unwrapList<any>(jobsRes)

        const todayKey = getISTDateKeyFromDate(new Date())
        const isToday = (value?: string) => {
          const key = getISTDateKey(value)
          return Boolean(key && todayKey && key === todayKey)
        }
        setTodayUploads(uploads.filter((u) => isToday(u.received_at)).length)
        setFailedRecent(uploads.filter((u) => u.status === "failed" && isWithinDays(u.received_at, 7)).length)
        setOpenIncidents(incidents.filter((r) => r.state === "open" || r.state === "in_progress").length)
        setActiveRuns(runs.filter((r) => ["running", "queued", "retrying"].includes(String(r.status))).length)
        setActiveSchedules(jobs.filter((job) => Boolean(job.schedule_cron)).length)

        // last MTTR: latest resolved incident (best-effort)
        const resolved = incidents.filter((r) => r.state === "resolved" && r.resolved_at)
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

        const completedRuns = runs.filter((r) => typeof r.duration_ms === "number" && r.duration_ms > 0)
        const recentRuns = completedRuns.slice(0, 10)
        if (recentRuns.length > 0) {
          const avgMs = Math.round(recentRuns.reduce((acc, r) => acc + r.duration_ms, 0) / recentRuns.length)
          setAvgRunTime(formatDurationShort(avgMs))
        } else {
          setAvgRunTime("—")
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
          setMetricsWarning("Metrics feed is temporarily unavailable.")
        }
      } catch {
        if (!mounted) return
        setMetricsWarning("Metrics feed is temporarily unavailable.")
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
          <div className="pageTitle">Batch overview</div>
          <div className="pageSub">A simple snapshot of batch intake, issues, and system status.</div>
          {metricsWarning ? <div className="warnPill">Warning: {metricsWarning}</div> : null}
        </div>

        <div className="rightHeader">
          <div className="pipelinePill">
            <span className="dot" />{" "}
            <span className="muted">{activeRuns > 0 ? "Batch engine busy" : "Batch engine idle"}</span>
            <span className="muted">
              {activeRuns > 0 ? `${activeRuns} active run(s)` : "Waiting for new batches"}
            </span>
          </div>
        </div>
      </div>

      <div className="kpiGrid">
        <Card>
          <KpiCard label="TODAY'S BATCHES" value={todayUploads} hint="Batch intakes today" />
        </Card>
        <Card>
          <KpiCard label="FAILED (7 DAYS)" value={failedRecent} hint="Batches needing a re-run" />
        </Card>
        <Card>
          <KpiCard label="AVG RUN TIME" value={avgRunTime} hint="Last 10 completed runs" />
        </Card>
        <Card>
          <KpiCard label="OPEN BATCH ISSUES" value={openIncidents} hint="Need attention" />
        </Card>
        <Card>
          <KpiCard label="ACTIVE SCHEDULES" value={activeSchedules} hint="Jobs on a timer" />
        </Card>
        <Card>
          <KpiCard label="LAST RESOLUTION TIME" value={lastMttr} hint="Last batch issue closed" />
        </Card>
      </div>

      <div className="grid2">
        <Card>
          <CardTitle title="How batches are handled" right={<span className="miniPill">Intake + Check + Improve + Share</span>} />
          <div className="pipelineBoxes">
            <div className="pipelineBox">
              <div className="pipelineBoxTitle">Intake</div>
              <div className="pipelineBoxText">Batches arrive and wait their turn.</div>
            </div>
            <div className="pipelineBox">
              <div className="pipelineBoxTitle">Check & clean</div>
              <div className="pipelineBoxText">We verify the batch data and tidy it up.</div>
            </div>
            <div className="pipelineBox">
              <div className="pipelineBoxTitle">Ready to deliver</div>
              <div className="pipelineBoxText">Batch reports are prepared for download.</div>
            </div>
          </div>
        </Card>

        <Card>
          <CardTitle title="System status" right={<span className="muted">Live status check</span>} />
          {!health ? (
            <div className="muted">Status is unavailable right now.</div>
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
        <div className="sectionTitle">Batch automations for schools</div>
        <ul className="plainList">
          <li>Send attendance reminders to students or parents.</li>
          <li>Send a daily system status summary to admins.</li>
          <li>Check exam or admissions portals for updates.</li>
          <li>Pull nightly data drops from each department.</li>
          <li>Clean up older records and archive reports.</li>
          <li>Run daily backups during off-peak hours.</li>
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
  const [processMode, setProcessMode] = useState<ProcessMode>("transform_gradebook")
  const [appendRowInput, setAppendRowInput] = useState("")
  const [deleteColumn, setDeleteColumn] = useState("")
  const [deleteValue, setDeleteValue] = useState("")
  const [deleteRulesInput, setDeleteRulesInput] = useState("")
  const [customInstructions, setCustomInstructions] = useState("")
  const [processError, setProcessError] = useState<string | null>(null)
  const [recentUploads, setRecentUploads] = useState<any[]>([])
  const [recentLoading, setRecentLoading] = useState(false)
  const [recentError, setRecentError] = useState<string | null>(null)
  const [recentRefresh, setRecentRefresh] = useState(0)
  const [recentQuery, setRecentQuery] = useState("")
  const [recentStatusFilter, setRecentStatusFilter] = useState("all")
  const [recentFromDate, setRecentFromDate] = useState("")
  const [recentToDate, setRecentToDate] = useState("")
  const [visibleUploadCount, setVisibleUploadCount] = useState(PAGE_STEP)
  const [stagesModal, setStagesModal] = useState<{ upload: any; run: any; steps: any[] } | null>(null)
  const [stagesLoading, setStagesLoading] = useState(false)
  const [stagesError, setStagesError] = useState<string | null>(null)
  const [stageDetail, setStageDetail] = useState<StageDetailPayload | null>(null)

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

  useEffect(() => {
    setProcessError(null)
    setAppendRowInput("")
    setDeleteColumn("")
    setDeleteValue("")
    setDeleteRulesInput("")
    setCustomInstructions("")
  }, [processMode])

  useEffect(() => {
    setVisibleUploadCount(PAGE_STEP)
  }, [recentQuery, recentStatusFilter, recentFromDate, recentToDate, recentUploads.length])

  useEffect(() => {
    let mounted = true
    async function loadRecent() {
      try {
        setRecentLoading(true)
        const list = await apiClient.uploads.list({ ordering: "-received_at" })
        const items = Array.isArray(list) ? list : []
        let runs: any[] = []
        try {
          const runList = await apiClient.jobRuns.list({ ordering: "-started_at" })
          runs = Array.isArray(runList) ? runList : []
        } catch {
          runs = []
        }
        const latestByUpload = new Map<string, any>()
        runs.forEach((run) => {
          const uploadId = run?.upload?.upload_id ?? run?.upload_id
          if (!uploadId) return
          const key = String(uploadId)
          if (!latestByUpload.has(key)) {
            latestByUpload.set(key, run)
          }
        })
        const merged = items.map((upload) => {
          const key = String(upload?.upload_id ?? "")
          const latestRun = key ? latestByUpload.get(key) : undefined
          return latestRun ? { ...upload, latestRun } : upload
        })
        if (!mounted) return
        setRecentUploads(merged)
        setRecentError(null)
      } catch {
        if (!mounted) return
        setRecentUploads([])
        setRecentError("Couldn't load files right now. Please refresh.")
      } finally {
        if (!mounted) return
        setRecentLoading(false)
      }
    }
    loadRecent()
    const t = setInterval(loadRecent, 6000)
    return () => {
      mounted = false
      clearInterval(t)
    }
  }, [currentUpload?.upload_id, recentRefresh])

  const recentQueryValue = recentQuery.trim().toLowerCase()
  const recentStartDate = parseDateOnly(recentFromDate)
  const recentEndDate = parseDateOnly(recentToDate)
  const filteredUploads = recentUploads.filter((upload) => {
    const status = String(upload.status || "").toLowerCase()
    if (recentStatusFilter !== "all" && status !== recentStatusFilter) return false
    if (recentQueryValue) {
      const name = String(upload.filename ?? "").toLowerCase()
      const department = String(upload.department ?? "").toLowerCase()
      if (!name.includes(recentQueryValue) && !department.includes(recentQueryValue)) return false
    }
    if (recentStartDate || recentEndDate) {
      const targetDate = getLocalDate(upload.received_at)
      if (!targetDate) return false
      if (recentStartDate && targetDate < recentStartDate) return false
      if (recentEndDate && targetDate > recentEndDate) return false
    }
    return true
  })
  const visibleUploads = filteredUploads.slice(0, visibleUploadCount)
  const showNoUploads = recentUploads.length === 0
  const showNoUploadMatches = !showNoUploads && filteredUploads.length === 0
  const showUploadLoadMore = filteredUploads.length > visibleUploads.length

  function buildProcessingConfig() {
    if (processMode === "append_record") {
      if (!appendRowInput.trim()) {
        throw new Error("Provide the record you want to append.")
      }
      let parsed
      try {
        parsed = JSON.parse(appendRowInput)
      } catch {
        throw new Error("Record must be valid JSON (use an object or array of objects).")
      }
      if (typeof parsed !== "object" || parsed === null) {
        throw new Error("Record must be a JSON object (or array of objects).")
      }
      const payload = Array.isArray(parsed) ? parsed : [parsed]
      if (!payload.every((item) => typeof item === "object" && item !== null && !Array.isArray(item))) {
        throw new Error("Each record must be a JSON object (key/value map).")
      }
      return { records: payload }
    }
    if (processMode === "delete_record") {
      if (deleteRulesInput.trim()) {
        let parsedRules
        try {
          parsedRules = JSON.parse(deleteRulesInput)
        } catch {
          throw new Error("Rules must be valid JSON (array of { column, value }).")
        }
        if (!Array.isArray(parsedRules) || parsedRules.length === 0) {
          throw new Error("Provide at least one rule with column/value.")
        }
        const normalized = parsedRules
          .filter((rule: any) => rule && typeof rule === "object")
          .map((rule: any) => ({ column: String(rule.column || "").trim(), value: String(rule.value ?? "") }))
          .filter((rule: any) => rule.column)
        if (normalized.length === 0) {
          throw new Error("Each delete rule must include a column name.")
        }
        return { rules: normalized }
      }
      if (!deleteColumn.trim() || !deleteValue.trim()) {
        throw new Error("Provide both the column name and value to delete rows.")
      }
      return { column: deleteColumn.trim(), value: deleteValue.trim() }
    }
    if (processMode === "custom_rules") {
      if (!customInstructions.trim()) {
        throw new Error("Describe the custom rule you want BatchOps to apply.")
      }
      return { notes: customInstructions.trim() }
    }
    return {}
  }

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
    let processConfig: any = {}
    try {
      processConfig = buildProcessingConfig()
      setProcessError(null)
    } catch (err: any) {
      setProcessError(typeof err?.message === "string" ? err.message : "Please check the details you entered.")
      return
    }
    const planConfigJson = JSON.stringify(processConfig)
    setSubmitting(true)
    try {
      const uploadOne = async (f: File) => {
        const fd = new FormData()
        fd.append("file", f)
        fd.append("department", department)
        fd.append("notes", notes)
        fd.append("process_mode", processMode)
        fd.append("process_config", planConfigJson)
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
          alert(`Queued ${successes.length} uploads. Monitor their progress in Batch Runs.`)
        }
      }

      const failures = results.filter((r) => r.status === "rejected")
      if (failures.length > 0 && successes.length === 0) {
        setUploadError(
          "All uploads failed. Please try again or contact an administrator."
        )
      } else if (failures.length > 0) {
        setUploadError(`${failures.length} upload(s) failed. Please try again.`)
      }
      setQueuedFiles([])
      persistQueuedFiles([]).catch(() => {})
      setFileError(null)
      if (fileInputRef.current) {
        fileInputRef.current.value = ""
      }
    } catch (e: any) {
      setUploadError("Upload failed. Please try again or contact an administrator.")
    } finally {
      setSubmitting(false)
    }
  }

  async function downloadReport(format: DownloadFormat) {
    if (!currentUpload?.upload_id) return
    if (currentUpload.status !== "published") {
      setReportHint("The report becomes available once processing is finished.")
      return
    }
    setReportHint(null)
    setReportLoading(true)
    try {
      await ensureReportDownloaded(currentUpload.upload_id, {
        processMode: currentUpload.process_mode as ProcessMode,
        format,
      })
    } catch (e: any) {
      setReportHint(typeof e?.message === "string" ? e.message : "Download failed. Try again shortly.")
    } finally {
      setReportLoading(false)
    }
  }

  async function openUploadStages(upload: any) {
    if (!upload?.upload_id) return
    setStagesError(null)
    setStagesLoading(true)
    const cachedRun = upload.latestRun ?? null
    const cachedSteps = Array.isArray(cachedRun?.details?.steps) ? cachedRun.details.steps : []
    setStagesModal({ upload, run: cachedRun, steps: cachedSteps })
    try {
      const runs = await apiClient.jobRuns.list({ upload_id: upload.upload_id, ordering: "-started_at" })
      const latestRun = Array.isArray(runs) ? runs[0] ?? null : null
      const steps = Array.isArray(latestRun?.details?.steps) ? latestRun.details.steps : []
      setStagesModal({ upload, run: latestRun, steps })
    } catch {
      if (cachedSteps.length === 0) {
      setStagesError("Couldn't load stages for this batch.")
      }
    } finally {
      setStagesLoading(false)
    }
  }

  function closeUploadStages() {
    setStagesModal(null)
    setStagesError(null)
    setStagesLoading(false)
    setStageDetail(null)
  }

  return (
    <div className="page">
      <div className="pageHeader">
        <div>
          <div className="pageTitle">Batch intake</div>
          <div className="pageSub">Upload batch files and we'll prepare clear reports for you.</div>
        </div>
      </div>

      <div className="grid2">
        <Card>
          <div className="formHeader">
            <div className="formTitle">Add batch files</div>
            <div className="muted">CSV, Excel, or PDF (up to 50 MB)</div>
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
            <label className="label">What should we do with the file?</label>
            <Segments
              value={processMode}
              onChange={(value) => setProcessMode(value as ProcessMode)}
              options={[
                { value: "transform_gradebook", label: "Create clean report" },
                { value: "append_record", label: "Add rows" },
                { value: "delete_record", label: "Remove rows" },
                { value: "custom_rules", label: "Custom request" },
              ]}
            />
            <div className="muted" style={{ marginTop: 6 }}>
              Pick what you want us to do before we build the report.
            </div>
            {processMode === "append_record" ? (
              <div style={{ marginTop: 12 }}>
                <textarea
                  className="textarea"
                  value={appendRowInput}
                  onChange={(e) => {
                    setAppendRowInput(e.target.value)
                    setProcessError(null)
                  }}
                  placeholder='Paste the rows to add (JSON). Example: [{"student_id":"S001","score":95}]'
                />
                <div className="muted" style={{ marginTop: 4 }}>
                  Use valid JSON. If unsure, leave this blank.
                </div>
              </div>
            ) : null}
            {processMode === "delete_record" ? (
              <div style={{ marginTop: 12 }}>
                <div className="muted" style={{ marginBottom: 6 }}>
                  Tell us which rows to remove. Use the quick fields or paste JSON rules.
                </div>
                <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                  <input
                    className="input"
                    style={{ flex: 1, minWidth: 160 }}
                    value={deleteColumn}
                    onChange={(e) => {
                      setDeleteColumn(e.target.value)
                      setProcessError(null)
                    }}
                    placeholder="Column name (example: student_id)"
                  />
                  <input
                    className="input"
                    style={{ flex: 1, minWidth: 160 }}
                    value={deleteValue}
                    onChange={(e) => {
                      setDeleteValue(e.target.value)
                      setProcessError(null)
                    }}
                    placeholder="Value to remove"
                  />
                </div>
                <textarea
                  className="textarea"
                  style={{ marginTop: 10 }}
                  value={deleteRulesInput}
                  onChange={(e) => {
                    setDeleteRulesInput(e.target.value)
                    setProcessError(null)
                  }}
                  placeholder='Optional JSON rules. Example: [{"column":"class","value":"10-A"},{"column":"status","value":"absent"}]'
                />
              </div>
            ) : null}
            {processMode === "custom_rules" ? (
              <div style={{ marginTop: 12 }}>
                <textarea
                  className="textarea"
                  value={customInstructions}
                  onChange={(e) => {
                    setCustomInstructions(e.target.value)
                    setProcessError(null)
                  }}
                  placeholder="Describe the change you want (example: highlight top performers)."
                />
              </div>
            ) : null}
            {processError ? <div className="errorLine">{processError}</div> : null}
          </div>

          <div className="field">
            <label className="label">Batch files</label>
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
                  ? `Queued ${queuedFiles.length}/${MAX_UPLOAD_FILES} batch files${
                      filesAtLimit ? " (limit reached, remove one to add another)" : ""
                    }. They will be processed together once you start.`
                  : `Choose up to ${MAX_UPLOAD_FILES} batch files. They will be processed together once you start.`}
              </div>
            </div>
            {fileError ? <div className="errorLine">{fileError}</div> : null}
            {queuedFiles.length > 0 ? (
              <div className="fileList">
                <div className="fileListHead">
                  <div>Batch file</div>
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
              ? `Start batch processing ${queuedFiles.length} files`
              : "Start batch processing"}
          </button>
        </Card>

        <Card>
          <div className="formHeader">
            <div className="formTitle">Latest batch</div>
            {currentUpload?.upload_id ? (
              <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                <button className="ghostBtn" type="button" onClick={() => openUploadStages(currentUpload)} disabled={stagesLoading}>
                  {stagesLoading ? "Loading stages..." : "View stages"}
                </button>
                <button className="ghostBtn" type="button" onClick={() => downloadReport("csv")} disabled={reportLoading}>
                  {reportLoading ? "Fetching..." : "Download CSV"}
                </button>
                <button className="ghostBtn" type="button" onClick={() => downloadReport("pdf")} disabled={reportLoading}>
                  {reportLoading ? "Fetching..." : "Download PDF"}
                </button>
              </div>
            ) : null}
          </div>
          {reportHint ? <div className="errorLine">{reportHint}</div> : null}

          {!currentUpload ? (
            <div className="muted">After you upload, the status and report links will appear here.</div>
          ) : (
            <div className="currentUpload">
              <div className="row">
                <span className="muted">Intake ID</span>
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
                    <span className="muted">Batch stages</span>
                    <span className="mono">
                      {["standardize_results", "validate_results", "transform_gradebook", "generate_summary", "publish_results"].map(
                        (step) => {
                          const r = currentRuns.find((run) => run.job?.name === step || run.job_name === step)
                          const s = r?.status ?? "pending"
                          return `${formatStepName(step)}: ${s}`
                        },
                      ).join(" | ")}
                    </span>
                  </div>
                ) : null}
                {currentIncident ? (
                  <div className="row">
                  <span className="muted">Last issue</span>
                    <span title={String(currentIncident.error ?? "")}>
                      {currentIncident.state === "resolved" ? "Resolved" : "Open"} |{" "}
                      {currentIncident.is_known ? "Known issue" : "Unknown"} |{" "}
                      {clampText(currentIncident.error ?? "", 80) || "—"}
                    </span>
                  </div>
                ) : null}
            </div>
          )}
        </Card>
      </div>

      <Card style={{ marginTop: 16 }}>
        <div className="tableHeader">
          <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
            <div className="muted">Batch intakes</div>
            <div className="muted" style={{ fontSize: 12 }}>
              Open a batch to see each stage and its notes.
            </div>
          </div>
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
            <input
              className="input"
              style={{ minWidth: 200 }}
              value={recentQuery}
              onChange={(e) => setRecentQuery(e.target.value)}
              placeholder="Search batches or department"
            />
            <select className="input" value={recentStatusFilter} onChange={(e) => setRecentStatusFilter(e.target.value)}>
              <option value="all">All statuses</option>
              <option value="pending">Pending</option>
              <option value="processing">Processing</option>
              <option value="published">Published</option>
              <option value="failed">Failed</option>
            </select>
            <button className="ghostBtn" type="button" onClick={() => setRecentRefresh((x) => x + 1)} disabled={recentLoading}>
              {recentLoading ? "Refreshing..." : "Refresh"}
            </button>
          </div>
        </div>
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginTop: 10 }}>
          <div>
            <div className="muted" style={{ fontSize: 12 }}>From</div>
            <input className="input" type="date" value={recentFromDate} onChange={(e) => setRecentFromDate(e.target.value)} />
          </div>
          <div>
            <div className="muted" style={{ fontSize: 12 }}>To</div>
            <input className="input" type="date" value={recentToDate} onChange={(e) => setRecentToDate(e.target.value)} />
          </div>
          <button
            className="ghostBtn"
            type="button"
            onClick={() => {
              setRecentQuery("")
              setRecentStatusFilter("all")
              setRecentFromDate("")
              setRecentToDate("")
            }}
          >
            Clear filters
          </button>
        </div>
        {recentError ? <div className="errorLine">{recentError}</div> : null}
        <div className="tableWrapper">
          <div className="table uploadsTable">
            <div className="tHead">
              <div>Batch file</div>
              <div>Status</div>
              <div>Received</div>
              <div>Progress</div>
              <div>Actions</div>
            </div>
            {showNoUploads ? (
              <div className="muted" style={{ paddingTop: 10 }}>
                No batch intakes yet.
              </div>
            ) : showNoUploadMatches ? (
              <div className="muted" style={{ paddingTop: 10 }}>
                No files match the current filters.
              </div>
            ) : (
              visibleUploads.map((upload) => {
                const summary = summarizeSteps(upload.latestRun?.details)
                const progress = stageProgressLabel(upload, summary)
                const progressLabel = clampText(progress, 80)
                return (
                  <div className="tRow" key={upload.upload_id ?? upload.id ?? Math.random()}>
                    <div title={upload.filename || ""}>{upload.filename || "Untitled file"}</div>
                    <div>
                      <span
                        className={cx(
                          "statusChip",
                          upload.status === "failed" ? "bad" : upload.status === "published" ? "ok" : "",
                        )}
                      >
                        {upload.status || "pending"}
                      </span>
                    </div>
                    <div>{fmtDate(upload.received_at)}</div>
                    <div title={summary || progress}>{progressLabel}</div>
                    <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                      <button className="ghostBtn ghostBtnSmall" type="button" onClick={() => openUploadStages(upload)}>
                        View stages
                      </button>
                    </div>
                  </div>
                )
              })
            )}
          </div>
        </div>
        {!showNoUploads && filteredUploads.length > 0 ? (
          <div style={{ display: "flex", justifyContent: "space-between", marginTop: 12, alignItems: "center" }}>
            <div className="muted">
              Showing {Math.min(visibleUploadCount, filteredUploads.length)} of {filteredUploads.length}
            </div>
            {showUploadLoadMore ? (
              <button className="ghostBtn" type="button" onClick={() => setVisibleUploadCount((c) => c + PAGE_STEP)}>
                Load more
              </button>
            ) : null}
          </div>
        ) : null}
      </Card>

      {stagesModal ? (
        <DetailModal
          title="Batch stages"
          subtitle={
            <span className="muted">
              {stagesModal.upload?.filename ? `${stagesModal.upload.filename} | ` : ""}Intake{" "}
              {stagesModal.upload?.upload_id ? String(stagesModal.upload.upload_id).slice(0, 8) : "-"}
            </span>
          }
          onClose={closeUploadStages}
        >
          {stagesError ? <div className="errorLine">{stagesError}</div> : null}
          {stagesLoading ? (
            <div className="muted">Loading stage details...</div>
          ) : stagesModal.steps.length === 0 ? (
            <div className="muted">No stages recorded for this batch yet.</div>
          ) : (
            <div className="stepList">
              {stagesModal.steps.map((step: any) => {
                const stepKey = `${step.name ?? "step"}-${step.started_at ?? ""}`
                return (
                  <div className="stepRow" key={`${stagesModal.upload?.upload_id ?? "file"}-${stepKey}`}>
                    <div>
                      <div className="stepName">{formatStepName(step.name)}</div>
                      <div className="muted">{stepDescription(step.name)}</div>
                      <div className="mono">
                        {fmtDate(step.started_at)} to {fmtDate(step.finished_at)}
                      </div>
                    </div>
                    <div className="stepActions">
                      <span
                        className={cx(
                          "statusChip",
                          step.status === "failed" ? "bad" : step.status === "success" ? "ok" : "",
                        )}
                      >
                        {step.status}
                      </span>
                      <button
                        className="ghostBtn ghostBtnSmall"
                        type="button"
                        onClick={() =>
                          setStageDetail(buildStageDetail(step, stagesModal.steps, stagesModal.run, stagesModal.upload))
                        }
                      >
                        View stage
                      </button>
                    </div>
                  </div>
                )
              })}
            </div>
          )}
        </DetailModal>
      ) : null}

      {stageDetail ? <StageDetailModal stage={stageDetail} onClose={() => setStageDetail(null)} /> : null}
    </div>
  )
}

function JobRunsPage() {
  const [filter, setFilter] = useState("all")
  const [counts, setCounts] = useState({ total: 0, queued: 0, running: 0, success: 0, failed: 0, retrying: 0 })
  const [rows, setRows] = useState<any[]>([])
  const [err, setErr] = useState<string | null>(null)
  const [refreshTick, setRefreshTick] = useState(0)
  const [runDateFrom, setRunDateFrom] = useState("")
  const [runDateTo, setRunDateTo] = useState("")
  const [visibleRunCount, setVisibleRunCount] = useState(PAGE_STEP)

  const [selectedRun, setSelectedRun] = useState<any | null>(null)
  const [selectedIncident, setSelectedIncident] = useState<any | null>(null)
  const [detailError, setDetailError] = useState<string | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [stageDetail, setStageDetail] = useState<StageDetailPayload | null>(null)
  const selectedSteps = selectedRun && Array.isArray(selectedRun.details?.steps) ? selectedRun.details.steps : []
  const selectedUploadId =
    selectedRun && (selectedRun.upload?.upload_id ?? selectedRun.upload_id ?? (typeof selectedRun.upload === "string" ? selectedRun.upload : null))

  useEffect(() => {
    setVisibleRunCount(PAGE_STEP)
  }, [filter, runDateFrom, runDateTo])

  useEffect(() => {
    let mounted = true
    async function load() {
      try {
        const res = await apiClient.jobRuns.list({ ordering: "-started_at" })
        const all = unwrapList<any>(res)
        const startDate = parseDateOnly(runDateFrom)
        const endDate = parseDateOnly(runDateTo)
        const dateFiltered = all.filter((r) => {
          if (!startDate && !endDate) return true
          const targetDate = getLocalDate(r.started_at || r.finished_at || r.created_at)
          if (!targetDate) return false
          if (startDate && targetDate < startDate) return false
          if (endDate && targetDate > endDate) return false
          return true
        })
        const c = {
          total: dateFiltered.length,
          queued: dateFiltered.filter((r) => r.status === "queued").length,
          running: dateFiltered.filter((r) => r.status === "running").length,
          success: dateFiltered.filter((r) => r.status === "success").length,
          failed: dateFiltered.filter((r) => r.status === "failed").length,
          retrying: dateFiltered.filter((r) => r.status === "retrying").length,
        }
        const filtered = filter === "all" ? dateFiltered : dateFiltered.filter((r) => r.status === filter)
        if (!mounted) return
        setCounts(c)
        setRows(filtered)
        setErr(null)
      } catch {
        if (!mounted) return
        setCounts({ total: 0, queued: 0, running: 0, success: 0, failed: 0, retrying: 0 })
        setRows([])
        setErr("Couldn't load batch runs. Please refresh.")
      }
    }
    load()
    const t = setInterval(load, 4000)
    return () => {
      mounted = false
      clearInterval(t)
    }
  }, [filter, refreshTick, runDateFrom, runDateTo])

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
      setDetailError("Couldn't load issue details for this run.")
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

  useEffect(() => {
    setStageDetail(null)
  }, [selectedRun?.run_id, selectedRun?.id])

  const visibleRuns = rows.slice(0, visibleRunCount)
  const showRunLoadMore = rows.length > visibleRunCount

  return (
    <div className="page pageTall">
      <div className="pageHeader">
        <div>
          <div className="pageTitle">Batch runs</div>
          <div className="pageSub">See how each batch run is progressing and when it finishes.</div>
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
        <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginTop: 12, alignItems: "flex-end" }}>
          <div>
            <div className="muted" style={{ fontSize: 12 }}>From</div>
            <input className="input" type="date" value={runDateFrom} onChange={(e) => setRunDateFrom(e.target.value)} />
          </div>
          <div>
            <div className="muted" style={{ fontSize: 12 }}>To</div>
            <input className="input" type="date" value={runDateTo} onChange={(e) => setRunDateTo(e.target.value)} />
          </div>
          <button
            className="ghostBtn"
            type="button"
            onClick={() => {
              setRunDateFrom("")
              setRunDateTo("")
            }}
          >
            Clear dates
          </button>
        </div>
      </Card>

      <Card>
        <div className="tableHeader">
          <div className="muted">Latest batch runs</div>
          <div className="muted">Auto-updating</div>
        </div>
        {err ? <div className="errorLine">{err}</div> : null}

        <div className="tableWrapper">
          <div className="table">
            <div className="tHead">
              <div>Batch run ID</div>
              <div>Batch job</div>
              <div>Status</div>
              <div>Intake ID</div>
              <div>Batch summary</div>
              <div>Started</div>
              <div>Finished</div>
              <div>Duration</div>
              <div>Result</div>
            </div>

            {rows.length === 0 ? (
              <div className="muted" style={{ paddingTop: 10 }}>
                No batch runs yet.
              </div>
            ) : (
              visibleRuns.map((r) => {
                const uploadId =
                  r.upload?.upload_id ??
                  r.upload_id ??
                  (typeof r.upload === "string" ? r.upload : null)
                return (
                  <div className="tRow" key={r.run_id ?? r.id ?? Math.random()} onClick={() => loadDetails(r)}>
                    <div className="mono">{String(r.run_id ?? "—").slice(0, 8)}</div>
                    <div className="taskCell">{r.job?.name ?? r.job_name ?? "—"}</div>
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
        {rows.length > 0 ? (
          <div style={{ display: "flex", justifyContent: "space-between", marginTop: 12, alignItems: "center" }}>
            <div className="muted">
              Showing {Math.min(visibleRunCount, rows.length)} of {rows.length}
            </div>
            {showRunLoadMore ? (
              <button className="ghostBtn" type="button" onClick={() => setVisibleRunCount((c) => c + PAGE_STEP)}>
                Load more
              </button>
            ) : null}
          </div>
        ) : null}
      </Card>

      {selectedRun ? (
        <DetailModal
          title="Batch run details"
          subtitle={
            <span className="muted">
              {selectedRun.job?.name ?? selectedRun.job_name ?? "—"} |{" "}
              {selectedUploadId ? String(selectedUploadId).slice(0, 8) : "no file"}
            </span>
          }
          onClose={closeRunDetail}
        >
          {detailError ? <div className="errorLine">{detailError}</div> : null}
          <div className="detailGrid">
            <div>
              <div className="muted">Batch run ID</div>
              <div className="mono">{String(selectedRun.run_id ?? "—")}</div>
            </div>
            <div>
              <div className="muted">Intake ID</div>
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
              <div className="muted">Result code</div>
              <div className="mono">{typeof selectedRun.exit_code === "number" ? selectedRun.exit_code : "—"}</div>
            </div>
          </div>

          <div className="detailSection">
            <div className="detailTitle">Batch stages</div>
            {selectedSteps.length === 0 ? (
              <div className="muted">No step details recorded for this run.</div>
            ) : (
              <div className="stepList">
                {selectedSteps.map((step: any) => {
                  const stepKey = `${step.name ?? "step"}-${step.started_at ?? ""}`
                  return (
                    <div className="stepRow" key={`${selectedRun.run_id}-${stepKey}`}>
                      <div>
                        <div className="stepName">{formatStepName(step.name)}</div>
                        <div className="muted">{stepDescription(step.name)}</div>
                        <div className="mono">
                          {fmtDate(step.started_at)} to {fmtDate(step.finished_at)}
                        </div>
                      </div>
                      <div className="stepActions">
                        <span
                          className={cx(
                            "statusChip",
                            step.status === "failed" ? "bad" : step.status === "success" ? "ok" : "",
                          )}
                        >
                          {step.status}
                        </span>
                        <button
                          className="ghostBtn ghostBtnSmall"
                          type="button"
                          onClick={() =>
                            setStageDetail(
                              buildStageDetail(step, selectedSteps, selectedRun, { upload_id: selectedUploadId }),
                            )
                          }
                        >
                          View stage
                        </button>
                      </div>
                    </div>
                  )
                })}
              </div>
            )}
          </div>

          <div className="detailSection">
            <div className="detailTitle">System notes</div>
            <pre className="detailLogs">{String(selectedRun.logs ?? "No notes captured for this run.")}</pre>
          </div>

          <div className="detailSection">
            <div className="detailTitle">Issue</div>
            {detailLoading ? (
              <div className="muted">Loading related issue...</div>
            ) : !selectedIncident ? (
              <div className="muted">No issue recorded for this run yet.</div>
            ) : (
              <div className="incidentDetail">
                <div className="detailRow">
                  <span className="muted">Issue ID</span>
                  <span className="mono">
                    {selectedIncident.incident_id
                      ? String(selectedIncident.incident_id).slice(0, 8)
                      : String(selectedIncident.id ?? "—").slice(0, 8)}
                  </span>
                </div>
                <div className="detailRow">
                  <span className="muted">Status</span>
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
                  <span className="muted">Known issue type</span>
                  <span>
                    {selectedIncident.is_known
                      ? selectedIncident.matched_known_error_name || "Known issue type"
                      : "Unknown"}
                  </span>
                </div>
                <div className="detailRow">
                  <span className="muted">What went wrong</span>
                  <span>{clampText(selectedIncident.error ?? "", 160) || "-"}</span>
                </div>
                <div className="detailRow">
                  <span className="muted">What caused it</span>
                  <span>{selectedIncident.root_cause || "-"}</span>
                </div>
                <div className="detailRow">
                  <span className="muted">How we fixed it</span>
                  <span>{selectedIncident.corrective_action || "-"}</span>
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

      {stageDetail ? <StageDetailModal stage={stageDetail} onClose={() => setStageDetail(null)} /> : null}
    </div>
  )
}

function IncidentsPage() {
  const auth = useAuth()
  const [filter, setFilter] = useState("all")
  const [counts, setCounts] = useState({ total: 0, open: 0, in_progress: 0, resolved: 0 })
  const [rows, setRows] = useState<any[]>([])
  const [loadError, setLoadError] = useState<string | null>(null)
  const [refreshTick, setRefreshTick] = useState(0)
  const [dateFilterMode, setDateFilterMode] = useState("all")
  const [dateFilterDate, setDateFilterDate] = useState("")
  const [dateFilterDay, setDateFilterDay] = useState("1")
  const [dateFilterMonth, setDateFilterMonth] = useState("")
  const [dateFilterYear, setDateFilterYear] = useState(() => String(new Date().getFullYear()))
  const [visibleIncidentCount, setVisibleIncidentCount] = useState(PAGE_STEP)

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
    setVisibleIncidentCount(PAGE_STEP)
  }, [filter, dateFilterMode, dateFilterDate, dateFilterDay, dateFilterMonth, dateFilterYear])

  const matchesIncidentDate = (value?: string) => {
    if (dateFilterMode === "all") return true
    const parts = getLocalDateParts(value)
    if (!parts) return false
    if (dateFilterMode === "date") {
      const selected = parseDateOnly(dateFilterDate)
      if (!selected) return true
      return (
        parts.year === selected.getFullYear() &&
        parts.month === selected.getMonth() + 1 &&
        parts.day === selected.getDate()
      )
    }
    if (dateFilterMode === "day") {
      const dayNumber = Number(dateFilterDay)
      if (Number.isNaN(dayNumber)) return true
      return parts.weekday === dayNumber
    }
    if (dateFilterMode === "month") {
      const selected = parseMonthOnly(dateFilterMonth)
      if (!selected) return true
      return parts.year === selected.year && parts.month === selected.month
    }
    if (dateFilterMode === "year") {
      const yearNumber = Number(dateFilterYear)
      if (!yearNumber) return true
      return parts.year === yearNumber
    }
    return true
  }

  useEffect(() => {
    let mounted = true
    async function load() {
      try {
        const res = await apiClient.incidents.list({ ordering: "-created_at" })
        const all = unwrapList<any>(res)
        const dateFiltered = all.filter((r) => matchesIncidentDate(r.created_at || r.updated_at))
        const c = {
          total: dateFiltered.length,
          open: dateFiltered.filter((r) => r.state === "open").length,
          in_progress: dateFiltered.filter((r) => r.state === "in_progress").length,
          resolved: dateFiltered.filter((r) => r.state === "resolved").length,
        }
        const filtered = filter === "all" ? dateFiltered : dateFiltered.filter((r) => r.state === filter)
        if (!mounted) return
        setCounts(c)
        setRows(filtered)
        setLoadError(null)
      } catch {
        if (!mounted) return
        setCounts({ total: 0, open: 0, in_progress: 0, resolved: 0 })
        setRows([])
        setLoadError("Couldn't load issues. Please refresh.")
      }
    }
    load()
    const t = setInterval(load, 5000)
    return () => {
      mounted = false
      clearInterval(t)
    }
  }, [
    filter,
    refreshTick,
    dateFilterMode,
    dateFilterDate,
    dateFilterDay,
    dateFilterMonth,
    dateFilterYear,
  ])

  async function createIncident() {
    if (!auth.canCreateIncidents) {
      alert("Only moderators and admins can create batch issues.")
      return
    }
    if (!newUploadId.trim()) {
      alert("Intake ID is required to create a batch issue.")
      return
    }
    const payload: any = {
      upload: newUploadId.trim(),
      error: newErrorText.trim() || "Manual issue created from dashboard.",
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
      alert("Create issue failed. Please try again.")
    } finally {
      setActionBusy(null)
    }
  }

  async function assignIncident(incidentId?: string) {
    if (!auth.canManageIncidents) {
      alert("Only moderators and admins can assign batch issues.")
      return
    }
    if (!incidentId) {
      alert("Missing issue ID.")
      return
    }
    let assignee = ""
    if (auth.role === "admin") {
      assignee = prompt("Assign to (moderator username):") || ""
    } else {
      assignee = auth.user?.username || ""
      if (!assignee) {
        alert("Sign in again so we can claim this issue.")
        return
      }
    }
    if (!assignee) return
    try {
      setActionBusy(incidentId)
      await postJSON(`/api/incidents/${incidentId}/assign/`, { assignee })
      setRefreshTick((x) => x + 1)
    } catch {
      alert("Assign failed. Please try again.")
    } finally {
      setActionBusy(null)
    }
  }

  async function resolveIncident(incidentId?: string) {
    if (!auth.canManageIncidents) {
      alert("Only moderators and admins can resolve batch issues.")
      return
    }
    if (!incidentId) {
      alert("Missing issue ID.")
      return
    }
    const root = prompt("What caused it (optional):") ?? ""
    const action = prompt("How we fixed it (optional):") ?? ""
    const report = prompt("Resolution summary (optional):") ?? ""
    try {
      setActionBusy(incidentId)
      await postJSON(`/api/incidents/${incidentId}/resolve/`, {
        root_cause: root,
        corrective_action: action,
        resolution_report: report,
      })
      setRefreshTick((x) => x + 1)
    } catch {
      alert("Resolve failed. Please try again.")
    } finally {
      setActionBusy(null)
    }
  }

  async function analyzeIncident(incidentId?: string) {
    if (!auth.canManageIncidents) {
      alert("Only moderators and admins can update analysis.")
      return
    }
    if (!incidentId) {
      alert("Missing issue ID.")
      return
    }
    const severity = prompt("Severity (low/medium/high/critical):", "medium") || "medium"
    const impact = prompt("Who or what was affected (optional):") || ""
    const analysis = prompt("Notes:", "") || ""
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
    if (!auth.canManageIncidents) {
      alert("Only moderators and admins can retry batch issues.")
      return
    }
    if (!incidentId) {
      alert("Missing issue ID.")
      return
    }
    const notes = prompt("Retry notes (optional):") || ""
    try {
      setActionBusy(incidentId)
      await postJSON(`/api/incidents/${incidentId}/retry/`, { notes })
      setRefreshTick((x) => x + 1)
    } catch {
      alert("Retry failed. Please try again.")
    } finally {
      setActionBusy(null)
    }
  }

  async function archiveIncident(incidentId?: string) {
    if (!auth.canManageIncidents) {
      alert("Only moderators and admins can archive batch issues.")
      return
    }
    if (!incidentId) {
      alert("Missing issue ID.")
      return
    }
    const confirmArchive = confirm("Archive this issue? This will mark it resolved and hide it from the list.")
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
      setIncidentDetailError("Couldn't load latest issue details.")
    } finally {
      setIncidentDetailLoading(false)
    }
  }

  function closeIncidentDetail() {
    setInspectingIncident(null)
    setIncidentDetailError(null)
    setIncidentDetailLoading(false)
  }

  const visibleIncidents = rows.slice(0, visibleIncidentCount)
  const showIncidentLoadMore = rows.length > visibleIncidentCount

  return (
    <div className="page pageTall">
      <div className="pageHeader">
        <div>
          <div className="pageTitle">Batch issues</div>
          <div className="pageSub">Problems found while processing batches. Track and resolve them here.</div>
          {auth.role === "user" ? (
            <div className="muted" style={{ fontSize: 12 }}>
              View-only access. Ask a moderator to assign, resolve, or retry issues.
            </div>
          ) : null}
        </div>
        <div style={{ display: "flex", gap: 10 }}>
          <button className="ghostBtn" type="button" onClick={() => setRefreshTick((x) => x + 1)}>
            Refresh
          </button>
          <button
            className="primaryBtnSmall"
            type="button"
            onClick={() => setShowCreate(true)}
            disabled={!auth.canCreateIncidents}
            title={auth.canCreateIncidents ? "" : "Moderators and admins can create issues."}
          >
            + Create batch issue
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
        <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginTop: 12, alignItems: "flex-end" }}>
          <div>
            <div className="muted" style={{ fontSize: 12 }}>Filter by</div>
            <select className="input" value={dateFilterMode} onChange={(e) => setDateFilterMode(e.target.value)}>
              <option value="all">All dates</option>
              <option value="date">Exact date</option>
              <option value="day">Day of week</option>
              <option value="month">Month</option>
              <option value="year">Year</option>
            </select>
          </div>
          {dateFilterMode === "date" ? (
            <div>
              <div className="muted" style={{ fontSize: 12 }}>Date</div>
              <input
                className="input"
                type="date"
                value={dateFilterDate}
                onChange={(e) => setDateFilterDate(e.target.value)}
              />
            </div>
          ) : null}
          {dateFilterMode === "day" ? (
            <div>
              <div className="muted" style={{ fontSize: 12 }}>Day</div>
              <select className="input" value={dateFilterDay} onChange={(e) => setDateFilterDay(e.target.value)}>
                <option value="0">Sunday</option>
                <option value="1">Monday</option>
                <option value="2">Tuesday</option>
                <option value="3">Wednesday</option>
                <option value="4">Thursday</option>
                <option value="5">Friday</option>
                <option value="6">Saturday</option>
              </select>
            </div>
          ) : null}
          {dateFilterMode === "month" ? (
            <div>
              <div className="muted" style={{ fontSize: 12 }}>Month</div>
              <input
                className="input"
                type="month"
                value={dateFilterMonth}
                onChange={(e) => setDateFilterMonth(e.target.value)}
              />
            </div>
          ) : null}
          {dateFilterMode === "year" ? (
            <div>
              <div className="muted" style={{ fontSize: 12 }}>Year</div>
              <input
                className="input"
                type="number"
                value={dateFilterYear}
                onChange={(e) => setDateFilterYear(e.target.value)}
                min="2000"
                max="2100"
              />
            </div>
          ) : null}
          <button
            className="ghostBtn"
            type="button"
            onClick={() => {
              setDateFilterMode("all")
              setDateFilterDate("")
              setDateFilterDay("1")
              setDateFilterMonth("")
              setDateFilterYear(String(new Date().getFullYear()))
            }}
          >
            Clear date filters
          </button>
        </div>
      </Card>

      <Card>
        <div className="tableHeader">
          <div className="muted">Latest batch issues</div>
          <div className="muted">Auto-updating</div>
        </div>
        {loadError ? <div className="errorLine">{loadError}</div> : null}

        <div className="tableWrapper">
          <div className="table">
            <div className="tHead">
              <div>Issue ID</div>
              <div>Status</div>
              <div>Severity</div>
              <div>Created</div>
              <div>Intake</div>
              <div>Error</div>
              <div>Assignee</div>
              <div>Known issue?</div>
              <div>Actions</div>
            </div>

            {rows.length === 0 ? (
              <div className="muted" style={{ paddingTop: 10 }}>
                No issues yet.
              </div>
            ) : (
              visibleIncidents.map((r) => {
                const incidentKey = r.incident_id ?? r.id ?? Math.random()
                const incidentId = r.incident_id ?? r.id
                const assignee = r.assignee ? String(r.assignee) : ""
                const isAssignedToOther = assignee && assignee !== auth.user?.username
                const assignLabel = auth.role === "admin" ? "Assign" : "Claim"
                const assignDisabled =
                  actionBusy === incidentId ||
                  r.state === "resolved" ||
                  !auth.canManageIncidents ||
                  (auth.role === "moderator" && isAssignedToOther)
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
                    <div>{assignee || "Unassigned"}</div>
                    <div>{r.is_known ? "Known" : "Unknown"}</div>
                    <div style={{ display: "flex", gap: 8 }}>
                      <button
                        className="ghostBtn"
                        type="button"
                        disabled={assignDisabled}
                        onClick={(e) => {
                          e.stopPropagation()
                          assignIncident(incidentId ? String(incidentId) : undefined)
                        }}
                        title={
                          !auth.canManageIncidents
                            ? "Moderators and admins can assign issues."
                            : auth.role === "moderator" && isAssignedToOther
                            ? "Already assigned to another moderator."
                            : ""
                        }
                      >
                        {assignLabel}
                      </button>
                      <button
                        className="ghostBtn"
                        type="button"
                        disabled={actionBusy === incidentId || r.state === "resolved" || !auth.canManageIncidents}
                        onClick={(e) => {
                          e.stopPropagation()
                          resolveIncident(incidentId ? String(incidentId) : undefined)
                        }}
                        title={auth.canManageIncidents ? "" : "Moderators and admins can resolve issues."}
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
        {rows.length > 0 ? (
          <div style={{ display: "flex", justifyContent: "space-between", marginTop: 12, alignItems: "center" }}>
            <div className="muted">
              Showing {Math.min(visibleIncidentCount, rows.length)} of {rows.length}
            </div>
            {showIncidentLoadMore ? (
              <button className="ghostBtn" type="button" onClick={() => setVisibleIncidentCount((c) => c + PAGE_STEP)}>
                Load more
              </button>
            ) : null}
          </div>
        ) : null}
      </Card>

      {inspectingIncident ? (
        <DetailModal
          title="Issue details"
          subtitle={
            inspectingIncident.upload?.upload_id ? (
              <span className="muted">
                Intake {String(inspectingIncident.upload.upload_id).slice(0, 8)}
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
              disabled={!auth.canManageIncidents}
              onClick={() => analyzeIncident(String(inspectingIncident.incident_id))}
              title={auth.canManageIncidents ? "" : "Moderators and admins can add notes."}
            >
              Add notes
            </button>
            <button
              className="ghostBtn"
              type="button"
              disabled={!auth.canManageIncidents}
              onClick={() => retryIncident(String(inspectingIncident.incident_id))}
              title={auth.canManageIncidents ? "" : "Moderators and admins can retry issues."}
            >
              Try again
            </button>
            <button
              className="ghostBtn"
              type="button"
              disabled={!auth.canManageIncidents}
              onClick={() => archiveIncident(String(inspectingIncident.incident_id))}
              title={auth.canManageIncidents ? "" : "Moderators and admins can archive issues."}
            >
              Archive
            </button>
          </div>
          {incidentDetailError ? <div className="errorLine">{incidentDetailError}</div> : null}
          {incidentDetailLoading ? <div className="muted">Refreshing issue data...</div> : null}
          <div className="detailGrid">
            <div>
              <div className="muted">Issue ID</div>
              <div className="mono">
                {String(inspectingIncident.incident_id ?? inspectingIncident.id ?? "—").slice(0, 8)}
              </div>
            </div>
            <div>
              <div className="muted">Status</div>
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
              <div className="muted">Intake</div>
              <div className="mono">
                {inspectingIncident.upload?.upload_id
                  ? String(inspectingIncident.upload.upload_id).slice(0, 8)
                  : "—"}
              </div>
            </div>
            <div>
              <div className="muted">Run</div>
              <div className="mono">
                {inspectingIncident.job_run?.run_id
                  ? String(inspectingIncident.job_run.run_id).slice(0, 8)
                  : inspectingIncident.job_run_id
                  ? String(inspectingIncident.job_run_id).slice(0, 8)
                  : "—"}
              </div>
            </div>
            <div>
              <div className="muted">Known issue type</div>
              <div>
                {inspectingIncident.is_known
                  ? inspectingIncident.matched_known_error_name || "Known issue type"
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
              <div className="muted">Attempts</div>
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
            <div className="detailTitle">What went wrong</div>
            <div className="detailTextBlock">{inspectingIncident.error || "No details captured yet."}</div>
          </div>

          <div className="detailSection">
            <div className="detailTitle">Who or what was affected</div>
            <div className="detailTextBlock">{inspectingIncident.impact_summary || "Not recorded yet."}</div>
          </div>

          <div className="detailSection">
            <div className="detailTitle">What caused it</div>
            <div className="detailTextBlock">{inspectingIncident.root_cause || "Not recorded yet."}</div>
          </div>

          <div className="detailSection">
            <div className="detailTitle">How we fixed it</div>
            <div className="detailTextBlock">{inspectingIncident.corrective_action || "Not recorded yet."}</div>
          </div>

          <div className="detailSection">
            <div className="detailTitle">Notes</div>
            <div className="detailTextBlock">{inspectingIncident.analysis_notes || "Not recorded yet."}</div>
          </div>

  ***

          {inspectingIncident.suggested_fix ? (
            <div className="detailSection">
              <div className="detailTitle">Suggested next step</div>
              <div className="detailTextBlock">
                {typeof inspectingIncident.suggested_fix === "string"
                  ? inspectingIncident.suggested_fix
                  : JSON.stringify(inspectingIncident.suggested_fix, null, 2)}
              </div>
            </div>
          ) : null}

          <div className="detailSection">
            <div className="detailTitle">How it was resolved</div>
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
              <div className="muted">Timeline will populate as the issue progresses.</div>
            )}
          </div>
        </DetailModal>
      ) : null}

      {showCreate ? (
        <div className="modalBackdrop" onClick={() => setShowCreate(false)}>
          <div className="modal" onClick={(e) => e.stopPropagation()}>
            <div className="modalTitle">Create batch issue</div>

            <div className="field">
              <label className="label">Intake ID</label>
              <input
                className="input"
                value={newUploadId}
                onChange={(e) => setNewUploadId(e.target.value)}
                placeholder="Paste the Intake ID from the Batch Intake page"
              />
            </div>

            <div className="field">
              <label className="label">What happened</label>
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
                placeholder="e.g. data check, missing file"
              />
            </div>

            <div className="field">
              <label className="label">Who or what was affected (optional)</label>
              <textarea
                className="textarea"
                value={newImpactSummary}
                onChange={(e) => setNewImpactSummary(e.target.value)}
                placeholder="Describe who was impacted"
              />
            </div>

            <div className="field">
              <label className="label">Notes (optional)</label>
              <textarea
                className="textarea"
                value={newAnalysisNotes}
                onChange={(e) => setNewAnalysisNotes(e.target.value)}
                placeholder="Any immediate observations?"
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
        setErr("Couldn't load tasks. Please refresh.")
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
      alert("Create task failed. Please try again.")
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
      alert("Assign failed. Please try again.")
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
      alert("Resolve failed. Please try again.")
    } finally {
      setActionBusy(null)
    }
  }

  async function analyzeLinkedIncident(incidentId?: string) {
    if (!incidentId) {
      alert("Ticket is not linked to an issue.")
      return
    }
    const severity = prompt("Severity (low/medium/high/critical):", "medium") || "medium"
    const impact = prompt("Who or what was affected (optional):") || ""
    const analysis = prompt("Notes:", "") || ""
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
      alert("Ticket is not linked to an issue.")
      return
    }
    const notes = prompt("Retry notes (optional):") || ""
    try {
      setActionBusy(incidentId)
      await postJSON(`/api/incidents/${incidentId}/retry/`, { notes })
      setRefreshTick((x) => x + 1)
    } catch {
      alert("Retry failed. Please try again.")
    } finally {
      setActionBusy(null)
    }
  }

  return (
    <div className="page">
      <div className="pageHeader">
        <div>
          <div className="pageTitle">Batch tasks</div>
          <div className="pageSub">Track follow-up tasks tied to batch issues and mark them complete.</div>
        </div>

        <div style={{ display: "flex", gap: 10 }}>
          <button className="ghostBtn" type="button" onClick={() => setRefreshTick((x) => x + 1)}>
            Refresh
          </button>
          <button className="primaryBtnSmall" type="button" onClick={() => setShowCreate(true)}>
            + Create batch task
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
          <div className="muted">Latest batch tasks</div>
          <div className="muted">Auto-updating</div>
        </div>

        <div className="table">
          <div className="tHead">
            <div>Task</div>
            <div>Status</div>
            <div>Created by</div>
            <div>Assignee</div>
            <div>Issue</div>
            <div>Title</div>
            <div>Actions</div>
          </div>

          {rows.length === 0 ? (
            <div className="muted" style={{ paddingTop: 10 }}>
              No tasks yet.
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
                        alert("Ticket is not linked to an issue.")
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
                        alert("Ticket is not linked to an issue.")
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
            <div className="modalTitle">Create batch task</div>

            <div className="field">
              <label className="label">Issue ID (optional)</label>
              <input className="input" value={newIncidentId} onChange={(e) => setNewIncidentId(e.target.value)} placeholder="Paste Issue ID (optional)" />
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
  const [reportFormat, setReportFormat] = useState<DownloadFormat>("csv")

  async function download() {
    const id = jobRunId.trim()
    if (!id) {
      setHint("Enter a Run ID first.")
      return
    }
    setHint(null)
    setLoading(true)
    try {
      const run = await apiClient.jobRuns.get(id)
      const uploadId = run?.upload?.upload_id || run?.upload_id || run?.upload
      if (!uploadId) {
        setHint("This run has no file linked yet.")
        return
      }
      if (run.status !== "success") {
        if (run.status === "failed") {
        setHint("This run failed. Check Batch Issues for details before downloading.")
        return
      }
      setHint("This run is still processing. Please wait for it to finish.")
      return
      }
      await ensureReportDownloaded(uploadId, { format: reportFormat })
      setHint(null)
    } catch (e: any) {
      if (typeof e?.message === "string") {
        setHint(e.message)
      } else if (e?.response?.status === 404) {
        setHint("Run ID not found. Use one from the Batch Runs tab.")
      } else {
        setHint("Failed to download report. Ensure the run is complete and try again.")
      }
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="page">
      <div className="pageHeader">
        <div>
          <div className="pageTitle">Batch reports</div>
          <div className="pageSub">Download the batch report for an intake file in CSV or PDF.</div>
        </div>
      </div>

      <Card>
        <div className="sectionTitle">Download a report</div>
        <div className="muted" style={{ marginTop: 8 }}>
          Paste the Run ID from Batch Runs and BatchOps will fetch the related report.
        </div>

        <div className="field" style={{ marginTop: 12 }}>
          <label className="label">Run ID</label>
          <input
            className="input"
            value={jobRunId}
            onChange={(e) => setJobRunId(e.target.value)}
            placeholder="Example: f4a6c63e-9d1b..."
          />
        </div>

        <div className="field" style={{ marginTop: 12 }}>
          <label className="label">Format</label>
          <Segments
            value={reportFormat}
            onChange={(value) => setReportFormat(value as DownloadFormat)}
            options={[
              { value: "csv", label: "CSV" },
              { value: "pdf", label: "PDF" },
            ]}
          />
        </div>

        {hint ? <div className="errorLine">{hint}</div> : null}

        <button className="primaryBtnSmall" type="button" onClick={download} disabled={loading}>
          {loading ? "Fetching..." : `Download ${reportFormat.toUpperCase()}`}
        </button>
      </Card>

      <Card>
        <div className="sectionTitle">Quick steps</div>
        <ol className="steps">
          <li>Go to Batch Intake.</li>
          <li>Upload a CSV, Excel, or PDF.</li>
          <li>Wait until the status shows "published".</li>
          <li>Select "Download report".</li>
        </ol>
      </Card>
    </div>
  )
}

function JobsPage() {
  const auth = useAuth()
  const [jobs, setJobs] = useState<any[]>([])
  const [loading, setLoading] = useState(false)
  const [jobError, setJobError] = useState<string | null>(null)
  const [newJobName, setNewJobName] = useState("")
  const [newJobCallable, setNewJobCallable] = useState("")
  const [newJobCron, setNewJobCron] = useState("")
  const [newJobArgs, setNewJobArgs] = useState("")
  const [scheduleDrafts, setScheduleDrafts] = useState<Record<string, string>>({})
  const [savingJobId, setSavingJobId] = useState<string | null>(null)
  const [jobQuery, setJobQuery] = useState("")
  const [showAllJobs, setShowAllJobs] = useState(false)

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
      setJobError("Couldn't load schedules. Please refresh.")
      setJobs([])
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    loadJobs()
  }, [])

  const filteredJobs = jobs.filter((job) => {
    if (!jobQuery.trim()) return true
    const q = jobQuery.trim().toLowerCase()
    const name = String(job.name ?? "").toLowerCase()
    const callable = String(job.config?.callable ?? "").toLowerCase()
    return name.includes(q) || callable.includes(q)
  })
  const visibleJobs = showAllJobs ? filteredJobs : filteredJobs.slice(0, 6)

  async function createJob() {
    if (!auth.canEditSchedules) {
      setJobError("Only admins can create batch schedules.")
      return
    }
    if (!newJobName.trim() || !newJobCallable.trim()) {
      setJobError("Task name and action are required.")
      return
    }
    let args: any[] = []
    if (newJobArgs.trim()) {
      try {
        const parsed = JSON.parse(newJobArgs)
        args = Array.isArray(parsed) ? parsed : [parsed]
      } catch {
        setJobError("If you provide details, they must be valid JSON (e.g. [\"foo\", 123]).")
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
      setJobError(e?.response?.data?.error || "Create schedule failed. Please try again.")
    } finally {
      setSavingJobId(null)
    }
  }

  async function saveSchedule(jobId: string) {
    if (!auth.canEditSchedules) {
      setJobError("Only admins can update schedules.")
      return
    }
    const cron = scheduleDrafts[jobId] ?? ""
    try {
      setSavingJobId(jobId)
      await apiClient.jobs.update(jobId, { schedule_cron: cron.trim() || null })
      setJobError(null)
      loadJobs()
    } catch {
      setJobError("Failed to update the schedule. Please check the format.")
    } finally {
      setSavingJobId(null)
    }
  }

  async function triggerJob(jobId: string) {
    try {
      setSavingJobId(jobId)
      await apiClient.jobs.trigger(jobId, {})
      setJobError(null)
      alert("Task started successfully.")
    } catch {
      setJobError("Failed to start the task.")
    } finally {
      setSavingJobId(null)
    }
  }

  return (
    <div className="page">
      <div className="pageHeader">
        <div>
          <div className="pageTitle">Batch schedules</div>
          <div className="pageSub">Set batch jobs to run automatically and start them anytime.</div>
          {!auth.canEditSchedules ? (
            <div className="muted" style={{ fontSize: 12 }}>
              You can run schedules, but only admins can create or edit them.
            </div>
          ) : null}
        </div>
      </div>

      <div className="grid2">
        {auth.canEditSchedules ? (
          <Card>
            <div className="formHeader">
              <div className="formTitle">New batch schedule</div>
            </div>
            <div className="field">
              <label className="label">Batch job name</label>
              <input className="input" value={newJobName} onChange={(e) => setNewJobName(e.target.value)} placeholder="e.g. nightly_reports" />
            </div>
            <div className="field">
              <label className="label">Batch action to run (advanced)</label>
              <input
                className="input"
                value={newJobCallable}
                onChange={(e) => setNewJobCallable(e.target.value)}
                placeholder="module.path:function_name"
              />
            </div>
            <div className="field">
              <label className="label">Optional details (JSON)</label>
              <textarea
                className="textarea"
                value={newJobArgs}
                onChange={(e) => setNewJobArgs(e.target.value)}
                placeholder='e.g. ["arg1", 42]'
              />
            </div>
            <div className="field">
              <label className="label">Batch schedule (optional)</label>
              <input className="input" value={newJobCron} onChange={(e) => setNewJobCron(e.target.value)} placeholder="*/5 * * * *" />
            </div>
            <button className="primaryBtn" type="button" disabled={savingJobId === "create"} onClick={createJob}>
              {savingJobId === "create" ? "Creating..." : "Create batch schedule"}
            </button>
          </Card>
        ) : (
          <Card>
            <div className="formHeader">
              <div className="formTitle">New batch schedule</div>
              <div className="muted" style={{ fontSize: 12 }}>Admin only</div>
            </div>
            <div className="muted">
              Ask an admin to add new schedules. You can still run any existing schedule from the list.
            </div>
          </Card>
        )}

        <Card>
          <div className="formHeader">
            <div>
              <div className="formTitle">Batch schedules</div>
              <div className="muted" style={{ fontSize: 12 }}>
                {filteredJobs.length} total
              </div>
            </div>
            <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
              <input
                className="input"
                style={{ minWidth: 180 }}
                value={jobQuery}
                onChange={(e) => setJobQuery(e.target.value)}
                placeholder="Search batch schedules"
              />
              <button className="ghostBtn" type="button" onClick={loadJobs} disabled={loading}>
                {loading ? "Loading..." : "Refresh"}
              </button>
            </div>
          </div>
          {jobError ? <div className="errorLine">{jobError}</div> : null}
          {filteredJobs.length === 0 ? (
            <div className="muted">No batch schedules configured yet.</div>
          ) : (
            <div className="jobList">
              {visibleJobs.map((job: any) => {
                const jobId = String(job.id)
                return (
                  <div className="jobRow" key={jobId}>
                    <div>
                      <div className="muted">Name</div>
                      <div className="mono">{job.name}</div>
                      <div className="muted" style={{ fontSize: 12 }}>Batch job</div>
                    </div>
                    <div>
                      <div className="muted">Action</div>
                      <div className="mono">{job.config?.callable || "—"}</div>
                    </div>
                    <div>
                      <div className="muted">Schedule</div>
                      <input
                        className="input"
                        value={scheduleDrafts[jobId] ?? ""}
                        onChange={(e) => setScheduleDrafts((draft) => ({ ...draft, [jobId]: e.target.value }))}
                        placeholder="Schedule format (cron)"
                        disabled={!auth.canEditSchedules}
                        title={auth.canEditSchedules ? "" : "Admins can edit schedules."}
                      />
                    </div>
                    <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                      <button
                        className="ghostBtn"
                        type="button"
                        disabled={savingJobId === jobId || !auth.canEditSchedules}
                        onClick={() => saveSchedule(jobId)}
                        title={auth.canEditSchedules ? "" : "Admins can edit schedules."}
                      >
                        Save
                      </button>
                      <button
                        className="ghostBtn"
                        type="button"
                        disabled={savingJobId === jobId}
                        onClick={() => triggerJob(jobId)}
                      >
                        Run batch now
                      </button>
                    </div>
                  </div>
                )
              })}
              {filteredJobs.length > 6 ? (
                <button
                  className="ghostBtn"
                  type="button"
                  onClick={() => setShowAllJobs((prev) => !prev)}
                  style={{ marginTop: 6, alignSelf: "flex-start" }}
                >
                  {showAllJobs ? "Show fewer schedules" : `Show all (${filteredJobs.length})`}
                </button>
              ) : null}
            </div>
          )}
        </Card>
      </div>
    </div>
  )
}

/** ---------------- APP SHELL ---------------- */

export default function App() {
  const [authUser, setAuthUser] = useState<AuthUser | null>(null)
  const [authLoading, setAuthLoading] = useState(true)
  const [authBusy, setAuthBusy] = useState(false)
  const [authError, setAuthError] = useState<string | null>(null)
  const [authUsername, setAuthUsername] = useState("")
  const [authPassword, setAuthPassword] = useState("")
  const [showAuthPassword, setShowAuthPassword] = useState(false)
  const [showReset, setShowReset] = useState(false)
  const [resetStep, setResetStep] = useState<"request" | "verify" | "done">("request")
  const [resetUsername, setResetUsername] = useState("")
  const [resetId, setResetId] = useState("")
  const [resetCode, setResetCode] = useState("")
  const [resetPassword, setResetPassword] = useState("")
  const [resetConfirm, setResetConfirm] = useState("")
  const [resetError, setResetError] = useState<string | null>(null)
  const [resetBusy, setResetBusy] = useState(false)
  const [resetExpires, setResetExpires] = useState("")
  const [showVerify, setShowVerify] = useState(false)
  const [verifyStep, setVerifyStep] = useState<"request" | "confirm" | "done">("request")
  const [verifyUsername, setVerifyUsername] = useState("")
  const [verifyId, setVerifyId] = useState("")
  const [verifyCode, setVerifyCode] = useState("")
  const [verifyError, setVerifyError] = useState<string | null>(null)
  const [verifyBusy, setVerifyBusy] = useState(false)
  const [verifyExpires, setVerifyExpires] = useState("")
  const [active, setActive] = useState<NavKey>(() => {
    if (typeof window === "undefined") return "dashboard"
    const saved = localStorage.getItem(LAST_NAV_KEY)
    return NAV.some((item) => item.key === saved) ? (saved as NavKey) : "dashboard"
  })
  const [systemOnline, setSystemOnline] = useState(true)

  useEffect(() => {
    let mounted = true
    async function loadMe() {
      if (typeof window === "undefined") {
        setAuthLoading(false)
        return
      }
      const token = localStorage.getItem("jwt_token")
      if (!token) {
        setAuthLoading(false)
        return
      }
      setApiToken(token)
      try {
        const res = await fetch(`${baseURL()}/api/auth/me`, {
          headers: { Authorization: `Token ${token}` },
        })
        if (!res.ok) throw new Error("Auth failed")
        const data = await res.json()
        if (!mounted) return
        setAuthUser(data.user ?? null)
      } catch {
        if (!mounted) return
        try {
          localStorage.removeItem("jwt_token")
        } catch {
          // ignore
        }
        setApiToken(null)
        setAuthUser(null)
      } finally {
        if (!mounted) return
        setAuthLoading(false)
      }
    }
    loadMe()
    return () => {
      mounted = false
    }
  }, [])

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

  const role = resolveRole(authUser)
  const authValue = useMemo<AuthContextValue>(
    () => ({
      user: authUser,
      role,
      canEditSchedules: role === "admin",
      canRunSchedules: true,
      canManageIncidents: role !== "user",
      canCreateIncidents: role !== "user",
    }),
    [authUser, role],
  )

  async function handleLogin() {
    if (!authUsername.trim() || !authPassword.trim()) {
      setAuthError("Enter both username and password.")
      return
    }
    setAuthBusy(true)
    setAuthError(null)
    try {
      const res = await fetch(`${baseURL()}/api/auth/login`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username: authUsername.trim(), password: authPassword }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) {
        if (data?.verification_required) {
          setAuthError("Email not verified. Please verify to continue.")
          setShowVerify(true)
          setShowReset(false)
          setVerifyStep("request")
          setVerifyUsername(authUsername.trim())
          setVerifyId("")
          setVerifyCode("")
          setVerifyError(null)
          setVerifyExpires("")
          return
        }
        throw new Error(data?.error || "Login failed.")
      }
      if (data?.token) {
        localStorage.setItem("jwt_token", data.token)
        setApiToken(data.token)
      }
      setAuthUser(data.user ?? null)
      setAuthPassword("")
    } catch (err: any) {
      setAuthError(err?.message || "Login failed.")
    } finally {
      setAuthBusy(false)
    }
  }

  async function handleLogout() {
    const token = localStorage.getItem("jwt_token")
    try {
      if (token) {
        await fetch(`${baseURL()}/api/auth/logout`, {
          method: "POST",
          headers: { Authorization: `Token ${token}` },
        })
      }
    } catch {
      // ignore logout errors
    } finally {
      try {
        localStorage.removeItem("jwt_token")
      } catch {
        // ignore
      }
      setApiToken(null)
      setAuthUser(null)
      setActive("dashboard")
    }
  }

  function openReset() {
    setShowReset(true)
    setShowVerify(false)
    setResetStep("request")
    setResetUsername(authUsername.trim() || "")
    setResetId("")
    setResetCode("")
    setResetPassword("")
    setResetConfirm("")
    setResetError(null)
    setResetExpires("")
  }

  function closeReset() {
    setShowReset(false)
    setResetStep("request")
    setResetUsername("")
    setResetId("")
    setResetCode("")
    setResetPassword("")
    setResetConfirm("")
    setResetError(null)
    setResetExpires("")
  }

  function openVerify() {
    setShowVerify(true)
    setShowReset(false)
    setVerifyStep("request")
    setVerifyUsername(authUsername.trim() || "")
    setVerifyId("")
    setVerifyCode("")
    setVerifyError(null)
    setVerifyExpires("")
  }

  function closeVerify() {
    setShowVerify(false)
    setVerifyStep("request")
    setVerifyUsername("")
    setVerifyId("")
    setVerifyCode("")
    setVerifyError(null)
    setVerifyExpires("")
  }

  async function handleResetRequest() {
    if (!resetUsername.trim()) {
      setResetError("Enter your username to continue.")
      return
    }
    setResetBusy(true)
    setResetError(null)
    try {
      const res = await fetch(`${baseURL()}/api/auth/forgot`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username: resetUsername.trim() }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) {
        throw new Error(data?.error || "Could not start reset.")
      }
      setResetId(data.reset_id || "")
      setResetExpires(data.expires_at || "")
      setResetStep("verify")
    } catch (err: any) {
      setResetError(err?.message || "Could not start reset.")
    } finally {
      setResetBusy(false)
    }
  }

  async function handleResetConfirm() {
    if (!resetCode.trim() || !resetPassword.trim() || !resetConfirm.trim()) {
      setResetError("Fill in the reset code and both password fields.")
      return
    }
    if (resetPassword !== resetConfirm) {
      setResetError("Passwords do not match.")
      return
    }
    setResetBusy(true)
    setResetError(null)
    try {
      const res = await fetch(`${baseURL()}/api/auth/reset`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          reset_id: resetId,
          code: resetCode.trim(),
          new_password: resetPassword,
        }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) {
        throw new Error(data?.error || "Reset failed.")
      }
      setResetStep("done")
      setAuthUsername(resetUsername.trim())
      setResetPassword("")
      setResetConfirm("")
    } catch (err: any) {
      setResetError(err?.message || "Reset failed.")
    } finally {
      setResetBusy(false)
    }
  }

  async function handleVerifyRequest() {
    if (!verifyUsername.trim()) {
      setVerifyError("Enter your username to continue.")
      return
    }
    setVerifyBusy(true)
    setVerifyError(null)
    try {
      const res = await fetch(`${baseURL()}/api/auth/verify/send`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username: verifyUsername.trim() }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) {
        throw new Error(data?.error || "Could not send verification.")
      }
      if (data?.status === "already_verified") {
        setVerifyStep("done")
        return
      }
      setVerifyId(data.verification_id || "")
      setVerifyExpires(data.expires_at || "")
      setVerifyStep("confirm")
    } catch (err: any) {
      setVerifyError(err?.message || "Could not send verification.")
    } finally {
      setVerifyBusy(false)
    }
  }

  async function handleVerifyConfirm() {
    if (!verifyCode.trim()) {
      setVerifyError("Enter the verification code.")
      return
    }
    setVerifyBusy(true)
    setVerifyError(null)
    try {
      const res = await fetch(`${baseURL()}/api/auth/verify/confirm`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          verification_id: verifyId,
          username: verifyUsername.trim(),
          code: verifyCode.trim(),
        }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) {
        throw new Error(data?.error || "Verification failed.")
      }
      setVerifyStep("done")
    } catch (err: any) {
      setVerifyError(err?.message || "Verification failed.")
    } finally {
      setVerifyBusy(false)
    }
  }

  if (authLoading) {
    return (
      <div className="authShell">
        <div className="authCard">
          <div className="authTitle">Checking session...</div>
          <div className="muted">One moment while we verify access.</div>
        </div>
      </div>
    )
  }

  if (!authUser) {
    return (
      <div className="authShell">
        <div className="authBackdrop">
          <div className="authBlob authBlobOne" />
          <div className="authBlob authBlobTwo" />
        </div>
        <div className="authLayout">
          <div className="authVisual">
            <div className="authBrandRow">
              <div className="authMark">BO</div>
              <div>
                <div className="authBrandName">BatchOps</div>
                <div className="authBrandSub">School data, made simple</div>
              </div>
            </div>
            <h1 className="authHeadline">All your school files, in one calm workspace.</h1>
            <p className="authLead">
              Upload, check, and share results without the technical noise. Everyone sees what they need, nothing more.
            </p>
            <div className="authBadgeRow">
              <span className="authBadge">Access levels</span>
              <span className="authBadge">Clear status</span>
              <span className="authBadge">Safe records</span>
            </div>
            <div className="authFlow">
              <div className="authStep">
                <div className="authStepBadge">1</div>
                <div>
                  <div className="authStepTitle">Collect</div>
                  <div className="authStepText">Departments send files to one place, on schedule.</div>
                </div>
              </div>
              <div className="authStep">
                <div className="authStepBadge">2</div>
                <div>
                  <div className="authStepTitle">Check</div>
                  <div className="authStepText">We look for missing data and keep everyone informed.</div>
                </div>
              </div>
              <div className="authStep">
                <div className="authStepBadge">3</div>
                <div>
                  <div className="authStepTitle">Share</div>
                  <div className="authStepText">Download clean reports when everything is ready.</div>
                </div>
              </div>
            </div>
            <ul className="authChecklist">
              <li>Admins set schedules and see everything.</li>
              <li>Moderators review issues and keep things on track.</li>
              <li>Users upload files and watch progress.</li>
            </ul>
          </div>
          <div className="authPanel">
            <div className="authCard">
              {showReset ? (
                <>
                  <div className="authCardHeader">
                    <div className="authTitle">Reset password</div>
                    <div className="authSubtitle">We will help you get back into your account.</div>
                  </div>
                  {resetError ? <div className="errorLine">{resetError}</div> : null}
                  {resetStep === "request" ? (
                    <>
                      <div className="field">
                        <label className="label">Username</label>
                        <input
                          className="input"
                          value={resetUsername}
                          onChange={(e) => setResetUsername(e.target.value)}
                          placeholder="e.g. ops_admin"
                        />
                      </div>
                      <button className="primaryBtn authSubmit" type="button" disabled={resetBusy} onClick={handleResetRequest}>
                        {resetBusy ? "Sending..." : "Send reset code"}
                      </button>
                      <div className="authNote">We will email a reset code to the address on file.</div>
                    </>
                  ) : null}
                  {resetStep === "verify" ? (
                    <>
                      <div className="field">
                        <label className="label">Reset code</label>
                        <input
                          className="input"
                          value={resetCode}
                          onChange={(e) => setResetCode(e.target.value)}
                          placeholder="6-digit code"
                        />
                      </div>
                      <div className="field">
                        <label className="label">New password</label>
                        <input
                          className="input"
                          type="password"
                          value={resetPassword}
                          onChange={(e) => setResetPassword(e.target.value)}
                          placeholder="Create a new password"
                        />
                      </div>
                      <div className="field">
                        <label className="label">Confirm password</label>
                        <input
                          className="input"
                          type="password"
                          value={resetConfirm}
                          onChange={(e) => setResetConfirm(e.target.value)}
                          placeholder="Repeat the new password"
                        />
                      </div>
                      {resetExpires ? (
                        <div className="authNote">Code expires at {fmtDate(resetExpires)}.</div>
                      ) : null}
                      <button className="primaryBtn authSubmit" type="button" disabled={resetBusy} onClick={handleResetConfirm}>
                        {resetBusy ? "Updating..." : "Update password"}
                      </button>
                    </>
                  ) : null}
                  {resetStep === "done" ? (
                    <>
                      <div className="authSuccess">Your password is updated. You can sign in now.</div>
                      <button className="primaryBtn authSubmit" type="button" onClick={closeReset}>
                        Return to sign in
                      </button>
                    </>
                  ) : null}
                  {resetStep !== "done" ? (
                    <button className="authLink" type="button" onClick={closeReset}>
                      Back to sign in
                    </button>
                  ) : null}
                </>
              ) : showVerify ? (
                <>
                  <div className="authCardHeader">
                    <div className="authTitle">Verify email</div>
                    <div className="authSubtitle">Confirm your email so you can sign in.</div>
                  </div>
                  {verifyError ? <div className="errorLine">{verifyError}</div> : null}
                  {verifyStep === "request" ? (
                    <>
                      <div className="field">
                        <label className="label">Username</label>
                        <input
                          className="input"
                          value={verifyUsername}
                          onChange={(e) => setVerifyUsername(e.target.value)}
                          placeholder="e.g. ops_admin"
                        />
                      </div>
                      <button className="primaryBtn authSubmit" type="button" disabled={verifyBusy} onClick={handleVerifyRequest}>
                        {verifyBusy ? "Sending..." : "Send verification code"}
                      </button>
                      <div className="authNote">We will email a verification code to your address.</div>
                    </>
                  ) : null}
                  {verifyStep === "confirm" ? (
                    <>
                      <div className="field">
                        <label className="label">Verification code</label>
                        <input
                          className="input"
                          value={verifyCode}
                          onChange={(e) => setVerifyCode(e.target.value)}
                          placeholder="6-digit code"
                        />
                      </div>
                      {verifyExpires ? (
                        <div className="authNote">Code expires at {fmtDate(verifyExpires)}.</div>
                      ) : null}
                      <button className="primaryBtn authSubmit" type="button" disabled={verifyBusy} onClick={handleVerifyConfirm}>
                        {verifyBusy ? "Verifying..." : "Verify email"}
                      </button>
                    </>
                  ) : null}
                  {verifyStep === "done" ? (
                    <>
                      <div className="authSuccess">Your email is verified. You can sign in now.</div>
                      <button className="primaryBtn authSubmit" type="button" onClick={closeVerify}>
                        Return to sign in
                      </button>
                    </>
                  ) : null}
                  {verifyStep !== "done" ? (
                    <button className="authLink" type="button" onClick={closeVerify}>
                      Back to sign in
                    </button>
                  ) : null}
                </>
              ) : (
                <>
                  <div className="authCardHeader">
                    <div className="authTitle">Sign in</div>
                    <div className="authSubtitle">Use your account to open your workspace; sign in with your role.</div>
                  </div>
                  {authError ? <div className="errorLine">{authError}</div> : null}
                  <div className="field">
                    <label className="label">Username</label>
                    <input
                      className="input"
                      value={authUsername}
                      onChange={(e) => setAuthUsername(e.target.value)}
                      placeholder="e.g. ops_admin"
                    />
                  </div>
                  <div className="field">
                    <label className="label">Password</label>
                    <div className="inputWithAction">
                      <input
                        className="input"
                        type={showAuthPassword ? "text" : "password"}
                        value={authPassword}
                        onChange={(e) => setAuthPassword(e.target.value)}
                        placeholder="••••••••"
                      />
                      <button
                        className="ghostBtn ghostBtnSmall"
                        type="button"
                        onClick={() => setShowAuthPassword((v) => !v)}
                      >
                        {showAuthPassword ? "Hide" : "Show"}
                      </button>
                    </div>
                  </div>
                  <button className="primaryBtn authSubmit" type="button" disabled={authBusy} onClick={handleLogin}>
                    {authBusy ? "Signing in..." : "Sign in"}
                  </button>
                  <div className="authHint">Admins can add new accounts for the team.</div>
                  <button className="authLink" type="button" onClick={openReset}>
                    Forgot password?
                  </button>
                  <button className="authLink" type="button" onClick={openVerify}>
                    Verify email
                  </button>
                </>
              )}
            </div>
            <div className="authFootnote">
              Need access? Ask your admin to add you.
            </div>
          </div>
        </div>
      </div>
    )
  }

  return (
    <AuthContext.Provider value={authValue}>
      <div className="shell">
        <aside className="sidebar">
          <div className="brand">
            <div className="brandMark">BO</div>
            <div>
              <div className="brandName">BatchOps</div>
              <div className="brandSub">School workflows</div>
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
            <div className="runtimeTitle">Powered by BatchOps</div>
            <div className="runtimeText">Secure processing and background jobs</div>
            <div className="runtimeText">Health monitoring active</div>
          </div>
        </aside>

        <main className="main">
          <div className="topbar">
            <div className="topbarLeft" />
            <div className="topbarRight">
              <div className="roleBadge">{role.toUpperCase()}</div>
              <div className="userMeta">{authUser.username}</div>
              <button className="ghostBtn ghostBtnSmall" type="button" onClick={handleLogout}>
                Sign out
              </button>
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
    </AuthContext.Provider>
  )
}
