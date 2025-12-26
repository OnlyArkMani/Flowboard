import axios, { type AxiosError, type AxiosInstance, type AxiosResponse } from "axios"

const baseURL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000"

const DEFAULT_PAGE_SIZE = 5000

function withPageSize(params?: any) {
  return { page_size: DEFAULT_PAGE_SIZE, ...(params ?? {}) }
}

const api: AxiosInstance = axios.create({
  baseURL,
  headers: {
    "Content-Type": "application/json",
  },
})

// Interceptor: attach JWT from localStorage
api.interceptors.request.use((config) => {
  const token = localStorage.getItem("jwt_token")
  if (token) {
    config.headers.Authorization = `Token ${token}`
  }
  return config
})

// Interceptor: handle errors
api.interceptors.response.use(
  (response) => response,
  (error: AxiosError) => {
    console.error("[API Error]", error.response?.status, error.message)
    return Promise.reject(error)
  },
)

/**
 * Django REST Framework pagination shape
 * (Default pagination uses "results", not "data")
 */
export type DRFPage<T> = {
  count: number
  next: string | null
  previous: string | null
  results: T[]
}

/** Helper: unwrap AxiosResponse<T> -> T */
async function unwrap<T>(p: Promise<AxiosResponse<T>>): Promise<T> {
  const res = await p
  return res.data
}

/** Helper: unwrap AxiosResponse<DRFPage<T>> -> DRFPage<T> */
async function unwrapPage<T>(p: Promise<AxiosResponse<DRFPage<T>>>): Promise<DRFPage<T>> {
  const res = await p
  return res.data
}

/** Helper: unwrap AxiosResponse<DRFPage<T>> -> T[] (results only) */
async function unwrapResults<T>(p: Promise<AxiosResponse<DRFPage<T>>>): Promise<T[]> {
  const page = await unwrapPage<T>(p)
  return page.results
}

export const apiClient = {
  uploads: {
    // returns array directly (so UI doesn't do page.data / page.results)
    list: (params?: any) => unwrapResults<any>(api.get("/api/uploads/", { params: withPageSize(params) })),
    get: (id: string) => unwrap<any>(api.get(`/api/uploads/${id}/`)),

    upload: (formData: FormData) =>
      unwrap<any>(
        api.post("/api/uploads/", formData, {
          headers: { "Content-Type": "multipart/form-data" },
        }),
      ),

    retry: (uploadId: string) => unwrap<any>(api.post(`/api/uploads/${uploadId}/retry/`)),
  },

  jobs: {
    list: (params?: any) => unwrapResults<any>(api.get("/api/jobs/", { params })),
    create: (data: any) => unwrap<any>(api.post("/api/jobs/", data)),
    update: (id: string, data: any) => unwrap<any>(api.patch(`/api/jobs/${id}/`, data)),
    trigger: (id: string, data?: any) => unwrap<any>(api.post(`/api/jobs/${id}/trigger/`, data)),
  },

  jobRuns: {
    list: (params?: any) => unwrapResults<any>(api.get("/api/job-runs/", { params: withPageSize(params) })),
    get: (id: string) => unwrap<any>(api.get(`/api/job-runs/${id}/`)),
  },

  incidents: {
    list: (params?: any) => unwrapResults<any>(api.get("/api/incidents/", { params: withPageSize(params) })),
    get: (id: string) => unwrap<any>(api.get(`/api/incidents/${id}/`)),
    create: (data: any) => unwrap<any>(api.post("/api/incidents/", data)),
    update: (id: string, data: any) => unwrap<any>(api.patch(`/api/incidents/${id}/`, data)),
  },

  tickets: {
    list: (params?: any) => unwrapResults<any>(api.get("/api/tickets/", { params })),
    create: (data: any) => unwrap<any>(api.post("/api/tickets/", data)),
    update: (id: string, data: any) => unwrap<any>(api.patch(`/api/tickets/${id}/`, data)),
  },

  reports: {
    // NOTE: your backend returns CSV text, not JSON
    summary: (params?: any) =>
      api.get("/api/reports/summary/", {
        params,
        responseType: "text",
      }),
  },
}

export default api
