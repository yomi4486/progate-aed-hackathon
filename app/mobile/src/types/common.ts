export type Lang = "ja" | "en";

export interface ErrorResponse {
  error: string;
  detail?: string;
}

export interface HealthStatus {
  status?: "ok" | "degraded" | "down";
  version?: string;
  opensearch?: "ok" | "down";
  cache?: "ok" | "down";
}

export interface Highlight {
  field: string;
  snippets?: Array<string>;
}

export interface Pagination {
  page?: number;
  size?: number;
}

export interface Snippet {
  text: string;
  offset?: number;
  score?: number;
}

export interface TimeWindow {
  gte?: string;
  lte?: string;
}
