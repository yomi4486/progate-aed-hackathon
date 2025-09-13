import type { Lang } from './common';

export interface CrawlResult {
  url: string;
  status_code: number;
  fetched_at: string;
  html_s3_key: string;
  error?: string;
}

export interface ParsedContent {
  url: string;
  title?: string;
  description?: string;
  body_text: string;
  lang?: Lang;
  published_at?: string;
  metadata?: Record<string, unknown>;
  parsed_s3_key: string;
}

export interface URLState {
  url_hash: string;
  domain: string;
  last_crawled?: string;
  state?: "pending" | "in_progress" | "done" | "failed";
  retries?: number;
  s3_key?: string;
}
