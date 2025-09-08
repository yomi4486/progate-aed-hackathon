import type { Lang } from './common';

export interface Document {
  id: string;
  url: string;
  site: string;
  lang: Lang;
  title?: string;
  body?: string;
  published_at?: string;
  crawled_at?: string;
  content_hash?: string;
  popularity_score?: number;
  s3_key?: string;
  embedding?: Array<number>;
}

export interface IndexReadyDocument {
  id: string;
  url: string;
  site: string;
  lang: Lang;
  title: string;
  snippet?: string;
  published_at?: string;
  crawled_at?: string;
  content_hash?: string;
  popularity_score?: number;
  embedding?: Array<number>;
}
