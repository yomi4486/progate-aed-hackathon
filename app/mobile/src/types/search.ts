import type { Highlight, Lang } from './common';

export interface SearchHit {
  id: string;
  title?: string;
  url: string;
  site: string;
  lang: Lang;
  score: number;
  snippet?: string;
  highlights?: Array<Highlight>;
}

export interface SearchQuery {
  q: string;
  page?: number;
  size?: number;
  lang?: Lang;
  site?: string;
  sort?: "_score" | "published_at" | "popularity_score";
}

export interface SearchResponse {
  total: number;
  hits: Array<SearchHit>;
  page: number;
  size: number;
}

export interface SuggestResponse {
  suggestions: Array<string>;
}
