import type { ErrorResponse, HealthStatus } from './types/common';
import type { SearchResponse, SuggestResponse } from './types/search';

export interface RPCClient {
  search(query?: string, page?: number, size?: number, lang?: "ja" | "en", site?: string, sort?: string): Promise<SearchResponse | ErrorResponse>;
  suggest(q?: string, size?: number): Promise<SuggestResponse | ErrorResponse>;
  health(): Promise<HealthStatus | ErrorResponse>;
}

export class RPCClientImpl implements RPCClient {
  private baseUrl: string;

  constructor(baseUrl: string) {
    this.baseUrl = baseUrl;
  }

  async search(query?: string, page?: number, size?: number, lang?: "ja" | "en", site?: string, sort?: string): Promise<SearchResponse | ErrorResponse> {
    const response = await fetch(`${this.baseUrl}/rpc/search?${[(query !== undefined ? 'query=' + encodeURIComponent(query) : ''), (page !== undefined ? 'page=' + encodeURIComponent(page) : ''), (size !== undefined ? 'size=' + encodeURIComponent(size) : ''), (lang !== undefined ? 'lang=' + encodeURIComponent(lang) : ''), (site !== undefined ? 'site=' + encodeURIComponent(site) : ''), (sort !== undefined ? 'sort=' + encodeURIComponent(sort) : '')].filter(Boolean).join('&')}`);
    if (!response.ok) {
      return response.json() as Promise<ErrorResponse>;
    }
    return response.json() as Promise<SearchResponse>;
  }
  async suggest(q?: string, size?: number): Promise<SuggestResponse | ErrorResponse> {
    const response = await fetch(`${this.baseUrl}/rpc/suggest?${[(q !== undefined ? 'q=' + encodeURIComponent(q) : ''), (size !== undefined ? 'size=' + encodeURIComponent(size) : '')].filter(Boolean).join('&')}`);
    if (!response.ok) {
      return response.json() as Promise<ErrorResponse>;
    }
    return response.json() as Promise<SuggestResponse>;
  }
  async health(): Promise<HealthStatus | ErrorResponse> {
    const response = await fetch(`${this.baseUrl}/rpc/health`);
    if (!response.ok) {
      return response.json() as Promise<ErrorResponse>;
    }
    return response.json() as Promise<HealthStatus>;
  }
}
