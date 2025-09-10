import type { ErrorResponse } from './types/common';
import type { SearchResponse } from './types/search';

export interface RPCClient {
  search(query: string): Promise<SearchResponse | ErrorResponse>;
}

export class RPCClientImpl implements RPCClient {
  private baseUrl: string;

  constructor(baseUrl: string) {
    this.baseUrl = baseUrl;
  }

  async search(query: string): Promise<SearchResponse | ErrorResponse> {
    const response = await fetch(`${this.baseUrl}/rpc/search?${'query=' + encodeURIComponent(query)}`);
    if (!response.ok) {
      return response.json() as Promise<ErrorResponse>;
    }
    return response.json() as Promise<SearchResponse>;
  }
}
