import { useState, useEffect, useRef } from 'react';
import { FaSearch, FaChevronLeft, FaChevronRight } from 'react-icons/fa';
import './App.css';
import { RPCClientImpl } from './rpc-client';
import logo from './assets/logo.png';
import type { SearchHit } from './types/search';

const baseURL = import.meta.env.VITE_API_BASE_URL!;

const rpc = new RPCClientImpl(baseURL);

function App() {
  // クエリパラメータから初期値を取得
  function getParam(name: string, def: string = ''): string {
    const params = new URLSearchParams(window.location.search);
    return params.get(name) ?? def;
  }
  function getParamInt(name: string, def: number): number {
    const v = getParam(name);
    const n = parseInt(v, 10);
    return isNaN(n) ? def : n;
  }

  const inputRef = useRef<HTMLInputElement>(null);
  const [inputValue, setInputValue] = useState(() => getParam('q', ''));
  const [results, setResults] = useState<SearchHit[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [searched, setSearched] = useState(() => !!getParam('q', ''));
  const [page, setPage] = useState(() => getParamInt('page', 1));
  const [total, setTotal] = useState(0);
  const [size] = useState(30);
  const [query, setQuery] = useState(() => getParam('q', ''));

  // クエリパラメータを更新
  function updateQueryParams(q: string, p: number) {
    const params = new URLSearchParams(window.location.search);
    if (q) {
      params.set('q', q);
    } else {
      params.delete('q');
    }
    if (p > 1) {
      params.set('page', String(p));
    } else {
      params.delete('page');
    }
    const newUrl = `${window.location.pathname}?${params.toString()}`;
    window.history.replaceState(null, '', newUrl);
  }

  const fetchSearch = async (q: string, p: number, s: number) => {
    updateQueryParams(q, p);
    setLoading(true);
    setError(null);
    setSearched(true);
    try {
      const res = await rpc.search(q, p, s);
      if ('error' in res) {
        setError(res.error);
        setResults([]);
        setTotal(0);
      } else {
        setResults(res.hits);
        setTotal(res.total);
      }
    } catch (err) {
      setError('検索中にエラーが発生しました');
      setResults([]);
      setTotal(0);
    } finally {
      setLoading(false);
    }
  };

  // 検索時のみinputの値を取得
  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    const q = inputRef.current?.value ?? '';
    setQuery(q);
    setPage(1);
    fetchSearch(q, 1, size);
  };

  const handlePageChange = (newPage: number) => {
    setPage(newPage);
    fetchSearch(query, newPage, size);
  };

  // 初回マウント時にクエリパラメータがあれば自動検索
  useEffect(() => {
    if (query) {
      fetchSearch(query, page, size);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // ページ数計算
  const totalPages = Math.max(1, Math.ceil(total / size));

  return (
    <div className="search-root">
      <div className={`topmenu ${(searched || loading) ? "has-query" : "webhp"}`}> 
        {!(searched || loading) ? (
          <div className="search-center">
            <header
              className="search-header"
              onClick={() => { location.href = "/"; }}
            >
              <h1 className="search-title">
                <img src={logo} alt="Progate Search" className="search-logo-toppage"/>
              </h1>
            </header>
            <form className="search-form" onSubmit={handleSearch} autoComplete="off">
              <input
                className="search-input"
                type="text"
                placeholder="検索ワードを入力..."
                ref={inputRef}
                defaultValue={query}
                value={inputValue}
                onInput={e => setInputValue((e.target as HTMLInputElement).value)}
              />
              <button className="search-btn" type="submit" disabled={loading || !inputValue}>
                <FaSearch style={{ marginRight: 6, verticalAlign: 'middle' }} />
              </button>
            </form>
          </div>
        ) : (
          <div className="search-bar-row">
            <header
              className={`search-header ${searched ? "has-query" : ""}`}
              onClick={() => { location.href = "/"; }}
            >
              <h1 className={`search-title ${searched ? "has-query" : ""}`}>
                <img src={logo} alt="Progate Search" className="search-logo" />
              </h1>
            </header>
            <form className="search-form" onSubmit={handleSearch} autoComplete="off">
              <input
                className="search-input"
                type="text"
                placeholder="検索ワードを入力..."
                ref={inputRef}
                defaultValue={query}
                value={inputValue}
                onInput={e => setInputValue((e.target as HTMLInputElement).value)}
              />
              <button className="search-btn" type="submit" disabled={loading || !inputValue}>
                <FaSearch style={{ marginRight: 6, verticalAlign: 'middle' }} />
              </button>
            </form>
          </div>
        )}
      </div>
      <main className="search-results">
        <div style={{height:'30px'}}></div>
        {searched && results.length > 0 && (
          <div className="search-results-count">
            {total} 件の結果が見つかりました
          </div>
        )}
        {loading && <div className="search-loading">検索中...</div>}
        {error && <div className="search-error">{error}</div>}
        {!loading && !error && searched && results.length === 0 && (
          <div className="search-empty">該当する結果がありません</div>
        )}
        {results.map(hit => (
          <div className="search-hit" key={hit.id}>
            <a className="search-hit-title" href={hit.url} target="_blank" rel="noopener noreferrer">
              {hit.title || hit.url}
            </a>
            <div className="search-hit-url">
              <a className="search-hit-url" href={hit.url}>{hit.url}</a>
            </div>
            {hit.snippet && <div className="search-hit-snippet" dangerouslySetInnerHTML={{ __html: hit.snippet }} />}
            <div className="search-hit-meta">
              <span className="search-hit-site">{hit.site}</span>
              <span className="search-hit-lang">{hit.lang}</span>
              <span className="search-hit-score">score: {hit.score.toFixed(2)}</span>
            </div>
          </div>
        ))}
        {/* ページネーション */}
        {totalPages > 1 && (
          <div className="search-pagination">
            <button
              className="search-pagination-btn"
              onClick={() => handlePageChange(page - 1)}
              disabled={page === 1 || loading}
            >
              <FaChevronLeft style={{ verticalAlign: 'middle' }} />
            </button>
            {Array.from({ length: totalPages }, (_, i) => i + 1).map(p =>
              Math.abs(p - page) <= 2 || p === 1 || p === totalPages ? (
                <button
                  key={p}
                  className={`search-pagination-btn${p === page ? ' active' : ''}`}
                  onClick={() => handlePageChange(p)}
                  disabled={p === page || loading}
                >
                  {p}
                </button>
              ) :
                (p === page - 3 || p === page + 3) ? (
                  <span key={p} className="search-pagination-ellipsis">…</span>
                ) : null
            )}
            <button
              className="search-pagination-btn"
              onClick={() => handlePageChange(page + 1)}
              disabled={page === totalPages || loading}
            >
              <FaChevronRight style={{ verticalAlign: 'middle' }} />
            </button>
          </div>
        )}
      </main>
    </div>
  );
}

export default App;
