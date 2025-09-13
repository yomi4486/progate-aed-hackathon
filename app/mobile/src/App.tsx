import { useState, useEffect } from 'react';
import { RPCClientImpl } from './rpc-client';
import { View, Text, ScrollView, StyleSheet, useWindowDimensions, TouchableOpacity, Platform, StatusBar } from 'react-native';
import { SearchBar } from './components/SearchBar';
import { SearchResults } from './components/SearchResults';
import { Pagination } from './components/Pagination';
import { Logo } from './components/Logo';

const baseURL = process.env.EXPO_PUBLIC_API_BASE_URL || '';
const rpc = new RPCClientImpl(baseURL);

export default function App() {
  const [inputValue, setInputValue] = useState('');
  const [results, setResults] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [searched, setSearched] = useState(false);
  const [page, setPage] = useState(1);
  const [total, setTotal] = useState(0);
  const [size] = useState(30);
  const [query, setQuery] = useState('');

  // クエリパラメータから初期値取得
  useEffect(() => {
    if (Platform.OS === 'web') {
      const params = new URLSearchParams(window.location.search);
      const q = params.get('q') || '';
      const p = parseInt(params.get('page') || '1', 10);
      setInputValue(q);
      setQuery(q);
      setPage(isNaN(p) ? 1 : p);
      if (q) {
        setSearched(true);
        fetchSearch(q, isNaN(p) ? 1 : p, size);
      }
    }
  }, []);
  const { width, height } = useWindowDimensions();
  const isLandscape = width > height;

  const updateQueryParams = (q: string, p: number) => {
    if (typeof window !== 'undefined' && window.history && window.location) {
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
    // React Native (モバイル) では何もしない
  };

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

  const handleSearch = () => {
    setQuery(inputValue);
    setPage(1);
    fetchSearch(inputValue, 1, size);
  };

  const handlePageChange = (newPage: number) => {
    setPage(newPage);
    fetchSearch(query, newPage, size);
  };

  // ロゴタップで初期状態に戻す
  const handleLogoPress = () => {
    setInputValue('');
    setQuery('');
    setResults([]);
    setError(null);
    setSearched(false);
    setPage(1);
    updateQueryParams('', 1);
  };

  const totalPages = Math.max(1, Math.ceil(total / size));

  // 未検索時は中央配置、検索時は上部配置（横画面なら横並び）
  // Safe area top padding for status bar / notch
  const safeAreaTop = Platform.OS === 'ios' ? 44 : (StatusBar.currentHeight || 24);
  const safeAreaStyle = Platform.OS !== 'web' ? { paddingTop: safeAreaTop } : {};

  if (!searched) {
    return (
      <View style={[styles.centerRoot, safeAreaStyle]}> 
        <TouchableOpacity onPress={handleLogoPress} activeOpacity={0.7}>
          <Logo size={80} />
        </TouchableOpacity>
        <View style={{ height: 32 }} />
        <View style={{ width: '100%', maxWidth: 500 }}>
          <SearchBar
            value={inputValue}
            onChange={setInputValue}
            onSearch={handleSearch}
            loading={loading}
          />
        </View>
      </View>
    );
  }

  return (
    <View style={[styles.root, safeAreaStyle]}> 
      <View style={[styles.topmenu, isLandscape && styles.topmenuRow]}>
        <TouchableOpacity onPress={handleLogoPress} activeOpacity={0.7}>
          <Logo size={32} />
        </TouchableOpacity>
        <View style={{ width: isLandscape ? 16 : 0 }} />
        <SearchBar
          value={inputValue}
          onChange={setInputValue}
          onSearch={handleSearch}
          loading={loading}
        />
      </View>
      <ScrollView style={styles.results}>
        <SearchResults
          results={results}
          loading={loading}
          error={error}
          searched={searched}
          total={total}
        />
        <Pagination
          page={page}
          totalPages={totalPages}
          onPageChange={handlePageChange}
          loading={loading}
        />
      </ScrollView>
    </View>
  );
}

const styles = StyleSheet.create({
  root: { flex: 1, backgroundColor: '#fff' },
  topmenu: { flexDirection: 'row', alignItems: 'center', padding: 12, backgroundColor: '#f8f8f8', borderBottomWidth: 1, borderColor: '#eee', justifyContent: 'center' },
  topmenuRow: { flexDirection: 'row' },
  results: { flex: 1, padding: 16 },
  centerRoot: {
    flex: 1,
    justifyContent: 'center',
    alignItems: 'center',
    backgroundColor: '#fff',
    paddingHorizontal: 32,
  },
  // centerRow: { // no longer needed for vertical layout
  //   flexDirection: 'row',
  //   alignItems: 'center',
  //   width: '100%',
  //   maxWidth: 1400,
  //   justifyContent: 'center',
  // },
});
