import React from 'react';
import { View, Text, TouchableOpacity, Linking, StyleSheet } from 'react-native';
import type { SearchHit } from '../types/search';

interface SearchResultsProps {
  results: SearchHit[];
  loading?: boolean;
  error?: string | null;
  searched: boolean;
  total: number;
}

export const SearchResults: React.FC<SearchResultsProps> = ({ results, loading, error, searched, total }) => {
  if (loading) return (
    <View style={styles.loadingRow}>
      <Text style={styles.loading}>検索中...</Text>
    </View>
  );
  if (error) return <Text style={styles.error}>{error}</Text>;
  if (searched && results.length === 0) return <Text style={styles.empty}>該当する結果がありません</Text>;
  return (
    <View style={styles.container}>
      {searched && results.length > 0 && (
        <Text style={styles.count}>{total} 件の結果が見つかりました</Text>
      )}
      {results.map(hit => (
        <View style={styles.hit} key={hit.id}>
          <TouchableOpacity onPress={() => Linking.openURL(hit.url)}>
            <Text style={styles.hitTitle} numberOfLines={2} ellipsizeMode="tail">{hit.title || hit.url}</Text>
          </TouchableOpacity>
          <Text style={styles.hitUrl} numberOfLines={1} ellipsizeMode="middle">{hit.url}</Text>
          {hit.snippet && <Text style={styles.hitSnippet}>{hit.snippet.replace(/<[^>]+>/g, '')}</Text>}
          <View style={styles.hitMeta}>
            <Text style={styles.hitMetaText}>{hit.site}</Text>
            <Text style={styles.hitMetaText}>{hit.lang}</Text>
            <Text style={styles.hitMetaText}>score: {hit.score.toFixed(2)}</Text>
          </View>
        </View>
      ))}
    </View>
  );
};

const styles = StyleSheet.create({
  container: {
    width: '100%',
    maxWidth: 700,
    alignSelf: 'center',
    paddingHorizontal: 8,
  },
  count: {
    marginBottom: 12,
    color: '#5f6368',
    fontSize: 13,
  },
  loadingRow: {
    flexDirection: 'row',
    alignItems: 'center',
    marginVertical: 16,
  },
  loading: {
    color: '#007AFF',
    textAlign: 'left',
    fontSize: 15,
  },
  error: { color: 'red', marginBottom: 8, textAlign: 'center' },
  empty: { color: '#888', marginBottom: 8, textAlign: 'center' },
  hit: {
    marginBottom: 28,
    paddingBottom: 4,
    borderBottomWidth: 1,
    borderBottomColor: '#e0e0e0',
  },
  hitTitle: {
    fontSize: 17,
    fontWeight: '500',
    color: '#1a0dab',
    marginBottom: 2,
    lineHeight: 22,
  },
  hitUrl: {
    color: '#006621',
    fontSize: 13,
    marginBottom: 2,
  },
  hitSnippet: {
    color: '#4d5156',
    fontSize: 14,
    lineHeight: 20,
    marginBottom: 2,
  },
  hitMeta: {
    flexDirection: 'row',
    flexWrap: 'wrap',
    marginTop: 2,
    marginBottom: 2,
    gap: 8,
  },
  hitMetaText: {
    color: '#70757a',
    marginRight: 8,
    fontSize: 12,
  },
});
