import React from 'react';
import { View, TouchableOpacity, Text, StyleSheet } from 'react-native';
import { Ionicons } from '@expo/vector-icons';

interface PaginationProps {
  page: number;
  totalPages: number;
  onPageChange: (p: number) => void;
  loading?: boolean;
}

export const Pagination: React.FC<PaginationProps> = ({ page, totalPages, onPageChange, loading }) => {
  if (totalPages <= 1) return null;
  return (
    <View style={styles.container}>
      <TouchableOpacity
        style={styles.pageBtn}
        onPress={() => onPageChange(page - 1)}
        disabled={page === 1 || loading}
      >
        <Ionicons name="chevron-back" size={20} color={page === 1 || loading ? '#aaa' : '#007AFF'} />
      </TouchableOpacity>
      {Array.from({ length: totalPages }, (_, i) => i + 1).map(p =>
        Math.abs(p - page) <= 2 || p === 1 || p === totalPages ? (
          <TouchableOpacity
            key={p}
            style={[styles.pageBtn, p === page && styles.pageBtnActive]}
            onPress={() => onPageChange(p)}
            disabled={p === page || loading}
          >
            <Text style={[styles.pageBtnText, p === page && styles.pageBtnTextActive]}>{p}</Text>
          </TouchableOpacity>
        ) :
          (p === page - 3 || p === page + 3) ? (
            <Text key={p} style={styles.ellipsis}>…</Text>
          ) : null
      )}
      <TouchableOpacity
        style={styles.pageBtn}
        onPress={() => onPageChange(page + 1)}
        disabled={page === totalPages || loading}
      >
        <Ionicons name="chevron-forward" size={20} color={page === totalPages || loading ? '#aaa' : '#007AFF'} />
      </TouchableOpacity>
    </View>
  );
};

const styles = StyleSheet.create({
  container: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'center',
    marginTop: 16,
    flexWrap: 'wrap',
    gap: 2,
  },
  pageBtn: {
    padding: 8,
    marginHorizontal: 2,
    borderRadius: 4,
    backgroundColor: 'transparent',
  },
  pageBtnActive: {
    backgroundColor: '#007AFF',
  },
  pageBtnText: {
    color: '#333',
    fontWeight: 'bold',
    fontSize: 14,
  },
  pageBtnTextActive: {
    color: '#fff',
  },
  ellipsis: {
    marginHorizontal: 4,
    color: '#888',
    fontSize: 16,
  },
});
