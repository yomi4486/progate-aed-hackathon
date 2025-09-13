import React, { useRef } from 'react';
import { View, TextInput, TouchableOpacity, Text, StyleSheet } from 'react-native';
import { Ionicons } from '@expo/vector-icons';

interface SearchBarProps {
  value: string;
  onChange: (v: string) => void;
  onSearch: () => void;
  loading?: boolean;
}

export const SearchBar: React.FC<SearchBarProps> = ({ value, onChange, onSearch, loading }) => {
  const inputRef = useRef<TextInput>(null);
  return (
    <View style={styles.container}>
      <TextInput
        ref={inputRef}
        style={styles.input}
        placeholder="検索ワードを入力..."
        value={value}
        onChangeText={onChange}
        onSubmitEditing={onSearch}
        returnKeyType="search"
        editable={!loading}
      />
      <TouchableOpacity style={styles.button} onPress={onSearch} disabled={loading || !value}>
        <Ionicons name="search" size={22} color={loading || !value ? '#aaa' : '#fff'} />
      </TouchableOpacity>
    </View>
  );
};

const styles = StyleSheet.create({
  container: {
    flexDirection: 'row',
    alignItems: 'center',
    marginBottom: 8,
    backgroundColor: '#f8f8f8',
    borderRadius: 24,
    paddingHorizontal: 8,
    paddingVertical: 2,
    shadowColor: '#000',
    shadowOpacity: 0.05,
    shadowRadius: 2,
    elevation: 1,
  },
  input: {
    flex: 1,
    height: 44,
    borderRadius: 24,
    paddingHorizontal: 16,
    fontSize: 16,
    backgroundColor: '#fff',
    color: '#222',
  },
  button: {
    backgroundColor: '#007AFF',
    borderRadius: 24,
    padding: 10,
    marginLeft: 4,
    justifyContent: 'center',
    alignItems: 'center',
  },
});
