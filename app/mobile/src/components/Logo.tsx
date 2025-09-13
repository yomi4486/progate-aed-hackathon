import React from 'react';
import { Text, StyleSheet } from 'react-native';

export const Logo: React.FC<{ size?: number }> = ({ size = 32 }) => (
  <Text style={[styles.text, { fontSize: size }]}>ProgateHackathon</Text>
);

const styles = StyleSheet.create({
  text: {
    fontWeight: 'bold',
    color: '#4285f4',
    letterSpacing: 1.2,
  },
});
