export interface CharWidthAnalysis {
  text: string;
  total_chars: number;
  fullwidth_chars: string[];
  halfwidth_chars: string[];
  other_chars: string[];
  fullwidth_count: number;
  halfwidth_count: number;
  other_count: number;
  is_all_fullwidth: boolean;
  is_all_halfwidth: boolean;
  has_mixed_widths: boolean;
}

export interface CharWidthResponse {
  original_query: string;
  is_chahan_related: boolean;
  analysis: CharWidthAnalysis;
  converted_fullwidth: string;
  converted_halfwidth: string;
  message: string;
}