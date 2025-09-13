//  WebTabView.swift
//  cygnet
//
//  Created by yomi4486 on 2025/09/14.

import SwiftUI
import WebKit

struct WebTabView: UIViewRepresentable {
    @ObservedObject var tab: WebTab
    
    func makeUIView(context: Context) -> WKWebView {
        tab.webView
    }
    
    func updateUIView(_ uiView: WKWebView, context: Context) {
        // No-op: handled by WebTab
    }
}
