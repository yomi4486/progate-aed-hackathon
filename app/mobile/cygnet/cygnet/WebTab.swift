//  WebTab.swift
//  cygnet
//
//  Created by yomi4486 on 2025/09/14.

import Foundation
import WebKit

class WebTab: ObservableObject, Identifiable {
    let id = UUID()
    @Published var webView: WKWebView
    @Published var url: URL
    
    init(url: URL) {
        self.url = url
        self.webView = WKWebView()
        self.webView.load(URLRequest(url: url))
    }
}
