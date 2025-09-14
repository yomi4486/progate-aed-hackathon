//
//  ContentView.swift
//  cygnet
//
//  Created by yomi4486 on 2025/09/14.
//

import SwiftUI
import WebKit
import UIKit

struct ContentView: View {
    private let baseUrl: String = Bundle.main.object(forInfoDictionaryKey: "BaseWebUrl") as? String ?? "https://main.deat62vf60g8n.amplifyapp.com"
    @State private var tabs: [WebTab] = []
    @State private var selectedTabId: UUID = UUID()
    @State private var urlInput: String = ""
    @FocusState private var urlFieldFocused: Bool
    @State private var showTabOverview: Bool = false
    @Environment(\.colorScheme) var colorScheme

    init() {
        // 初期タブをセット
        let url = URL(string: Bundle.main.object(forInfoDictionaryKey: "BaseWebUrl") as? String ?? "https://main.deat62vf60g8n.amplifyapp.com")!
        _tabs = State(initialValue: [WebTab(url: url)])
        _urlInput = State(initialValue: url.absoluteString)
    }

    var body: some View {
        GeometryReader { geo in
            ZStack(alignment: .bottom) {
                VStack(spacing: 0) {
                    // AppBar
                    HStack(spacing: 8) {
                        Image(systemName: "globe")
                            .font(.title2)
                            .foregroundColor(.accentColor)
                        TextField("URLを入力", text: $urlInput)
                            .keyboardType(.URL)
                            .textInputAutocapitalization(.never)
                            .disableAutocorrection(true)
                            .focused($urlFieldFocused)
                            .padding(8)
                            .background(Color(.systemGray6))
                            .cornerRadius(10)
                            .onSubmit {
                                loadUrlFromInput()
                            }
                        Button(action: loadUrlFromInput) {
                            Image(systemName: "arrow.right.circle.fill")
                                .font(.title2)
                                .foregroundColor(.accentColor)
                        }
                    }
                    .padding(.top, geo.safeAreaInsets.top + 2)
                    .padding(.horizontal, 10)
                    .padding(.bottom, 0)
                    .background(
                        Color(.systemBackground)
                            .opacity(0.98)
                            .shadow(color: Color.black.opacity(0.06), radius: 2, x: 0, y: 2)
                    )

                    // WebView area
                    ZStack {
                        if let tab = tabs.first(where: { $0.id == selectedTabId }) {
                            WebTabView(tab: tab)
                                .id(tab.id) // ここでidを付与
                        } else if let firstTab = tabs.first {
                            WebTabView(tab: firstTab)
                                .id(firstTab.id)
                        } else {
                            Text("No tabs open")
                        }
                    }
                }
                .edgesIgnoringSafeArea(.bottom)

                // BottomNavigationBar
                HStack(spacing: 32) {
                    Button(action: goBack) {
                        Image(systemName: "chevron.backward")
                            .font(.system(size: 24, weight: .regular))
                            .foregroundColor(canGoBack ? .accentColor : .gray)
                    }
                    .disabled(!canGoBack)

                    Button(action: goForward) {
                        Image(systemName: "chevron.forward")
                            .font(.system(size: 24, weight: .regular))
                            .foregroundColor(canGoForward ? .accentColor : .gray)
                    }
                    .disabled(!canGoForward)

                    Button(action: addTab) {
                        Image(systemName: "plus")
                            .font(.system(size: 28, weight: .regular))
                            .foregroundColor(.accentColor)
                    }

                    Button(action: { showTabOverview = true }) {
                        ZStack {
                            Image(systemName: "square.on.square")
                                .font(.system(size: 24, weight: .regular))
                                .foregroundColor(.accentColor)
                            if tabs.count > 1 {
                                Text("\(tabs.count)")
                                    .font(.caption2.bold())
                                    .foregroundColor(.white)
                                    .padding(4)
                                    .background(Color.red)
                                    .clipShape(Circle())
                                    .offset(x: 14, y: -14)
                            }
                        }
                    }
                }
                .frame(maxWidth: .infinity)
                .padding(.bottom, geo.safeAreaInsets.bottom + 8)
                .padding(.top, 8)
                .background(
                    Color(.systemBackground)
                        .opacity(0.98)
                        .shadow(color: Color.black.opacity(0.10), radius: 8, x: 0, y: -2)
                        .edgesIgnoringSafeArea(.bottom)
                )

                // タブ一覧ボトムシート
                if showTabOverview {
                    Color.black.opacity(0.3)
                        .edgesIgnoringSafeArea(.all)
                        .onTapGesture { showTabOverview = false }
                    VStack {
                        Spacer()
                        VStack(spacing: 16) {
                            Capsule()
                                .fill(Color(.systemGray3))
                                .frame(width: 40, height: 5)
                                .padding(.top, 8)
                            Text("タブ一覧")
                                .font(.headline)
                                .padding(.top, 0)
                            ScrollView(.horizontal, showsIndicators: false) {
                                HStack(spacing: 16) {
                                    ForEach(tabs) { tab in
                                        VStack(spacing: 8) {
                                            ZStack(alignment: .topTrailing) {
                                                Button(action: {
                                                    selectTab(tab)
                                                    showTabOverview = false
                                                }) {
                                                    RoundedRectangle(cornerRadius: 16)
                                                        .fill(selectedTabId == tab.id ? Color.accentColor.opacity(0.15) : Color(.systemGray6))
                                                        .frame(width: 180, height: 240)
                                                        .overlay(
                                                            VStack(alignment: .leading, spacing: 6) {
                                                                Text(tab.url.host ?? "Tab")
                                                                    .font(.subheadline.bold())
                                                                    .foregroundColor(.primary)
                                                                    .lineLimit(1)
                                                                Text(tab.url.absoluteString)
                                                                    .font(.caption2)
                                                                    .foregroundColor(.secondary)
                                                                    .lineLimit(2)
                                                                Spacer()
                                                                Image(systemName: "globe")
                                                                    .font(.largeTitle)
                                                                    .foregroundColor(.accentColor)
                                                            }
                                                            .padding(12)
                                                        )
                                                }
                                                Button(action: { closeTab(tab) }) {
                                                    Image(systemName: "xmark.circle.fill")
                                                        .font(.system(size: 20))
                                                        .foregroundColor(.gray)
                                                        .padding(6)
                                                }
                                                .offset(x: 8, y: -8)
                                            }
                                        }
                                    }
                                }
                                .padding(.horizontal, 16)
                            }
                            Button("閉じる") { showTabOverview = false }
                                .font(.headline)
                                .padding(.vertical, 12)
                        }
                        .frame(maxWidth: .infinity)
                        .background(
                            Color(.systemBackground)
                                .modifier(TopCornersRadius(radius: 24))
                                .shadow(radius: 16)
                        )
                        .padding(.horizontal, 0)
                        .padding(.bottom, geo.safeAreaInsets.bottom)
                    }
                    .transition(.move(edge: .bottom))
                    .animation(.easeOut(duration: 0.2), value: showTabOverview)
                }
// Extension for corner radius on specific corners

            }
        }
        .onAppear {
            if let first = tabs.first {
                selectedTabId = first.id
                urlInput = first.url.absoluteString
            }
        }
    }
    private var canGoBack: Bool {
        if let tab = tabs.first(where: { $0.id == selectedTabId }) {
            return tab.webView.canGoBack
        }
        return false
    }

    private var canGoForward: Bool {
        if let tab = tabs.first(where: { $0.id == selectedTabId }) {
            return tab.webView.canGoForward
        }
        return false
    }

    private func goBack() {
        if let tab = tabs.first(where: { $0.id == selectedTabId }) {
            tab.webView.goBack()
        }
    }

    private func goForward() {
        if let tab = tabs.first(where: { $0.id == selectedTabId }) {
            tab.webView.goForward()
        }
    }

    private func addTab() {
    let newTab = WebTab(url: URL(string: baseUrl)!)
    tabs.append(newTab)
    selectedTabId = newTab.id
    urlInput = newTab.url.absoluteString
    urlFieldFocused = true
    }

    private func closeTab(_ tab: WebTab) {
        if let idx = tabs.firstIndex(where: { $0.id == tab.id }) {
            tabs.remove(at: idx)
            if selectedTabId == tab.id {
                if let first = tabs.first {
                    selectedTabId = first.id
                    urlInput = first.url.absoluteString
                } else {
                    // タブがゼロになった場合は新しいタブを自動で作成
                    let newTab = WebTab(url: URL(string: baseUrl)!)
                    tabs.append(newTab)
                    selectedTabId = newTab.id
                    urlInput = newTab.url.absoluteString
                }
            }
        }
    }

    private func selectTab(_ tab: WebTab) {
        selectedTabId = tab.id
        urlInput = tab.url.absoluteString
        urlFieldFocused = false
    }

    private func loadUrlFromInput() {
        // 入力が有効なURLか判定
        if let url = URL(string: urlInput), url.scheme == "http" || url.scheme == "https" {
            if let idx = tabs.firstIndex(where: { $0.id == selectedTabId }) {
                tabs[idx].url = url
                tabs[idx].webView.load(URLRequest(url: url))
            }
        } else {
            // URLでなければ、環境変数のURLのqパラメータに値をセット
            var components = URLComponents(string: baseUrl)
            components?.queryItems = [URLQueryItem(name: "q", value: urlInput)]
            if let url = components?.url, let idx = tabs.firstIndex(where: { $0.id == selectedTabId }) {
                tabs[idx].url = url
                tabs[idx].webView.load(URLRequest(url: url))
            }
        }
        urlFieldFocused = false
    }
}


#Preview {
    ContentView()
        .modelContainer(for: Item.self, inMemory: true)
}

// Helper ViewModifier for top corners only
import SwiftUI
struct TopCornersRadius: ViewModifier {
    var radius: CGFloat
    func body(content: Content) -> some View {
        content
            .clipShape(RoundedCorner(radius: radius, corners: [.topLeft, .topRight]))
    }
}
