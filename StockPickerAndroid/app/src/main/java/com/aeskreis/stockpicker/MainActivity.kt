package com.aeskreis.stockpicker

import android.os.Bundle
import android.webkit.WebView
import android.webkit.WebViewClient
import androidx.activity.ComponentActivity

class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        
        // Create and configure WebView
        val webView = WebView(this)
        webView.settings.javaScriptEnabled = true
        webView.settings.domStorageEnabled = true
        webView.settings.loadWithOverviewMode = true
        webView.settings.useWideViewPort = true
        webView.settings.builtInZoomControls = true
        webView.settings.displayZoomControls = false
        
        // Set WebViewClient to handle page navigation within the WebView
        webView.webViewClient = WebViewClient()
        
        // Load Google
        webView.loadUrl("https://www.google.com")
        
        // Set the WebView as the content view
        setContentView(webView)
    }
}