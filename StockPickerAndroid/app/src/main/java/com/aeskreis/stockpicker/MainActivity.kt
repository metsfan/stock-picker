package com.aeskreis.stockpicker

import android.content.Context
import android.os.Bundle
import android.webkit.CookieManager
import android.webkit.WebView
import android.webkit.WebViewClient
import androidx.activity.ComponentActivity
import androidx.activity.compose.BackHandler
import androidx.activity.compose.setContent
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.material3.Button
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.text.input.PasswordVisualTransformation
import androidx.compose.ui.unit.dp
import androidx.compose.ui.viewinterop.AndroidView
import com.aeskreis.stockpicker.ui.theme.StockPickerTheme

class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContent {
            StockPickerTheme {
                Scaffold(modifier = Modifier.fillMaxSize()) { innerPadding ->
                    AppContent(
                        modifier = Modifier
                            .fillMaxSize()
                            .padding(innerPadding)
                    )
                }
            }
        }
    }
}

@Composable
fun AppContent(modifier: Modifier = Modifier) {
    val context = LocalContext.current
    val sharedPrefs = remember { context.getSharedPreferences("app_prefs", Context.MODE_PRIVATE) }
    
    // Load credentials from storage
    var username by remember { mutableStateOf(sharedPrefs.getString("username", "") ?: "") }
    var password by remember { mutableStateOf(sharedPrefs.getString("password", "") ?: "") }
    
    // isLoggedIn is based on whether credentials exist in storage
    val isLoggedIn = username.isNotEmpty() && password.isNotEmpty()
    
    if (isLoggedIn) {
        WebViewScreen(
            url = "https://$username:$password@stocks.arkarenamod.com",
            modifier = modifier
        )
    } else {
        LoginScreen(
            modifier = modifier,
            onLogin = { user, pass ->
                // Save credentials to SharedPreferences
                sharedPrefs.edit().apply {
                    putString("username", user)
                    putString("password", pass)
                    apply()
                }
                // Update state to trigger recomposition
                username = user
                password = pass
            }
        )
    }
}

@Composable
fun LoginScreen(
    modifier: Modifier = Modifier,
    onLogin: (username: String, password: String) -> Unit
) {
    var username by remember { mutableStateOf("") }
    var password by remember { mutableStateOf("") }
    
    Column(
        modifier = modifier
            .padding(32.dp),
        verticalArrangement = Arrangement.Center,
        horizontalAlignment = Alignment.CenterHorizontally
    ) {
        Text(
            text = "Stock Picker Login",
            style = MaterialTheme.typography.headlineMedium,
            color = MaterialTheme.colorScheme.primary
        )
        
        Spacer(modifier = Modifier.height(32.dp))
        
        OutlinedTextField(
            value = username,
            onValueChange = { username = it },
            label = { Text("Username") },
            singleLine = true,
            modifier = Modifier.fillMaxWidth(),
            keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Text)
        )
        
        Spacer(modifier = Modifier.height(16.dp))
        
        OutlinedTextField(
            value = password,
            onValueChange = { password = it },
            label = { Text("Password") },
            singleLine = true,
            modifier = Modifier.fillMaxWidth(),
            visualTransformation = PasswordVisualTransformation(),
            keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Password)
        )
        
        Spacer(modifier = Modifier.height(24.dp))
        
        Button(
            onClick = {
                if (username.isNotEmpty() && password.isNotEmpty()) {
                    onLogin(username, password)
                }
            },
            modifier = Modifier.fillMaxWidth(),
            enabled = username.isNotEmpty() && password.isNotEmpty()
        ) {
            Text("Login")
        }
    }
}

@Composable
fun WebViewScreen(url: String, modifier: Modifier = Modifier) {
    var webView by remember { mutableStateOf<WebView?>(null) }
    var canGoBack by remember { mutableStateOf(false) }
    
    // Handle back button press
    BackHandler(enabled = canGoBack) {
        webView?.goBack()
    }
    
    AndroidView(
        modifier = modifier,
        factory = { context ->
            // Enable cookies globally
            val cookieManager = CookieManager.getInstance()
            cookieManager.setAcceptCookie(true)
            cookieManager.setAcceptThirdPartyCookies(
                WebView(context), true
            )
            
            WebView(context).apply {
                settings.javaScriptEnabled = true
                settings.domStorageEnabled = true
                settings.loadWithOverviewMode = true
                settings.useWideViewPort = true
                settings.builtInZoomControls = true
                settings.displayZoomControls = false
                
                // Enable cookies for this WebView
                cookieManager.setAcceptThirdPartyCookies(this, true)
                
                webViewClient = object : WebViewClient() {
                    override fun doUpdateVisitedHistory(view: WebView?, url: String?, isReload: Boolean) {
                        super.doUpdateVisitedHistory(view, url, isReload)
                        canGoBack = view?.canGoBack() ?: false
                    }
                }
                
                loadUrl(url)
                webView = this
            }
        }
    )
    
    DisposableEffect(Unit) {
        onDispose {
            webView?.destroy()
        }
    }
}
