package com.hari.androidtvremote.utils

import android.content.Context
import android.net.nsd.NsdManager
import android.net.nsd.NsdServiceInfo
import android.net.wifi.WifiManager
import android.os.Build
import timber.log.Timber
import java.net.Inet4Address

/**
 * Finds Android TV / Google TV devices by their remote-control service
 * (`_androidtvremote2._tcp`, port 6466) — the same mDNS record the official
 * Google TV app uses. Unlike Cast discovery this works even when the TV's
 * "Chromecast built-in" app is disabled or missing.
 */
class AtvNsdDiscovery(
    context: Context,
    private val onFound: (host: String, name: String) -> Unit
) {
    private val nsd = context.getSystemService(Context.NSD_SERVICE) as NsdManager
    private val wifi = context.applicationContext.getSystemService(Context.WIFI_SERVICE) as WifiManager
    private var multicastLock: WifiManager.MulticastLock? = null
    private var listener: NsdManager.DiscoveryListener? = null
    private val queue = ArrayDeque<NsdServiceInfo>()
    private var resolving = false

    @Synchronized
    fun start() {
        if (listener != null) return
        try {
            multicastLock = wifi.createMulticastLock("atvr-discovery").apply {
                setReferenceCounted(false)
                acquire()
            }
        } catch (e: Exception) {
            Timber.w(e, "[AtvNsd] Multicast lock failed")
        }
        val l = object : NsdManager.DiscoveryListener {
            override fun onDiscoveryStarted(serviceType: String) {
                Timber.i("[AtvNsd] Discovery started")
            }
            override fun onDiscoveryStopped(serviceType: String) = Unit
            override fun onStartDiscoveryFailed(serviceType: String, errorCode: Int) {
                Timber.w("[AtvNsd] Start failed: $errorCode")
                synchronized(this@AtvNsdDiscovery) { listener = null }
            }
            override fun onStopDiscoveryFailed(serviceType: String, errorCode: Int) = Unit
            override fun onServiceFound(serviceInfo: NsdServiceInfo) = enqueue(serviceInfo)
            override fun onServiceLost(serviceInfo: NsdServiceInfo) = Unit
        }
        listener = l
        try {
            nsd.discoverServices(SERVICE_TYPE, NsdManager.PROTOCOL_DNS_SD, l)
        } catch (e: Exception) {
            Timber.w(e, "[AtvNsd] discoverServices threw")
            listener = null
        }
    }

    @Synchronized
    fun stop() {
        listener?.let { runCatching { nsd.stopServiceDiscovery(it) } }
        listener = null
        queue.clear()
        runCatching { multicastLock?.release() }
        multicastLock = null
    }

    fun restart() {
        stop()
        start()
    }

    @Synchronized
    private fun enqueue(info: NsdServiceInfo) {
        queue.addLast(info)
        resolveNext()
    }

    // NsdManager can only resolve one service at a time on older Android versions.
    @Synchronized
    private fun resolveNext() {
        if (resolving) return
        val next = queue.removeFirstOrNull() ?: return
        resolving = true
        try {
            @Suppress("DEPRECATION")
            nsd.resolveService(next, object : NsdManager.ResolveListener {
                override fun onResolveFailed(serviceInfo: NsdServiceInfo, errorCode: Int) {
                    Timber.w("[AtvNsd] Resolve failed for ${serviceInfo.serviceName}: $errorCode")
                    resolveDone()
                }

                override fun onServiceResolved(serviceInfo: NsdServiceInfo) {
                    val host = hostOf(serviceInfo)
                    Timber.i("[AtvNsd] Resolved ${serviceInfo.serviceName} -> $host")
                    if (host != null) onFound(host, serviceInfo.serviceName ?: "Android TV")
                    resolveDone()
                }
            })
        } catch (e: Exception) {
            Timber.w(e, "[AtvNsd] resolveService threw")
            resolving = false
            resolveNext()
        }
    }

    private fun resolveDone() {
        synchronized(this) { resolving = false }
        resolveNext()
    }

    private fun hostOf(info: NsdServiceInfo): String? {
        val addr = if (Build.VERSION.SDK_INT >= 34) {
            info.hostAddresses.firstOrNull { it is Inet4Address } ?: info.hostAddresses.firstOrNull()
        } else {
            @Suppress("DEPRECATION")
            info.host
        }
        return addr?.hostAddress?.takeIf { it.isNotBlank() }
    }

    companion object {
        const val SERVICE_TYPE = "_androidtvremote2._tcp."
    }
}
