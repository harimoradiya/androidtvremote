package com.hari.androidtvremote.androidLib.remote;

import android.util.Log;

import com.hari.androidtvremote.androidLib.exception.PairingException;
import com.hari.androidtvremote.androidLib.ssl.DummyTrustManager;
import com.hari.androidtvremote.androidLib.ssl.KeyStoreManager;
import com.hari.androidtvremote.androidLib.wire.PacketParser;

import java.io.IOException;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;

public class RemoteSession {

    private static final String TAG = "RemoteSession";


    private static final class SynchronizedOutputStream extends OutputStream {
        private final OutputStream delegate;
        SynchronizedOutputStream(OutputStream delegate) { this.delegate = delegate; }

        @Override public synchronized void write(int b) throws IOException { delegate.write(b); }
        @Override public synchronized void write(byte[] b) throws IOException { delegate.write(b); }
        @Override public synchronized void write(byte[] b, int off, int len) throws IOException { delegate.write(b, off, len); }
        @Override public synchronized void flush() throws IOException { delegate.flush(); }
        @Override public synchronized void close() throws IOException { delegate.close(); }
    }

    private final BlockingQueue<Remotemessage.RemoteMessage> mMessageQueue;
    private final RemoteMessageManager mMessageManager;

    private final String mHost;
    private final int mPort;
    private final RemoteSessionListener mRemoteSessionListener;

    // BUG FIX 3: Use AtomicBoolean so flag is thread-safe and can be reset cleanly
    // between sessions without a stale `true` blocking the next VoiceBegin.
    private final AtomicBoolean voiceSessionActive = new AtomicBoolean(false);
    private volatile int syncedVoiceSessionId = -1;

    // Central socket health flag.
    private final AtomicBoolean socketAlive = new AtomicBoolean(false);

    // Hold a reference to the socket so abort() can close it, immediately
    // unblocking any thread that is stuck in a native read() or TLS handshake.
    private volatile SSLSocket mSslSocket;

    /** Timeout for establishing the TCP+TLS connection (ms). */
    private static final int CONNECT_TIMEOUT_MS = 6_000;

    /**
     * Read timeout on the socket (ms).  After the handshake, blocking reads
     * must return within this window so the reader thread is never permanently
     * stuck if the TV stops responding silently.
     */
    private static final int READ_TIMEOUT_MS = 8_000;

    /**
     * How long to wait for a single handshake message from the TV (ms).
     * Must be ≤ READ_TIMEOUT_MS so poll() and setSoTimeout() agree.
     */
    private static final int HANDSHAKE_WAIT_MS = 8_000;

    int retry;
    private OutputStream outputStream;  // always a SynchronizedOutputStream after connect()
    private PacketParser packetParser;

    private volatile int syncedImeCounter = -1;
    private volatile int syncedFieldCounter = -1;
    private volatile int syncedTextInt5 = -1;
    private volatile String syncedTextLabel = "";
    private volatile Remotemessage.RemoteAppInfo syncedAppInfo = null;

    public RemoteSession(String host, int port, RemoteSessionListener remoteSessionListener) {
        mMessageQueue = new LinkedBlockingQueue<>();
        mMessageManager = new RemoteMessageManager();
        mHost = host;
        mPort = port;
        mRemoteSessionListener = remoteSessionListener;
    }


    private final java.util.concurrent.ExecutorService reconnectExecutor =
            java.util.concurrent.Executors.newSingleThreadExecutor();

    private final AtomicBoolean isConnecting = new AtomicBoolean(false);

    public void connect() throws GeneralSecurityException, IOException, InterruptedException, PairingException {
        // If already connected and alive, do not create another connection
        if (socketAlive.get() && mSslSocket != null && !mSslSocket.isClosed() && mSslSocket.isConnected()) {
            Log.i(TAG, "connect() skipped: socket is already alive and connected");
            return;
        }

        // Prevent parallel connect attempts from clashing
        if (!isConnecting.compareAndSet(false, true)) {
            Log.w(TAG, "connect() skipped: connection attempt already in progress");
            return;
        }

        try {
            // Clean up any existing socket/parser before opening a new connection
            if (packetParser != null) {
                try { packetParser.abort(); } catch (Exception ignored) {}
                packetParser = null;
            }
            if (mSslSocket != null) {
                try { mSslSocket.close(); } catch (Exception ignored) {}
                mSslSocket = null;
            }
            mMessageQueue.clear();

            SSLContext sSLContext = SSLContext.getInstance("TLS");
            sSLContext.init(
                    new KeyStoreManager().getKeyManagers(),
                    new TrustManager[]{new DummyTrustManager()},
                    new SecureRandom()
            );
            SSLSocketFactory sslSocketFactory = sSLContext.getSocketFactory();

            // ── Wake port knock (Android 14+ low power standby) ──────────────
            // In standby the TV closes 6466 and only keeps 6465 open. Connecting
            // there (same client cert) brings the remote service back, exactly
            // like the official Google TV app does.
            SSLSocket wakeSocket = null;
            if (!isPortOpen(mHost, mPort, 1200)) {
                wakeSocket = knockWakePort(sslSocketFactory);
                for (int i = 0; i < 10 && !isPortOpen(mHost, mPort, 800); i++) {
                    try { Thread.sleep(300); } catch (InterruptedException ignored) {}
                }
            }

            // ── Connect with timeout
            SSLSocket sSLSocket = (SSLSocket) sslSocketFactory.createSocket();
            try {
                sSLSocket.connect(new java.net.InetSocketAddress(mHost, mPort), CONNECT_TIMEOUT_MS);
            } finally {
                if (wakeSocket != null) {
                    final SSLSocket ws = wakeSocket;
                    new Thread(() -> {
                        try { Thread.sleep(5000); ws.close(); } catch (Exception ignored) {}
                    }, "wake-port-close").start();
                }
            }

            // Timeout during handshake
            sSLSocket.setSoTimeout(HANDSHAKE_WAIT_MS);

            sSLSocket.setNeedClientAuth(true);
            sSLSocket.setUseClientMode(true);
            sSLSocket.setKeepAlive(true);
            sSLSocket.setTcpNoDelay(true);
            sSLSocket.startHandshake();

            // Store reference so abort() can close the socket immediately
            mSslSocket = sSLSocket;

            outputStream = new SynchronizedOutputStream(sSLSocket.getOutputStream());

            packetParser = new RemotePacketParser(
                    sSLSocket.getInputStream(),
                    outputStream,
                    mMessageQueue,
                    new RemoteListener() {
                        @Override public void onConnected() { mRemoteSessionListener.onConnected(); }
                        @Override public void onDisconnected() {
                            boolean wasAlive = socketAlive.getAndSet(false);
                            if (wasAlive) {
                                Log.w(TAG, "PacketParser reported stream closed — notifying onDisconnected");
                                mRemoteSessionListener.onDisconnected();
                            }
                        }
                        @Override public void onVolume() {}
                        @Override public void onPerformInputDeviceRole() throws PairingException {}
                        @Override public void onPerformOutputDeviceRole(byte[] gamma) throws PairingException {}
                        @Override public void onSessionEnded() {}
                        @Override public void onError(String message) {}
                        @Override public void onImeShow(Remotemessage.RemoteTextFieldStatus status) {
                            updateSyncedTextFieldStatus(status);
                        }
                        @Override public void onLog(String message) {}
                        @Override public void sSLException() {}
                    }
            );

            packetParser.start();

            Remotemessage.RemoteMessage firstMsg =
                    mMessageQueue.poll(HANDSHAKE_WAIT_MS, java.util.concurrent.TimeUnit.MILLISECONDS);
            if (firstMsg == null) {
                throw new IOException("Handshake timeout: TV did not respond within " + HANDSHAKE_WAIT_MS + " ms");
            }

            outputStream.write(mMessageManager.createRemoteConfigure(
                    622, "ROG Strix G531GT_G531GT", "ASUSTeK COMPUTER INC.", 1, "1"));
            outputStream.flush();

            Remotemessage.RemoteMessage configAck =
                    mMessageQueue.poll(HANDSHAKE_WAIT_MS, java.util.concurrent.TimeUnit.MILLISECONDS);
            if (configAck == null) {
                throw new IOException("Handshake timeout: TV did not ack RemoteConfigure within " + HANDSHAKE_WAIT_MS + " ms");
            }

            outputStream.write(mMessageManager.createRemoteActive(622));
            outputStream.flush();

            // ── CRITICAL FIX: Handshake completed! Reset soTimeout to 0 (infinite wait).
            sSLSocket.setSoTimeout(0);

            socketAlive.set(true);

            startMessageReader();

        } catch (SSLException sslException) {
            mRemoteSessionListener.onSslError();
        } catch (Exception e) {
            e.printStackTrace();
            mRemoteSessionListener.onError(e.getMessage());
        } finally {
            isConnecting.set(false);
        }
    }


    private void startMessageReader() {
        new Thread(() -> {
            try {
                while (socketAlive.get()) {
                    Remotemessage.RemoteMessage message = waitForMessage();
                    if (message == null) {
                        if (packetParser != null && !packetParser.isAlive()) {
                            Log.w(TAG, "Message reader: PacketParser thread ended");
                            break;
                        }
                        continue;
                    }

                    if (message.hasRemoteImeShowRequest()) {
                        Remotemessage.RemoteImeShowRequest request = message.getRemoteImeShowRequest();
                        if (request.hasRemoteTextFieldStatus()) {
                            updateSyncedTextFieldStatus(request.getRemoteTextFieldStatus());
                        }
                    }

                    if (message.hasRemoteImeKeyInject()) {
                        Remotemessage.RemoteImeKeyInject imeKeyInject = message.getRemoteImeKeyInject();
                        if (imeKeyInject.hasAppInfo()) syncedAppInfo = imeKeyInject.getAppInfo();
                        if (imeKeyInject.hasTextFieldStatus()) {
                            updateSyncedTextFieldStatus(imeKeyInject.getTextFieldStatus());
                        }
                    }

                    if (message.hasRemoteImeBatchEdit()) {
                        syncedImeCounter = message.getRemoteImeBatchEdit().getImeCounter();
                        syncedFieldCounter = message.getRemoteImeBatchEdit().getFieldCounter();
                        Log.d(TAG, "ImeBatchEdit: imeCounter=" + syncedImeCounter
                                + " fieldCounter=" + syncedFieldCounter);
                    }

                    if (message.hasRemoteSetVolumeLevel()) {
                        Remotemessage.RemoteSetVolumeLevel vol = message.getRemoteSetVolumeLevel();
                        int level    = (int) vol.getVolumeLevel();
                        int maxLevel = (int) vol.getVolumeMax();
                        boolean muted = vol.getVolumeMuted();
                        Log.i(TAG, "Volume update: " + level + "/" + maxLevel + " muted=" + muted);
                        if (mVolumeListener != null) {
                            mVolumeListener.onVolumeChanged(level, maxLevel, muted);
                        }
                    }
                }
            } catch (InterruptedException e) {
                Log.i(TAG, "Message reader interrupted — session aborted");
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                Log.e(TAG, "Message reader error: " + e.getMessage());
            } finally {
                // ─────────────────────────────────────────────────────────────────
                // CRITICAL FIX: always notify the upper layer that the connection
                // is gone, regardless of why the reader loop exited.
                // ─────────────────────────────────────────────────────────────────
                boolean wasAlive = socketAlive.getAndSet(false);
                if (wasAlive) {
                    Log.w(TAG, "Message reader thread exiting — notifying onDisconnected()");
                    mRemoteSessionListener.onDisconnected();
                } else {
                    Log.i(TAG, "Message reader thread exiting — abort() already called");
                }
            }
        }, "RemoteSession-Reader").start();
    }

    private void updateSyncedTextFieldStatus(Remotemessage.RemoteTextFieldStatus status) {
        if (status == null) return;
        syncedFieldCounter = status.getCounterField();
        syncedTextInt5 = status.getInt5();
        syncedTextLabel = status.getLabel();
        mRemoteSessionListener.onImeShow(status.getValue(), status.getCounterField());
    }

    Remotemessage.RemoteMessage waitForMessage() throws InterruptedException {
        return mMessageQueue.poll(2000, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    public void attemptToReconnect() {
        if (socketAlive.get() || isConnecting.get()) {
            return;
        }
        retry++;
        reconnectExecutor.execute(() -> {
            try {
                Log.i(TAG, "attemptToReconnect: executing background connect...");
                connect();
            } catch (Exception e) {
                Log.e(TAG, "attemptToReconnect failed: " + e.getMessage());
                mRemoteSessionListener.onError(e.getMessage());
            }
        });
    }

    private void triggerReconnectAndSend(byte[] data, String callerTag) {
        reconnectExecutor.execute(() -> {
            try {
                if (!socketAlive.get()) {
                    Log.i(TAG, callerTag + ": socket not alive, attempting fast auto-reconnect...");
                    connect();
                }
                if (socketAlive.get() && data != null && outputStream != null) {
                    outputStream.write(data);
                    outputStream.flush();
                    Log.i(TAG, callerTag + ": successfully sent after auto-reconnect");
                }
            } catch (Exception e) {
                Log.e(TAG, callerTag + ": auto-reconnect and retry failed: " + e.getMessage());
            }
        });
    }

    /**
     * Abort the session.  Sets socketAlive=false BEFORE interrupting the
     * packetParser so the reader thread's finally-block sees wasAlive=false
     * and does NOT call onDisconnected() (the caller initiated the abort).
     *
     * Also closes the SSLSocket directly so any thread blocked in a native
     * read() call is unblocked immediately instead of waiting for the OS
     * idle timeout (~75-120 s).
     */
    public void abort() {
        isConnecting.set(false);
        socketAlive.set(false);
        voiceSessionActive.set(false);
        syncedVoiceSessionId = -1;
        if (packetParser != null) packetParser.abort();
        SSLSocket socketToClose = mSslSocket;
        if (socketToClose != null) {
            mSslSocket = null;
            try {
                socketToClose.close();
            } catch (IOException ignored) {
                // Ignore — we are tearing down the session anyway
            }
        }
        mMessageQueue.clear();
        Log.i(TAG, "abort() called — session terminated");
    }


    /**
     * Send a lightweight ping to keep the TCP connection alive.
     * Uses a RemoteActive message (the TV acknowledges but ignores it).
     *
     * @return true if the ping was sent successfully, false if the socket is dead.
     */
    public boolean sendPing() {
        if (!socketAlive.get()) {
            Log.w(TAG, "sendPing: socket not alive — connection may be lost");
            return false;
        }
        boolean ok = writeBytes(mMessageManager.createRemoteActive(1), "sendPing");
        if (!ok) {
            Log.e(TAG, "sendPing: write failed — connection lost");
        } else {
            Log.d(TAG, "sendPing: heartbeat sent");
        }
        return ok;
    }

    private boolean writeBytes(byte[] data, String callerTag) {
        if (!socketAlive.get()) {
            Log.w(TAG, callerTag + " skipped: socket not alive");
            return false;
        }
        try {
            outputStream.write(data);
            outputStream.flush();
            return true;
        } catch (IOException e) {
            Log.e(TAG, callerTag + " failed: " + e.getMessage());
            socketAlive.set(false);
            return false;
        }
    }

    public void sendCommand(Remotemessage.RemoteKeyCode remoteKeyCode, Remotemessage.RemoteDirection remoteDirection) {
        byte[] data = mMessageManager.createKeyCommand(remoteKeyCode, remoteDirection);
        if (!socketAlive.get()) {
            Log.w(TAG, "sendCommand: socket not alive — triggering fast reconnect and retry for " + remoteKeyCode);
            triggerReconnectAndSend(data, "sendCommand(" + remoteKeyCode + ")");
            return;
        }
        boolean ok = writeBytes(data, "sendCommand");
        if (ok) {
            Log.i(TAG, "sendCommand: " + remoteKeyCode + " dir=" + remoteDirection);
        } else {
            Log.w(TAG, "sendCommand write failed — triggering fast reconnect and retry for " + remoteKeyCode);
            triggerReconnectAndSend(data, "sendCommand(" + remoteKeyCode + ")");
        }
    }

    public void sendText(String text, int imeCounter, int fieldCounter) {
        if (text == null) text = "";
        int resolvedIme = syncedImeCounter >= 0 ? syncedImeCounter : Math.max(imeCounter, 0);
        int resolvedField = syncedFieldCounter >= 0 ? syncedFieldCounter : Math.max(fieldCounter, 0);
        byte[] part1 = mMessageManager.createImeText(
                text, resolvedIme, resolvedField, syncedTextInt5, syncedTextLabel, syncedAppInfo, false);
        byte[] part2 = mMessageManager.createImeBatchEditCompatV2(text, resolvedIme, resolvedField);
        byte[] combined = new byte[part1.length + part2.length];
        System.arraycopy(part1, 0, combined, 0, part1.length);
        System.arraycopy(part2, 0, combined, part1.length, part2.length);

        if (!socketAlive.get()) {
            Log.w(TAG, "sendText skipped: socket not alive — triggering reconnect and retry");
            triggerReconnectAndSend(combined, "sendText");
            return;
        }
        try {
            outputStream.write(combined);
            outputStream.flush();
            syncedImeCounter = resolvedIme;
            syncedFieldCounter = resolvedField;
        } catch (IOException e) {
            Log.e(TAG, "sendText failed: " + e.getMessage());
            socketAlive.set(false);
            triggerReconnectAndSend(combined, "sendText");
        }
    }

    public void sendAppCommand(String appLink) {
        byte[] data = mMessageManager.createAppCommand(appLink);
        if (!socketAlive.get()) {
            Log.w(TAG, "sendAppCommand skipped: socket not alive — triggering reconnect and retry");
            triggerReconnectAndSend(data, "sendAppCommand");
            return;
        }
        boolean ok = writeBytes(data, "sendAppCommand");
        if (!ok) {
            triggerReconnectAndSend(data, "sendAppCommand");
        }
    }

    public void sendImeEnter() {
        byte[] data = mMessageManager.createImeEnter();
        if (!socketAlive.get()) {
            triggerReconnectAndSend(data, "sendImeEnter");
            return;
        }
        boolean ok = writeBytes(data, "sendImeEnter");
        if (!ok) {
            triggerReconnectAndSend(data, "sendImeEnter");
        }
    }

    public void sendMessage(Remotemessage.RemoteMessage message) {
        if (message == null) return;
        writeBytes(mMessageManager.createRemoteMessage(message), "sendMessage");
    }



    // ─────────────────────────────────────────────────────────────────────────────
// ADD THESE METHODS TO RemoteSession.java
//
// Also add this field near the existing voiceSessionActive / syncedVoiceSessionId
// fields (they are already declared in your file):
//
//   private final java.util.concurrent.atomic.AtomicInteger voiceSessionCounter
//       = new java.util.concurrent.atomic.AtomicInteger(0);
// ─────────────────────────────────────────────────────────────────────────────

    // Counter used to generate unique, incrementing session IDs.
    // Declared alongside the other voice fields near the top of RemoteSession.
    private final java.util.concurrent.atomic.AtomicInteger voiceSessionCounter =
            new java.util.concurrent.atomic.AtomicInteger(0);

    /**
     * Opens a voice session on the TV (sends RemoteVoiceBegin).
     *
     * Returns the session ID that was assigned, or -1 if the socket is not alive
     * or a voice session is already active.
     *
     * Call sendVoiceChunk() repeatedly with PCM data, then stopVoice() when done.
     */
    public int startVoice() {
        if (!socketAlive.get()) {
            Log.w(TAG, "startVoice: socket not alive");
            return -1;
        }
        if (voiceSessionActive.get()) {
            Log.w(TAG, "startVoice: voice session already active (id=" + syncedVoiceSessionId + ")");
            return syncedVoiceSessionId;
        }

        // Assign a new session ID (1-based, wraps safely)
        int sessionId = voiceSessionCounter.incrementAndGet();
        syncedVoiceSessionId = sessionId;

        boolean ok = writeBytes(mMessageManager.createVoiceBegin(sessionId), "startVoice");
        if (ok) {
            voiceSessionActive.set(true);
            Log.i(TAG, "startVoice: RemoteVoiceBegin sent (sessionId=" + sessionId + ")");
        } else {
            syncedVoiceSessionId = -1;
            return -1;
        }

        return sessionId;
    }

    /**
     * Streams a chunk of raw 16-bit PCM, mono, 8 kHz audio to the TV.
     *
     * Must be called after startVoice() and before stopVoice().
     * Intended to be called from VoiceManager's ChunkCallback.
     *
     * @param pcmChunk  raw PCM bytes (~20 KB recommended, matching VoiceManager.CHUNK_SIZE_BYTES)
     */
    public void sendVoiceChunk(byte[] pcmChunk) {
        if (!voiceSessionActive.get()) {
            Log.w(TAG, "sendVoiceChunk: no active voice session — call startVoice() first");
            return;
        }
        if (pcmChunk == null || pcmChunk.length == 0) return;

        writeBytes(
                mMessageManager.createVoicePayload(syncedVoiceSessionId, pcmChunk),
                "sendVoiceChunk"
        );
    }

    /**
     * Ends the voice session (sends RemoteVoiceEnd).
     * The TV will process all received audio and perform speech recognition.
     *
     * Safe to call even if no session is active (no-op).
     */
    public void stopVoice() {
        if (!voiceSessionActive.get()) {
            Log.w(TAG, "stopVoice: no active voice session");
            return;
        }

        int sessionId = syncedVoiceSessionId;

        // Clear state before writing so abort() can't race us
        voiceSessionActive.set(false);
        syncedVoiceSessionId = -1;

        writeBytes(mMessageManager.createVoiceEnd(sessionId), "stopVoice");
        Log.i(TAG, "stopVoice: RemoteVoiceEnd sent (sessionId=" + sessionId + ")");
    }

    /**
     * Convenience: returns true if a voice session is currently streaming.
     */
    public boolean isVoiceActive() {
        return voiceSessionActive.get();
    }


    public boolean isSocketAlive() {
        return socketAlive.get();
    }

    public interface RemoteSessionListener {
        void onConnected();
        void onSslError() throws GeneralSecurityException, IOException, InterruptedException, PairingException;
        void onDisconnected();
        void onImeShow(String text, int fieldCounter);
        void onError(String message);

    }
    public interface VolumeListener {
        void onVolumeChanged(int level, int maxLevel, boolean muted);
    }

// ── Add this field near the other volatile fields ─────────────────────────────

    private volatile VolumeListener mVolumeListener;

    public void setVolumeListener(VolumeListener listener) {
        mVolumeListener = listener;
    }


    private static final int WAKE_PORT = 6465;

    private static boolean isPortOpen(String host, int port, int timeoutMs) {
        try (java.net.Socket sock = new java.net.Socket()) {
            sock.connect(new java.net.InetSocketAddress(host, port), timeoutMs);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private SSLSocket knockWakePort(SSLSocketFactory factory) {
        try {
            Log.i(TAG_WAKE, "Remote port closed, knocking wake port " + WAKE_PORT);
            SSLSocket s = (SSLSocket) factory.createSocket();
            s.connect(new java.net.InetSocketAddress(mHost, WAKE_PORT), 3000);
            s.setSoTimeout(3000);
            s.setUseClientMode(true);
            s.setKeepAlive(true);
            try { s.startHandshake(); } catch (Exception e) {
                Log.w(TAG_WAKE, "Wake port handshake failed (TCP knock still sent): " + e);
            }
            return s;
        } catch (Exception e) {
            Log.w(TAG_WAKE, "Wake port knock failed: " + e);
            return null;
        }
    }

    private static final String TAG_WAKE = "AtvWakePort";
}
