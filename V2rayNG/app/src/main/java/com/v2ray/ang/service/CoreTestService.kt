package com.v2ray.ang.service

import android.app.Service
import android.content.Intent
import android.os.IBinder
import com.v2ray.ang.AppConfig
import com.v2ray.ang.R
import com.v2ray.ang.dto.TestServiceMessage
import com.v2ray.ang.extension.serializable
import com.v2ray.ang.handler.MmkvManager
import com.v2ray.ang.core.CoreNativeManager
import com.v2ray.ang.util.LogUtil
import com.v2ray.ang.util.MessageUtil
import com.v2ray.ang.util.NotificationHelper
import com.v2ray.ang.enums.NotificationChannelType
import java.util.Collections

class CoreTestService : Service() {

    // manage active batch workers so each batch is independent and cancellable
    private val activeWorkers = Collections.synchronizedList(mutableListOf<RealPingWorkerService>())

    /**
     * Initializes the V2Ray environment.
     */
    override fun onCreate() {
        super.onCreate()
        CoreNativeManager.initCoreEnv(this)
    }

    /**
     * Binds the service.
     * @param intent The intent.
     * @return The binder.
     */
    override fun onBind(intent: Intent?): IBinder? {
        return null
    }

    /**
     * Cleans up resources when the service is destroyed.
     */
    override fun onDestroy() {
        LogUtil.i(AppConfig.TAG, "CoreTestService is being destroyed, cancelling ${activeWorkers.size} active workers")
        // cancel any active workers
        val snapshot = ArrayList(activeWorkers)
        snapshot.forEach { it.cancel() }
        activeWorkers.clear()
        NotificationHelper.stopForeground(this)
        super.onDestroy()
    }

    /**
     * Handles the start command for the service.
     * @param intent The intent.
     * @param flags The flags.
     * @param startId The start ID.
     * @return The start mode.
     */
    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        val message = intent?.serializable<TestServiceMessage>("content")
        if (message == null) {
            stopSelf(startId)
            return START_NOT_STICKY
        }

        when (message.key) {
            AppConfig.MSG_MEASURE_CONFIG_START -> {
                LogUtil.i(AppConfig.TAG, "CoreTestService starting worker   subscription ${message.subscriptionId}")

                // start foreground immediately to satisfy startForegroundService timing requirement
                NotificationHelper.startForeground(
                    this,
                    NotificationChannelType.CORE_TEST,
                    getString(R.string.app_name),
                    getString(R.string.title_real_ping_all_server)
                )

                val guidsList = if (message.serverGuids.isNotEmpty()) {
                    message.serverGuids
                } else if (message.subscriptionId.isNotEmpty()) {
                    MmkvManager.decodeServerList(message.subscriptionId)
                } else {
                    MmkvManager.decodeAllServerList()
                }

                if (guidsList.isNotEmpty()) {
                    lateinit var worker: RealPingWorkerService
                    worker = RealPingWorkerService(
                        context = this,
                        guids = guidsList,
                        onProgress = { progress ->
                            NotificationHelper.updateNotification(
                                channelType = NotificationChannelType.CORE_TEST,
                                context = this,
                                content = getString(R.string.connection_runing_task_left, progress)
                            )
                            // notify UI about progress updates
                            MessageUtil.sendMsg2UI(this@CoreTestService, AppConfig.MSG_MEASURE_CONFIG_NOTIFY, progress)
                        },
                        onFinish = { status ->
                            // notify UI and remove the worker from active list when finished
                            MessageUtil.sendMsg2UI(this@CoreTestService, AppConfig.MSG_MEASURE_CONFIG_FINISH, status)
                            activeWorkers.remove(worker)
                            if (activeWorkers.isEmpty()) {
                                NotificationHelper.stopForeground(this@CoreTestService)
                                stopSelf()
                            }

                        }
                    )
                    activeWorkers.add(worker)
                    worker.start()
                } else {
                    NotificationHelper.stopForeground(this)
                    stopSelf(startId)
                }
            }

            AppConfig.MSG_MEASURE_CONFIG_CANCEL -> {
                LogUtil.i(AppConfig.TAG, "CoreTestService received cancel message, cancelling ${activeWorkers.size} active workers")
                // cancel all running batch workers independently
                val snapshot = ArrayList(activeWorkers)
                snapshot.forEach { it.cancel() }
                activeWorkers.clear()
                NotificationHelper.stopForeground(this)
                stopSelf()
            }

            else -> {
                NotificationHelper.stopForeground(this)
                stopSelf(startId)
            }
        }
        return START_NOT_STICKY
    }
}