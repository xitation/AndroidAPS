package app.aaps.pump.omnipod.dash.driver.comm.scan

import android.bluetooth.BluetoothAdapter
import android.bluetooth.le.ScanFilter
import android.bluetooth.le.ScanSettings
import android.os.ParcelUuid
import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.pump.omnipod.dash.driver.comm.exceptions.ScanException
import app.aaps.pump.omnipod.dash.driver.comm.exceptions.ScanFailFoundTooManyException
import java.util.Arrays

/**
 * @deprecated Replaced by [RxPodScanner].
 *
 * This class uses a raw [android.bluetooth.le.ScanCallback] and `Thread.sleep(5000)` which
 * always blocks the calling thread for the full scan window even when a pod is found immediately.
 * [RxPodScanner] uses RxAndroidBle's reactive scan observable and exits as soon as the first
 * valid pod is found (~200-400 ms typical vs 5000 ms fixed).
 *
 * Scheduled for removal in Phase 4 of the RxAndroidBle migration.
 */
@Deprecated("Use RxPodScanner instead. Scheduled for deletion in Phase 4.")
class PodScanner(private val logger: AAPSLogger, private val bluetoothAdapter: BluetoothAdapter) {

    @Throws(InterruptedException::class, ScanException::class)
    fun scanForPod(serviceUUID: String?, podID: Long): BleDiscoveredDevice {
        val scanner = bluetoothAdapter.bluetoothLeScanner
        val filter = ScanFilter.Builder()
            .setServiceUuid(ParcelUuid.fromString(serviceUUID))
            .build()
        val scanSettings = ScanSettings.Builder()
            .setLegacy(false)
            .setCallbackType(ScanSettings.CALLBACK_TYPE_ALL_MATCHES)
            .setScanMode(ScanSettings.SCAN_MODE_LOW_LATENCY)
            .build()
        val scanCollector = ScanCollector(logger, podID)
        logger.debug(LTag.PUMPBTCOMM, "Scanning with filters: $filter settings$scanSettings")
        scanner.startScan(Arrays.asList(filter), scanSettings, scanCollector)
        Thread.sleep(SCAN_DURATION_MS.toLong())
        scanner.flushPendingScanResults(scanCollector)
        scanner.stopScan(scanCollector)
        val collected = scanCollector.collect()
        if (collected.isEmpty()) {
            throw ScanException("Not found")
        } else if (collected.size > 1) {
            throw ScanFailFoundTooManyException(collected)
        }
        return collected[0]
    }

    companion object {

        const val SCAN_FOR_SERVICE_UUID = "00004024-0000-1000-8000-00805F9B34FB"
        const val POD_ID_NOT_ACTIVATED = 0xFFFFFFFEL
        private const val SCAN_DURATION_MS = 5000
    }
}
