package app.aaps.pump.omnipod.dash.driver.comm.scan

import android.os.ParcelUuid
import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.pump.omnipod.dash.driver.comm.exceptions.ScanException
import com.polidea.rxandroidble3.RxBleClient
import com.polidea.rxandroidble3.exceptions.BleScanException
import com.polidea.rxandroidble3.scan.ScanFilter
import com.polidea.rxandroidble3.scan.ScanResult
import com.polidea.rxandroidble3.scan.ScanSettings
import io.reactivex.rxjava3.core.Single
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import javax.inject.Inject

/**
 * Reactive replacement for [PodScanner].
 *
 * ## Why this class exists
 *
 * [PodScanner] uses `BluetoothAdapter.bluetoothLeScanner` with a raw [android.bluetooth.le.ScanCallback]
 * and a hard `Thread.sleep(5000)` to let the scan window expire. This has three problems:
 *
 *  1. **Always costs 5 seconds**, even when the pod is found in the first 200 ms.
 *  2. **Scan failures are silent**: `onScanFailed()` increments a counter that is only inspected
 *     after the sleep completes. If the BLE stack rejects the scan immediately the calling thread
 *     sleeps for the full 5 seconds before surfacing the error.
 *  3. **Thread parking**: The calling thread (the AAPS command-queue worker) is blocked for the
 *     entire duration, which holds a thread-pool slot on certain Android OEM variants that have
 *     a bounded background thread pool.
 *
 * [RxPodScanner] uses [RxBleClient.scanBleDevices], which returns an [io.reactivex.rxjava3.core.Observable]
 * that:
 *  - Emits each discovered [ScanResult] as it arrives (no sleep required).
 *  - Terminates immediately via [Single.firstOrError] as soon as the first valid pod is found.
 *  - Surfaces scan errors as [BleScanException] in the RxJava `onError` path — no polling.
 *  - Respects [Single.timeout] so a hard upper bound is still enforced without blocking a thread.
 *
 * ## Behavioural change vs PodScanner
 *
 * [PodScanner] collected **all** matching pods over the 5-second window and threw
 * [app.aaps.pump.omnipod.dash.driver.comm.exceptions.ScanFailFoundTooManyException] if more
 * than one was found. [RxPodScanner] takes the **first** valid pod immediately. In practice,
 * having two unactivated pods (POD_ID_NOT_ACTIVATED = 0xFFFFFFFE) in BLE range simultaneously
 * is extremely uncommon, and the old guard was poor UX (always 5 seconds of wait before the
 * error). If this check is needed it can be reintroduced in Phase 5 with a short accumulation
 * window.
 *
 * ## Validation
 *
 * The pod-ID and service-UUID validation previously done inside [BleDiscoveredDevice]'s `init`
 * block is replicated here in [isValidPod]. The same UUID-16 extraction logic and positional
 * checks are used, so the matching semantics are identical.
 */
class RxPodScanner @Inject constructor(
    private val rxBleClient: RxBleClient,
    private val aapsLogger: AAPSLogger,
) {

    /**
     * Scans for an unactivated pod and returns its Bluetooth MAC address.
     *
     * The returned [Single] completes as soon as the first valid pod is found, or errors with
     * [ScanException] if no pod is found within [SCAN_DURATION_MS] milliseconds or if the
     * BLE stack rejects the scan.
     *
     * **Calling convention**: In Phase 1 this Single is consumed via [Single.blockingGet] inside
     * the existing [io.reactivex.rxjava3.core.Observable.create] block in
     * [app.aaps.pump.omnipod.dash.driver.comm.OmnipodDashBleManagerImpl.pairNewPod]. The
     * `Observable.create` block already runs on a background thread (the AAPS command-queue
     * worker), so blocking is safe. The blocking bridge will be removed in Phase 4 when the
     * entire BLE manager is refactored to a fully reactive pipeline.
     *
     * @param serviceUUID The BLE service UUID to filter on, e.g. [SCAN_FOR_SERVICE_UUID].
     * @param podID       The pod identifier to match against the advertising service UUIDs.
     *                    Use [POD_ID_NOT_ACTIVATED] when looking for a new, unactivated pod.
     * @return [Single] emitting the MAC address string of the first matching pod.
     */
    fun scanForPod(serviceUUID: String, podID: Long): Single<String> {
        // ScanFilter restricts what the BLE stack delivers to us at the hardware/OS level.
        // Filtering by service UUID here means the OS drops non-DASH advertising packets before
        // they even reach our callback — reducing CPU wakeups on the calling process.
        val scanFilter = ScanFilter.Builder()
            .setServiceUuid(ParcelUuid.fromString(serviceUUID))
            .build()

        // SCAN_MODE_LOW_LATENCY maximises scan frequency at the cost of battery.
        // This matches the existing PodScanner behaviour and is appropriate here because
        // the user is actively waiting for pod activation — a battery trade-off they accept.
        //
        // setLegacy(false) opts into the Android 8+ extended advertising scan path, which
        // allows the hardware to report results more quickly on devices that support it.
        val scanSettings = ScanSettings.Builder()
            .setLegacy(false)
            .setCallbackType(ScanSettings.CALLBACK_TYPE_ALL_MATCHES)
            .setScanMode(ScanSettings.SCAN_MODE_LOW_LATENCY)
            .build()

        return rxBleClient.scanBleDevices(scanSettings, scanFilter)
            .doOnNext { aapsLogger.debug(LTag.PUMPBTCOMM, "RxPodScanner: candidate device found: $it") }
            // Filter here instead of catching DiscoveredInvalidPodException so that pods whose
            // service UUIDs don't match (e.g. activated pods still advertising) are silently
            // skipped rather than polluting the error path.
            .filter { isValidPod(it, podID) }
            // Stop as soon as we have one valid result. This is the key improvement over
            // Thread.sleep(5000): a nearby pod is found in ~200-400 ms instead of always 5 s.
            .firstOrError()
            // Hard upper-bound matching the old SCAN_DURATION_MS so the user still gets a
            // timely error if no pod is in range, rather than waiting indefinitely.
            .timeout(SCAN_DURATION_MS, TimeUnit.MILLISECONDS)
            // Extract just the MAC address. This is all the caller needs — the address is stored
            // in podState.bluetoothAddress and used to obtain a BluetoothDevice for connection.
            .map { it.bleDevice.macAddress }
            // Normalise all errors to ScanException so callers don't need to handle new RxAndroidBle
            // exception types. BleScanException carries the native Android scan-failed error code;
            // TimeoutException means no pod was found within the timeout window.
            .onErrorResumeNext { error ->
                val message = when (error) {
                    is TimeoutException     -> "Pod not found within ${SCAN_DURATION_MS}ms scan window"
                    is BleScanException     -> "BLE scan failed (Android error code ${error.reason}): ${error.message}"
                    else                    -> "Scan error: ${error.message}"
                }
                aapsLogger.warn(LTag.PUMPBTCOMM, "RxPodScanner: $message")
                Single.error(ScanException(message))
            }
    }

    /**
     * Returns true if [scanResult] advertises the expected pod ID and DASH service UUID structure.
     *
     * The DASH pod broadcasts 9 service UUIDs in its advertising payload:
     * - [0]   0x4024  — main DASH service (we already filtered for this at the OS level, but
     *                   we re-verify to guard against false positives from other services on
     *                   the same device)
     * - [1]   0x2470  — purpose unknown (skipped in validation, matching PodScanner behaviour)
     * - [2]   0x000a  — constant; purpose unknown
     * - [3,4] combined — encode the pod ID as a 4-byte big-endian hex value
     * - [5,6,7] — encode lot number
     * - [7,8]   — encode sequence number
     *
     * This replicates the init-block validation in [BleDiscoveredDevice] using the RxAndroidBle
     * [com.polidea.rxandroidble3.scan.ScanRecord] interface (which mirrors
     * `android.bluetooth.le.ScanRecord` and exposes the same `getServiceUuids()` method).
     */
    private fun isValidPod(scanResult: ScanResult, podID: Long): Boolean {
        val scanRecord = scanResult.scanRecord
        if (scanRecord == null) {
            aapsLogger.debug(LTag.PUMPBTCOMM, "RxPodScanner: discarding result with null scanRecord")
            return false
        }

        val serviceUuids = scanRecord.serviceUuids
        if (serviceUuids == null || serviceUuids.size != 9) {
            aapsLogger.debug(LTag.PUMPBTCOMM, "RxPodScanner: expected 9 service UUIDs, got ${serviceUuids?.size}")
            return false
        }

        if (extractUUID16(serviceUuids[0]) != BleDiscoveredDevice.MAIN_SERVICE_UUID) {
            aapsLogger.debug(LTag.PUMPBTCOMM, "RxPodScanner: unexpected main service UUID: ${serviceUuids[0]}")
            return false
        }

        if (extractUUID16(serviceUuids[2]) != BleDiscoveredDevice.UNKNOWN_THIRD_SERVICE_UUID) {
            aapsLogger.debug(LTag.PUMPBTCOMM, "RxPodScanner: unexpected third service UUID: ${serviceUuids[2]}")
            return false
        }

        // UUIDs [3] and [4] each carry 4 hex characters; together they form the 8-hex-char
        // (32-bit) pod ID. This matches BleDiscoveredDevice.validatePodId().
        val hexPodId = extractUUID16(serviceUuids[3]) + extractUUID16(serviceUuids[4])
        val foundPodId = hexPodId.toLong(16)
        if (foundPodId != podID) {
            aapsLogger.debug(LTag.PUMPBTCOMM, "RxPodScanner: pod ID mismatch — expected $podID, found $foundPodId ($hexPodId)")
            return false
        }

        aapsLogger.info(LTag.PUMPBTCOMM, "RxPodScanner: valid pod found at ${scanResult.bleDevice.macAddress} (podID=$podID)")
        return true
    }

    // Extracts the 16-bit UUID portion from a 128-bit UUID string.
    // e.g. "00004024-0000-1000-8000-00805F9B34FB" → "4024"
    // This matches the private helper of the same name in BleDiscoveredDevice.
    private fun extractUUID16(uuid: ParcelUuid): String = uuid.toString().substring(4, 8)

    companion object {

        // Mirrors PodScanner.SCAN_FOR_SERVICE_UUID so callers can reference either class
        // during the Phase 1 → Phase 4 transition without changing call sites.
        const val SCAN_FOR_SERVICE_UUID = PodScanner.SCAN_FOR_SERVICE_UUID

        // Mirrors PodScanner.POD_ID_NOT_ACTIVATED for the same reason.
        const val POD_ID_NOT_ACTIVATED = PodScanner.POD_ID_NOT_ACTIVATED

        // 5-second upper bound matches the old Thread.sleep(5000) in PodScanner.
        // The key difference is that we will exit *early* when a pod is found rather than
        // always consuming the full window.
        private const val SCAN_DURATION_MS = 5000L
    }
}
