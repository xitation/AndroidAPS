# Phase 1 — Scanning

**Status**: Complete
**Risk level**: Low — scanning is entirely isolated from the connection path; no connection
code is touched in this phase.

---

## Problem Being Solved

`PodScanner.scanForPod()` uses a raw `android.bluetooth.le.ScanCallback` subclass (`ScanCollector`)
and calls `Thread.sleep(5000)` to hold the scan window open. This has three concrete problems:

### 1. Always costs 5 seconds

The scan window is fixed. Even when the pod is 10 cm from the phone and responds in the first
100 ms of advertising, the calling thread sleeps for the full 5 seconds before `stopScan()` is
called and results are inspected.

```kotlin
// PodScanner.kt:29 — the offending line
Thread.sleep(SCAN_DURATION_MS.toLong())   // always 5000 ms
```

### 2. Scan failures are silent until the sleep ends

`ScanCollector.onScanFailed()` increments a counter but does not unblock the sleeping thread.
If the Android BLE stack rejects the scan call immediately (e.g., because too many apps are
scanning simultaneously — the Android 7+ limit is 5 apps), the calling thread sleeps the full
5 seconds before surfacing the error.

```kotlin
// ScanCollector.kt:22-25
override fun onScanFailed(errorCode: Int) {
    logger.warn(LTag.PUMPBTCOMM, "Scan failed with errorCode: $errorCode")
    super.onScanFailed(errorCode)    // counter NOT incremented — bug: scanFailed stays 0
    // calling thread is still asleep
}
```

### 3. Thread parking

The calling context is the AAPS command-queue background thread, running inside
`OmnipodDashBleManagerImpl.pairNewPod()`'s `Observable.create` lambda. Parking this thread for
5 seconds holds a slot in the thread pool, which on certain Android OEM variants with bounded
background pools can delay other AAPS operations.

---

## Solution

`RxPodScanner` uses `RxBleClient.scanBleDevices()`, which returns a cold
`Observable<ScanResult>` that emits each discovered device as it is found by the BLE hardware.
We compose:

```
scanBleDevices()          — emits on each BLE advertising packet matching the service UUID filter
  .filter { isValidPod }  — drops devices that fail the DASH pod validation checks
  .firstOrError()         — completes immediately on the first valid result
  .timeout(5, SECONDS)    — hard upper bound: errors with TimeoutException if nothing found
  .map { macAddress }     — extract just the string the caller needs
  .onErrorResumeNext      — normalise BleScanException/TimeoutException → ScanException
```

---

## Files Changed

### `pump/omnipod/dash/build.gradle.kts`

**Change**: Added `implementation(libs.com.polidea.rxandroidble3)`.

**Why**: The library was already declared in `gradle/libs.versions.toml` at version `1.19.0`
(used by the `eopatch` module) but was not a declared dependency of the DASH module. Dagger
cannot resolve `RxBleClient` without this, and the Kotlin compiler cannot find the scan classes.

```kotlin
// Before
api(libs.com.github.guepardoapps.kulid)

// After
api(libs.com.github.guepardoapps.kulid)
implementation(libs.com.polidea.rxandroidble3)
```

---

### `di/OmnipodDashModule.kt`

**Change**: Added a `companion object` providing `RxBleClient` as a `@Singleton`.

**Why a companion object**: The module class is `abstract` because all its pre-existing methods
use `@Binds`, which requires abstract methods in Dagger. Dagger does not permit non-abstract
`@Provides` methods in an abstract class body. A `companion object` annotated `@JvmStatic`
generates a true Java static method in the bytecode, which Dagger can call at zero cost without
instantiating the companion.

**Why `@Singleton`**: `RxBleClient` holds a reference to the system `BluetoothAdapter` and
manages scan/connection state internally. Creating multiple instances risks adapter state
conflicts, duplicate scan registrations, and connection-state inconsistencies. One instance
per application process is the correct model.

**Why `context.applicationContext`**: The DASH module lives in the `@Singleton` Dagger scope.
Passing an activity or service `Context` would cause a memory leak because `RxBleClient` (a
singleton) would hold a reference to a shorter-lived component. `applicationContext` safely
matches the singleton lifetime.

```kotlin
companion object {
    @JvmStatic
    @Provides
    @Singleton
    fun provideRxBleClient(context: Context): RxBleClient =
        RxBleClient.create(context.applicationContext)
}
```

---

### `scan/RxPodScanner.kt` (new file)

**Location**: `pump/omnipod/dash/src/main/kotlin/app/aaps/pump/omnipod/dash/driver/comm/scan/`

**Return type**: `Single<String>` (the MAC address string).

**Why `Single<String>` and not `Single<BleDiscoveredDevice>`**:

`BleDiscoveredDevice`'s constructor takes `android.bluetooth.le.ScanResult` and
`android.bluetooth.le.ScanRecord` — Android native types. The RxAndroidBle library provides its
own `com.polidea.rxandroidble3.scan.ScanRecord` interface, which mirrors the same methods but is
a different type. Adapting between them would require modifying `BleDiscoveredDevice`, creating
unnecessary scope creep in Phase 1.

The only thing `pairNewPod()` ever did with the scan result was:
```kotlin
podScanner.scanForPod(...).scanResult.device.address
```
So `Single<String>` is sufficient and minimal.

**Validation logic**:

The DASH pod broadcasts 9 service UUIDs in its advertising payload. `BleDiscoveredDevice`'s
`init` block validated these. The same checks are replicated in `RxPodScanner.isValidPod()`
using the RxAndroidBle `ScanRecord` interface, which also exposes `getServiceUuids(): List<ParcelUuid>`.

```kotlin
// UUID positions validated (same as BleDiscoveredDevice):
// [0] — must be "4024" (main DASH service)
// [1] — skipped (0x2470, unknown purpose)
// [2] — must be "000a" (constant, unknown purpose)
// [3]+[4] — combined 8-char hex string must equal podID
// [5][6][7] — lot number (not validated, just present)
// [7][8]    — sequence number (not validated, just present)
```

**Behavioural change — removal of `ScanFailFoundTooManyException`**:

The old scanner collected all pods found in the 5-second window and threw
`ScanFailFoundTooManyException` if more than one unactivated pod was found. The new scanner
takes the first valid pod immediately (`firstOrError()`).

This change is intentional. Having two unactivated pods (`POD_ID_NOT_ACTIVATED = 0xFFFFFFFE`)
within BLE range simultaneously is extremely uncommon in practice. The old guard provided poor
UX (always 5 seconds of wait just to tell the user something was wrong). If this check is
needed it can be reintroduced in Phase 5 using a short accumulation window.

**Error normalisation**:

```kotlin
.onErrorResumeNext { error ->
    val message = when (error) {
        is TimeoutException  -> "Pod not found within 5000ms scan window"
        is BleScanException  -> "BLE scan failed (Android error code ${error.reason}): ..."
        else                 -> "Scan error: ..."
    }
    Single.error(ScanException(message))
}
```

All errors are wrapped in the existing `ScanException` so no changes are needed in the
catch blocks of `OmnipodDashBleManagerImpl`. `BleScanException` carries the native Android
`ScanCallback.SCAN_FAILED_*` error code in `.reason`.

**Constants delegation**:

```kotlin
companion object {
    const val SCAN_FOR_SERVICE_UUID = PodScanner.SCAN_FOR_SERVICE_UUID
    const val POD_ID_NOT_ACTIVATED  = PodScanner.POD_ID_NOT_ACTIVATED
    private const val SCAN_DURATION_MS = 5000L
}
```

Constants delegate to `PodScanner` rather than duplicating them. This prevents divergence
during the transition period. When `PodScanner` is deleted in Phase 4, these will become
the authoritative values.

---

### `scan/PodScanner.kt`

**Change**: `@Deprecated` annotation and KDoc added to the class declaration. No logic changed.

**Why keep it**: `RxPodScanner.companion` references `PodScanner.SCAN_FOR_SERVICE_UUID` and
`PodScanner.POD_ID_NOT_ACTIVATED`. Deleting `PodScanner` now would require moving those
constants elsewhere. The clean time to delete it is Phase 4, when the entire scan package is
reviewed together with `ScanCollector` and `BleDiscoveredDevice`.

```kotlin
@Deprecated("Use RxPodScanner instead. Scheduled for deletion in Phase 4.")
class PodScanner(...)
```

---

### `comm/OmnipodDashBleManagerImpl.kt`

**Three changes**:

#### 1. Constructor parameter added

```kotlin
// Before
class OmnipodDashBleManagerImpl @Inject constructor(
    private val context: Context,
    private val aapsLogger: AAPSLogger,
    private val podState: OmnipodDashPodStateManager,
    private val config: Config,
    private val preferences: Preferences,
)

// After
class OmnipodDashBleManagerImpl @Inject constructor(
    private val context: Context,
    private val aapsLogger: AAPSLogger,
    private val podState: OmnipodDashPodStateManager,
    private val config: Config,
    private val preferences: Preferences,
    private val rxPodScanner: RxPodScanner,   // ← added
)
```

Dagger injects `RxPodScanner` automatically because it has `@Inject constructor` and its
only dependency (`RxBleClient`) is now provided by the module.

#### 2. Scan block replaced in `pairNewPod()`

```kotlin
// Before
val adapter = bluetoothAdapter
    ?: throw ConnectException("Bluetooth not available")
val podScanner = PodScanner(aapsLogger, adapter)
val podAddress = podScanner.scanForPod(
    PodScanner.SCAN_FOR_SERVICE_UUID,
    PodScanner.POD_ID_NOT_ACTIVATED
).scanResult.device.address

// After
val podAddress = rxPodScanner.scanForPod(
    RxPodScanner.SCAN_FOR_SERVICE_UUID,
    RxPodScanner.POD_ID_NOT_ACTIVATED
).blockingGet()
```

**Why `.blockingGet()`**: The lambda inside `Observable.create` runs on the AAPS command-queue
background thread. This thread is already blocked for the entire pairing flow (connection → LTK
exchange → session establishment). Blocking it during scanning too is no different from the
previous `Thread.sleep`. This is an explicit temporary bridge, documented in an inline comment,
to be removed in Phase 4 when the full BLE manager is refactored to return a reactive chain
end-to-end.

#### 3. `adapter` acquisition moved after scan

```kotlin
// Before: adapter obtained before scan (needed to construct PodScanner)
val adapter = bluetoothAdapter ?: throw ...
val podScanner = PodScanner(aapsLogger, adapter)
...
val podDevice = adapter.getRemoteDevice(podAddress)

// After: adapter obtained after scan (only needed for getRemoteDevice)
val podAddress = rxPodScanner.scanForPod(...).blockingGet()
...
val adapter = bluetoothAdapter ?: throw ...
val podDevice = adapter.getRemoteDevice(podAddress)
```

`RxBleClient` accesses the `BluetoothAdapter` internally, so the caller no longer needs the
adapter reference until `getRemoteDevice()`. Moving the null-check later makes the code
express its actual dependency: the adapter is needed only for the connection, not the scan.

#### 4. Import updated

```kotlin
// Removed
import app.aaps.pump.omnipod.dash.driver.comm.scan.PodScanner

// Added
import app.aaps.pump.omnipod.dash.driver.comm.scan.RxPodScanner
```

---

## Testing This Phase

Manual verification steps (hardware required):

1. Build and install the app.
2. Open a new pod activation.
3. Observe that the "Scanning" state completes in under 1 second rather than exactly 5 seconds
   when the pod is nearby.
4. Test error path: attempt activation with no pod in range. After 5 seconds, the scan should
   time out with a `ScanException` and the activation wizard should display an appropriate error.
5. Test Bluetooth off: disable Bluetooth before scanning. The `BleScanException` should be
   normalised to `ScanException` and displayed correctly.

Unit test coverage: `RxPodScanner` can be unit tested using `MockRxAndroidBle` (added in
Phase 5). The validation logic in `isValidPod()` is pure (no Android system calls) and can be
tested with a fabricated `ScanResult` stub even before Phase 5.

---

## What Is Not Changed in This Phase

- Connection establishment (`Connection.kt`, `BleCommCallbacks.kt`) — Phase 2.
- Message I/O (`MessageIO.kt`, `BleIO.kt`, `IncomingPackets.kt`) — Phase 3.
- Session establishment (`Session.kt`, `SessionEstablisher.kt`) — unchanged throughout.
- All pod commands, responses, state, history — unchanged throughout.
