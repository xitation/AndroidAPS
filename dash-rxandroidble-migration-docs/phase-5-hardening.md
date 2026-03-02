# Phase 5 — Hardening, Reliability & Test Coverage

**Status**: Planned (not started)
**Depends on**: Phase 4 complete (fully reactive pipeline, no blocking bridges)
**Risk level**: Low-Medium — each item is an additive improvement; none removes working code.
Items can be implemented independently and in any order.

---

## Overview

Phase 5 adds the reliability improvements that are only possible once the full reactive pipeline
is in place. None of these could be added incrementally to the old blocking implementation
without significant restructuring.

---

## Item 1 — GATT 133 Retry with Exponential Backoff

**What it is**: GATT status 133 (`BluetoothGatt.GATT_ERROR`) is the most commonly reported
BLE connection failure on Android, caused by firmware bugs in vendor BLE stacks (particularly
Samsung, Xiaomi, and older Huawei). It is almost always transient — the same connection
succeeds on the second attempt 500 ms later.

**Current behaviour**: `BleGattException(status=GATT_ERROR)` propagates as
`FailedToConnectException` with no retry. The user sees a connection error and must manually
retry the operation.

**Proposed**: Add `retryWhen` on `dashConnection.connect()` inside the `OmnipodDashBleManagerImpl`
connect chain:

```kotlin
dashConnection.connect(timeoutMs)
    .retryWhen { errors ->
        errors.zipWith(Observable.range(1, MAX_GATT_RETRIES)) { error, attempt ->
            if (error is BleGattException && error.status == BleGattOperationState.GATT_ERROR) {
                aapsLogger.warn(LTag.PUMPBTCOMM, "GATT 133 on attempt $attempt, retrying...")
                attempt
            } else {
                throw error   // non-retryable — propagate immediately
            }
        }.flatMap { attempt ->
            Observable.timer(attempt * GATT_RETRY_DELAY_MS, TimeUnit.MILLISECONDS)
        }
    }
```

**Constants**:
- `MAX_GATT_RETRIES = 3`
- `GATT_RETRY_DELAY_MS = 500L`

**Imports needed**: `com.polidea.rxandroidble3.exceptions.BleGattException`,
`com.polidea.rxandroidble3.exceptions.BleGattOperationState`.

---

## Item 2 — Proactive Bluetooth State Observation

**What it is**: Subscribe to `rxBleClient.observeStateChanges()` to detect Bluetooth adapter
state changes (airplane mode, Bluetooth toggle, location permission revoked) without waiting
for a connection attempt to fail.

**Current behaviour**: The app discovers Bluetooth is off only when the next `connect()` call
fails. The pod state may show "Connected" in the UI even after the user toggled Bluetooth off.

**Proposed**: Add a subscription in `OmnipodDashBleManagerImpl` (or `OmnipodDashPumpPlugin`)
that reacts to state changes:

```kotlin
private val stateDisposable: Disposable = rxBleClient.observeStateChanges()
    .filter { it != RxBleClient.State.READY }
    .subscribe { state ->
        aapsLogger.warn(LTag.PUMPBTCOMM, "BLE state changed to $state — disconnecting")
        dashConnection.disconnect()
        // Emit a UI event so the overview fragment updates the connection indicator
        rxBus.send(EventOmnipodDashPumpValuesChanged())
    }
```

Dispose `stateDisposable` when the plugin is stopped (in `onStop()` or equivalent lifecycle
hook in `OmnipodDashPumpPlugin`).

**States emitted by `RxBleClient.State`**:
- `READY` — Bluetooth on, location available, permissions granted
- `BLUETOOTH_NOT_ENABLED`
- `BLUETOOTH_NOT_AVAILABLE` — device has no BLE hardware
- `LOCATION_SERVICES_NOT_ENABLED` — Android 6-11 require location for BLE scan
- `LOCATION_PERMISSION_NOT_GRANTED`

---

## Item 3 — Connection Priority Request

**What it is**: `BluetoothGatt.requestConnectionPriority(CONNECTION_PRIORITY_HIGH)` tells the
BLE stack to use a shorter connection interval (7.5–15 ms vs 100–200 ms default). This reduces
command round-trip time and improves throughput for fragmented data transfers.

**Current behaviour**: The code has this commented out in the existing `Connection.kt`:
```kotlin
// gatt.requestConnectionPriority(BluetoothGatt.CONNECTION_PRIORITY_HIGH)
```
It was presumably removed due to instability on some devices.

**RxAndroidBle approach**:

```kotlin
// Inside DashConnection.connect() flatMap, after setupIndication()
connection.requestConnectionPriority(BluetoothGatt.CONNECTION_PRIORITY_HIGH)
    .delay(100, TimeUnit.MILLISECONDS)   // brief delay before first command
    .andThen(/* continue chain */)
```

`rxBleConnection.requestConnectionPriority(priority)` returns `Completable`. If it fails on a
particular device (some older BLE stacks reject the request), the failure should be logged but
not propagate as a fatal error — wrap it with `.onErrorComplete()`.

**Note**: Test carefully on Samsung Galaxy S series (known to behave differently with
`CONNECTION_PRIORITY_HIGH`). If instability is observed, this item should be made
user-configurable via preferences.

---

## Item 4 — MTU Negotiation

**What it is**: The default BLE MTU is 23 bytes (20 bytes payload). The DASH protocol packets
are 20 bytes maximum (matched to this limit). Negotiating a larger MTU would allow larger
packets but is not needed for correctness — include for completeness and future-proofing.

**Proposed**:

```kotlin
connection.requestMtu(PREFERRED_MTU)
    .doOnSuccess { negotiatedMtu ->
        aapsLogger.info(LTag.PUMPBTCOMM, "MTU negotiated: $negotiatedMtu bytes")
    }
    .onErrorReturn { FALLBACK_MTU }
    .flatMap { /* continue with indication setup */ }
```

**Constants**:
- `PREFERRED_MTU = 512` (request maximum; Android/pod will agree on the actual value)
- `FALLBACK_MTU = 23` (BLE 4.0 default if negotiation fails)

**Risk**: Low. If the pod or device BLE stack does not support MTU negotiation above 23, the
connection continues at default MTU unchanged.

---

## Item 5 — Unit Tests with `MockRxAndroidBle`

**What it is**: The `mockrxandroidble3` artifact provides a fully in-memory mock of
`RxBleClient`, `RxBleDevice`, and `RxBleConnection`. Tests can simulate scan results,
connection establishment, characteristic reads/writes, indication emissions, and disconnections
without any Android system services or BLE hardware.

**Add to `build.gradle.kts`**:

```kotlin
testImplementation(libs.com.polidea.mockrxandroidble3)   // add to version catalog first
```

**Test coverage targets**:

| Class | Test scenarios |
|---|---|
| `RxPodScanner` | Valid pod found; no pod (timeout); BLE scan fail; multiple devices (first-wins) |
| `DashConnection` | Successful connect; GATT 133 retry; disconnect mid-setup; indication setup ordering |
| `RxMessageIO` | Full send RTS/CTS/DATA/SUCCESS; NACK mid-send; receive flow; timeout on each step |
| Integration (`DashConnection` + `RxMessageIO`) | Full command round-trip; disconnect mid-command |

**Example `RxPodScanner` test**:

```kotlin
@Test
fun `scan returns MAC address when valid pod found`() {
    val mockClient = MockRxBleClient.create()
    val scanner = RxPodScanner(mockClient, testLogger)

    // Arrange: inject a fake scan result
    mockClient.mockScanResult(
        MockBleScanResult(
            device = MockBleScanDevice("AA:BB:CC:DD:EE:FF"),
            scanRecord = fakeDashScanRecord(podId = PodScanner.POD_ID_NOT_ACTIVATED)
        )
    )

    // Act
    val result = scanner.scanForPod(PodScanner.SCAN_FOR_SERVICE_UUID, PodScanner.POD_ID_NOT_ACTIVATED)
        .test()

    // Assert
    result.assertValue("AA:BB:CC:DD:EE:FF")
    result.assertComplete()
}
```

---

## Item 6 — Diagnostic Logging for Connection Metrics

**What it is**: Log enough data to diagnose reliability issues reported by users without
requiring a debug build.

**Proposed additions** (in `DashConnection`):

```kotlin
private var connectionStartTime: Long = 0

// In connect():
.doOnSubscribe { connectionStartTime = System.currentTimeMillis() }
.doOnComplete {
    val elapsed = System.currentTimeMillis() - connectionStartTime
    aapsLogger.info(LTag.PUMPBTCOMM, "Connection established in ${elapsed}ms")
    podState.lastConnectionDurationMs = elapsed
}
.doOnError { error ->
    val elapsed = System.currentTimeMillis() - connectionStartTime
    aapsLogger.warn(LTag.PUMPBTCOMM, "Connection failed after ${elapsed}ms: $error")
}
```

**Metrics to persist in `OmnipodDashPodStateManager`** (extend the existing interface):

| Metric | Type | Purpose |
|---|---|---|
| `lastConnectionDurationMs` | `Long` | Track if connections are getting slower |
| `lastGatt133Count` | `Int` | Track GATT 133 frequency per session |
| `connectionSuccessRatio` | already exists | Keep and update correctly |

---

## Item 7 — Connection Timeout Tuning

**What it is**: Review and rationalise the connection timeout values now that they can be
applied at the RxJava operator level rather than buried in `CountDownLatch` implementations.

**Current timeouts** (from `Connection.kt`):
```kotlin
const val BASE_CONNECT_TIMEOUT_MS = 10_000L
// pairNewPod uses 3 * BASE = 30_000 ms
```

**Proposed structure**:
```kotlin
object DashConnectionTimeouts {
    const val SCAN_DURATION_MS           = 5_000L
    const val GATT_CONNECT_TIMEOUT_MS    = 10_000L
    const val INDICATION_SETUP_TIMEOUT_MS = 5_000L
    const val HELLO_WRITE_TIMEOUT_MS     = 2_000L
    const val SESSION_ESTABLISH_TIMEOUT_MS = 15_000L
    const val CMD_INDICATION_TIMEOUT_MS  = 3_000L
    const val DATA_RECEIVE_TIMEOUT_MS    = 5_000L
    const val MESSAGE_SEND_TIMEOUT_MS    = 10_000L
    const val MESSAGE_READ_TIMEOUT_MS    = 10_000L
    const val PAIRING_CONNECT_TIMEOUT_MS = 30_000L   // longer for first activation
}
```

Individual timeout values can be tuned based on field reports without touching the logic.

---

## Phase 5 Completion Criteria

- [ ] GATT 133 retry is in place and logs retry attempts
- [ ] Bluetooth-off is detected proactively; UI updates without waiting for next command
- [ ] At least 80% unit test coverage of `RxPodScanner`, `DashConnection`, `RxMessageIO`
- [ ] No regressions on existing `Session`, `SessionEstablisher`, crypto unit tests
- [ ] Connection establishment time is logged and visible in support logs
- [ ] Tested on minimum: Pixel (AOSP stack), Samsung Galaxy (One UI), Xiaomi (MIUI)
