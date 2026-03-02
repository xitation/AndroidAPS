# Phase 2 — Connection & Service Discovery

**Status**: Planned (not started)
**Depends on**: Phase 1 complete
**Risk level**: High — this phase replaces the core GATT connection lifecycle. A regression
here breaks all pod communication. Requires thorough testing on multiple physical devices
before merging.

---

## Problem Being Solved

Connection establishment in the current driver involves four separate classes coordinating
through shared state and blocking synchronisation primitives:

```
Connection.kt
  └─ creates BleCommCallbacks (BluetoothGattCallback subclass)
  └─ creates ServiceDiscoverer
  └─ creates CmdBleIO / DataBleIO
  └─ calls device.connectGatt(ctx, false, callbacks, TRANSPORT_LE)
  └─ blocks on BleCommCallbacks.waitForConnection() ← CountDownLatch.await()
      └─ BleCommCallbacks.onConnectionStateChange() ← signals latch
  └─ blocks on ServiceDiscoverer.waitForServiceDiscovery() ← another CountDownLatch.await()
      └─ BleCommCallbacks.onServicesDiscovered() ← signals second latch
  └─ creates CmdBleIO / DataBleIO from discovered characteristics
  └─ calls CmdBleIO.hello() ← write to CMD characteristic
  └─ calls CmdBleIO.readyToRead() ← enable indications on CMD
  └─ calls DataBleIO.readyToRead() ← enable indications on DATA
```

**Problems with this structure**:

1. **Race condition window between latches**: If `onConnectionStateChange(CONNECTED)` fires
   and the calling thread calls `gatt.discoverServices()`, but a disconnection event arrives
   between those two calls (e.g., the pod moves out of range in that 1-2 ms window), the
   `onServicesDiscovered` callback never fires and `ServiceDiscoverer.waitForServiceDiscovery()`
   blocks until its timeout (or indefinitely if the timeout path has a bug).

2. **GATT 133 not retried**: `BluetoothGatt.GATT_ERROR` (status code 133) is the most common
   BLE error on Android, caused by firmware bugs in vendor BLE stacks. The current code surfaces
   it as a `FailedToConnectException` with no retry. RxAndroidBle wraps it as `BleGattException`
   which can be caught in `retryWhen`.

3. **Deprecated write APIs**: `BleIO` uses `characteristic.setValue(bytes)` +
   `gatt.writeCharacteristic(characteristic)` — both deprecated since API 33. The new API is
   `gatt.writeCharacteristic(characteristic, bytes, writeType)`. RxAndroidBle handles this
   split automatically based on the device's API level.

4. **`BlockingQueue` for write confirmations**: `BleCommCallbacks.confirmWrite()` parks the
   calling thread on `writeQueue.poll(1000)`. If `onCharacteristicWrite` is swallowed by the
   stack (observed on Huawei EMUI and some Xiaomi MIUI builds), the thread parks for the full
   second and then reports a generic timeout with no GATT status information.

5. **Indication setup ordering**: `readyToRead()` on `CmdBleIO` writes to the CCCD descriptor
   to enable indications. The sequence is: connect → discover → write HELLO → enable CMD
   indication → enable DATA indication. If the pod sends an indication during CCCD setup but
   before the `onCharacteristicChanged` callback is registered, the indication is lost.

---

## Solution: `DashConnection`

Replace `Connection`, `BleCommCallbacks`, `ServiceDiscoverer`, `CmdBleIO`, `DataBleIO`, and
`IncomingPackets` with a single `DashConnection` class that uses:

- `rxBleDevice.establishConnection(false)` — emits `RxBleConnection` only after GATT is open
  and services are discovered (atomically, no second latch).
- `rxBleConnection.setupIndication(uuid)` — enables CCCD and returns an `Observable<ByteArray>`
  of incoming indications.
- `rxBleConnection.writeCharacteristic(uuid, bytes)` — returns `Single<ByteArray>` that
  completes on write confirmation; uses the modern non-deprecated API on API 33+.

### Connection sequence (reactive)

```kotlin
rxBleDevice.establishConnection(false)
    .timeout(timeoutMs, TimeUnit.MILLISECONDS)
    .flatMap { connection ->
        // Set up BOTH indication streams before sending HELLO.
        // If HELLO triggers an indication, we must already be subscribed.
        Observable.zip(
            connection.setupIndication(CMD_CHAR_UUID),   // Observable<Observable<ByteArray>>
            connection.setupIndication(DATA_CHAR_UUID),  // Observable<Observable<ByteArray>>
        ) { cmdStream, dataStream ->
            // Subscribe indication streams to PublishSubjects so Session/MessageIO
            // can observe them without holding a reference to RxBleConnection.
            cmdStream.subscribe(cmdSubject)
            dataStream.subscribe(dataSubject)
            connection
        }
    }
    .doOnNext { connection ->
        // HELLO is sent after indication subscriptions are active.
        // This is the exact same ordering as the current CmdBleIO.hello() call,
        // but now enforced structurally by the flatMap/doOnNext chain.
        connection.writeCharacteristic(CMD_CHAR_UUID, BleCommandHello(CONTROLLER_ID).data)
            .blockingGet()  // Phase 2 bridge — removed in Phase 4
    }
    .ignoreElements()   // Completable: caller awaits connection readiness
```

### Key design decisions

**Indication streams as `PublishSubject`**:

`setupIndication()` returns `Observable<Observable<ByteArray>>` — the outer observable emits
once when the CCCD write succeeds, and the inner observable emits each arriving indication.
We subscribe the inner observable to a `PublishSubject<ByteArray>` (one per characteristic)
so that `RxMessageIO` (Phase 3) can filter and consume indications reactively without needing
a reference to `RxBleConnection`.

Consider using `ReplaySubject.createWithSize(16)` instead of `PublishSubject` to buffer the
last 16 packets. This prevents losing an indication that arrives before `RxMessageIO` has set
up its filter — a realistic race during session establishment.

**`IMessageIO` interface introduced**:

`Session` and `SessionEstablisher` currently receive a `MessageIO` (the concrete blocking class).
In Phase 2 we introduce an `IMessageIO` interface so they can work with either the Phase 2
blocking bridge or the Phase 3 reactive implementation without changes to their own code:

```kotlin
interface IMessageIO {
    fun sendMessage(msg: MessagePacket): MessageSendResult
    fun receiveMessage(): MessagePacket?
}
```

The existing `MessageIO` is renamed `BlockingMessageIO : IMessageIO` (no logic changes).
`RxMessageIO : IMessageIO` is implemented in Phase 3.

---

## Files Changed in This Phase

### Deleted

| File | Reason |
|---|---|
| `callbacks/BleCommCallbacks.kt` | Entire `BluetoothGattCallback` subclass is replaced by RxAndroidBle's internal callback management |
| `callbacks/WriteConfirmation.kt` | Write confirmation is now the `Single<ByteArray>` completion of `writeCharacteristic()` |
| `session/ServiceDiscoverer.kt` | Service discovery is internal to `establishConnection()` |
| `session/DisconnectHandler.kt` | Disconnect propagates as `BleDisconnectedException` in the reactive `onError` path |
| `session/ConnectionStateChangeHandler.kt` | Replaced by `rxBleDevice.observeConnectionStateChanges()` |
| `io/IncomingPackets.kt` | `BlockingQueue` replaced by `PublishSubject<ByteArray>` fed from `setupIndication()` |

### Created

| File | Purpose |
|---|---|
| `session/DashConnection.kt` | Owns `RxBleDevice`, `establishConnection()` lifecycle, indication subjects, and session reference |
| `message/IMessageIO.kt` | Interface decoupling `Session`/`SessionEstablisher` from the blocking vs reactive implementation |
| `message/BlockingMessageIO.kt` | Rename of existing `MessageIO.kt` implementing `IMessageIO` (no logic change) |

### Heavily modified

| File | Change |
|---|---|
| `io/BleIO.kt`, `CmdBleIO.kt`, `DataBleIO.kt` | Kept as dead code; all callers replaced. Deleted in Phase 4. |
| `session/Connection.kt` | Kept as dead code; replaced by `DashConnection`. Deleted in Phase 4. |
| `comm/OmnipodDashBleManagerImpl.kt` | `connect()` and `pairNewPod()` rewired to use `DashConnection` |
| `session/Session.kt` | Constructor changed to accept `IMessageIO` instead of `MessageIO` |
| `session/SessionEstablisher.kt` | Constructor changed to accept `IMessageIO` instead of `MessageIO` |

---

## `DashConnection` — Proposed API

```kotlin
@Singleton
class DashConnection @Inject constructor(
    private val rxBleClient: RxBleClient,
    private val aapsLogger: AAPSLogger,
    private val config: Config,
    private val podState: OmnipodDashPodStateManager,
) {
    // Hot subjects: RxMessageIO subscribes to these for incoming data.
    val cmdIndications: ReplaySubject<ByteArray> = ReplaySubject.createWithSize(16)
    val dataIndications: ReplaySubject<ByteArray> = ReplaySubject.createWithSize(16)

    // The active RxBleConnection; used by RxMessageIO for writes.
    @Volatile var activeConnection: RxBleConnection? = null

    // Session established over this connection.
    @Volatile var session: Session? = null

    // Disposable tracking the full connection lifecycle.
    private val connectionDisposables = CompositeDisposable()

    fun connect(timeoutMs: Long): Completable
    fun disconnect()
    fun establishSession(ltk: ByteArray, msgSeq: Byte, ids: Ids, eapSqn: ByteArray): EapSqn?
    fun connectionState(): Observable<RxBleConnectionState>
}
```

---

## Critical Risk: Indication vs Notification

The DASH pod uses **GATT indications** (CCCD value `0x0002`), not notifications (`0x0001`).
Indications require the central (phone) to send an acknowledgement before the peripheral
(pod) sends the next one. If the phone uses notification semantics the pod stalls after the
first indication.

RxAndroidBle provides two distinct methods:
- `rxBleConnection.setupNotification(uuid)` — writes `ENABLE_NOTIFICATION_VALUE (0x0001)` to CCCD
- `rxBleConnection.setupIndication(uuid)` — writes `ENABLE_INDICATION_VALUE (0x0002)` to CCCD

**Always use `setupIndication()`**. The current code confirms this requirement:
```kotlin
// BleIO.kt (existing)
descriptor.setValue(BluetoothGattDescriptor.ENABLE_INDICATION_VALUE)
```

---

## Critical Risk: HELLO Before Indication Setup

The current connection sequence in `Connection.kt`:
```
1. connectGatt()
2. discoverServices()
3. new CmdBleIO (characteristic reference only, no indication setup yet)
4. new DataBleIO
5. cmdBleIO.hello()         ← write HELLO to CMD
6. cmdBleIO.readyToRead()   ← NOW enable CMD indications
7. dataBleIO.readyToRead()  ← NOW enable DATA indications
```

HELLO is sent *before* indications are enabled. This works only because the pod does not send
an indication in response to HELLO (it is a one-way announcement).

In `DashConnection`, the reactive chain must preserve this ordering:
```kotlin
.flatMap { conn ->
    // Enable BOTH indications FIRST (flatMap executes before doOnNext)
    Observable.zip(conn.setupIndication(CMD_UUID), conn.setupIndication(DATA_UUID)) { ... }
}
.doOnNext { conn ->
    // Send HELLO AFTER indications are active
    conn.writeCharacteristic(CMD_UUID, BleCommandHello(...).data).blockingGet()
}
```

---

## Testing This Phase

Manual verification steps (hardware required):

1. Build and install. Perform a full pod activation: scan → connect → pair → session → bolus.
2. Test reconnect after voluntary disconnect: let the pod disconnect idle, then trigger a status
   check. Should reconnect and re-establish session cleanly.
3. Test GATT 133: impossible to force directly, but move the phone close to interference
   (microwave, 2.4 GHz WiFi router). Observe that the connection retries rather than failing
   immediately.
4. Test Bluetooth toggle: disable and re-enable Bluetooth mid-operation. The `BleDisconnectedException`
   should propagate cleanly without a thread stuck in `CountDownLatch.await()`.
5. Test on at least one Samsung and one Xiaomi device (most common OEM BLE stack deviations).
