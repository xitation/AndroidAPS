# RxAndroidBle Migration — OmniPod DASH BLE Driver

## Overview

This document series covers the planned refactor of the OmniPod DASH BLE driver to use the
[RxAndroidBle](https://github.com/dariuszseweryn/RxAndroidBle) library in place of the current
hand-rolled raw Android GATT implementation.

**Goal**: Improve BLE connection reliability across all Android phones — particularly on vendor
variants (Samsung, Huawei, Xiaomi) that deviate from the AOSP Bluetooth stack behaviour.

**Library version targeted**: `com.polidea.rxandroidble3:rxandroidble:1.19.0` (RxJava 3 variant).
Already present in `gradle/libs.versions.toml`; previously used only by the `eopatch` module.

---

## Why RxAndroidBle?

The current driver is built on `BluetoothGatt` callbacks with hand-rolled synchronisation
(`CountDownLatch`, `BlockingQueue`). This causes several categories of reliability problems:

| Problem | Current symptom | Root cause file |
|---|---|---|
| 5-second scan delay always | User waits 5 s even if pod is 10 cm away | `PodScanner.kt:29` — `Thread.sleep(5000)` |
| GATT 133 errors not retried | Connection fails with no recovery | `BleCommCallbacks.kt` — no GATT status handling |
| Thread deadlocks on OEM BLE stacks | App hangs during connection | `Connection.kt` — `CountDownLatch.await()` |
| Stale write confirmation race | Intermittent write failures | `BleIO.kt` — `flushConfirmationQueue()` defensive hack |
| No proactive disconnect detection | App thinks it's connected when it isn't | No `observeConnectionStateChanges()` |

RxAndroidBle addresses each of these at the library level, has been battle-tested across OEM
variants, and ships a `MockRxAndroidBle` module for unit testing without hardware.

---

## Phase Summary

| Phase | Scope | Status |
|---|---|---|
| [Phase 1 — Scanning](phase-1-scanning.md) | Replace `PodScanner` with `RxPodScanner` | **Complete** |
| [Phase 2 — Connection & Service Discovery](phase-2-connection.md) | Replace `Connection` + `BleCommCallbacks` + `ServiceDiscoverer` | Planned |
| [Phase 3 — Reactive MessageIO](phase-3-message-io.md) | Replace blocking `MessageIO` with `RxMessageIO` | Planned |
| [Phase 4 — Full Reactive Pipeline](phase-4-full-reactive.md) | Remove all blocking bridges; delete deprecated files | Planned |
| [Phase 5 — Hardening](phase-5-hardening.md) | Retry logic, state observation, MTU, unit tests | Planned |

Each phase is self-contained and independently testable on a real device before the next begins.

---

## Files Affected (full migration)

### Deleted in Phase 4

| File | Replaced by |
|---|---|
| `driver/comm/callbacks/BleCommCallbacks.kt` | RxAndroidBle internal GATT callback management |
| `driver/comm/callbacks/WriteConfirmation.kt` | `Single<ByteArray>` from `writeCharacteristic()` |
| `driver/comm/io/BleIO.kt` | `RxBleCharacteristicIO` |
| `driver/comm/io/CmdBleIO.kt` | Merged into `RxBleCharacteristicIO` |
| `driver/comm/io/DataBleIO.kt` | Merged into `RxBleCharacteristicIO` |
| `driver/comm/io/IncomingPackets.kt` | Indication `Observable<ByteArray>` from `setupIndication()` |
| `driver/comm/scan/PodScanner.kt` | `RxPodScanner` (deprecated in Phase 1) |
| `driver/comm/scan/ScanCollector.kt` | Removed with `PodScanner` |
| `driver/comm/session/Connection.kt` | `DashConnection` |
| `driver/comm/session/DisconnectHandler.kt` | `onError(BleDisconnectedException)` propagation |
| `driver/comm/session/ConnectionStateChangeHandler.kt` | `rxBleDevice.observeConnectionStateChanges()` |
| `driver/comm/session/ServiceDiscoverer.kt` | Internal to `establishConnection()` |
| `driver/comm/message/MessageIO.kt` | `RxMessageIO` |

### Unchanged (pure logic, no BLE coupling)

`Session.kt`, `SessionEstablisher.kt`, `EnDecrypt.kt`, `Nonce.kt`, `Milenage.kt`,
`LTKExchanger.kt`, `KeyExchange.kt`, `PayloadSplitter.kt`, `PayloadJoiner.kt`,
`MessagePacket.kt`, `BlePacket.kt`, all command/response classes, all pod state classes,
all history/database classes, all UI classes.

---

## Key API Mapping

| Current raw GATT | RxAndroidBle equivalent |
|---|---|
| `bluetoothLeScanner.startScan()` + `Thread.sleep(5000)` | `rxBleClient.scanBleDevices().firstOrError().timeout(5s)` |
| `device.connectGatt(ctx, false, callback, TRANSPORT_LE)` | `rxBleDevice.establishConnection(false)` |
| `gatt.discoverServices()` + `CountDownLatch.await()` | Internal — `establishConnection()` only emits after discovery |
| `gatt.setCharacteristicNotification()` + `gatt.writeDescriptor()` | `rxBleConnection.setupIndication(uuid)` |
| `characteristic.setValue()` + `gatt.writeCharacteristic()` (deprecated) | `rxBleConnection.writeCharacteristic(uuid, bytes)` → `Single<ByteArray>` |
| `BluetoothGattCallback.onCharacteristicChanged()` → `BlockingQueue` | Inner `Observable<ByteArray>` from `setupIndication()` |
| `bleCommCallbacks.confirmWrite()` — `BlockingQueue.poll(timeout)` | `writeCharacteristic()` `Single` completes on write confirmation |
| `bluetoothManager.getConnectionState()` | `rxBleDevice.observeConnectionStateChanges()` |
| `gatt.close()` | `connectionDisposable.dispose()` |

---

## Critical Protocol Notes (DASH-specific)

These must be preserved across all phases:

1. **Indications, not notifications**: The pod uses GATT indications (CCCD value `0x0002`).
   Always use `rxBleConnection.setupIndication()`, never `setupNotification()`.

2. **Indication setup before HELLO**: The HELLO write to the CMD characteristic must happen
   *after* both CMD and DATA indication subscriptions are active. The reactive chain enforces
   this naturally (setup in `flatMap`, HELLO in `doOnNext`).

3. **Session key per connection**: EAP-AKA session establishment runs on every connection, not
   just on first pairing. The LTK is persistent; the session keys are ephemeral.

4. **Message sequence numbers**: `msgSeq` and nonce counters are stateful across commands
   within a session. The `OmnipodDashPodStateManager` persists these to survive crashes.

---

## BLE UUIDs (reference)

| Purpose | UUID |
|---|---|
| Service | `00004024-0000-1000-8000-00805F9B34FB` |
| CMD characteristic | `00004024-0000-1000-8001-00805F9B34FB` |
| DATA characteristic | `00004024-0000-1000-1000-00805F9B34FB` |
