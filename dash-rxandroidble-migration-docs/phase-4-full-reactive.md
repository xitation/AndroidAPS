# Phase 4 — Full Reactive Pipeline & Dead Code Removal

**Status**: Planned (not started)
**Depends on**: Phase 3 complete (all BLE operations are reactive internally, blocking bridges
in place)
**Risk level**: Medium — the underlying reactive plumbing is already tested in Phases 2-3.
This phase removes the bridging `blockingGet()` / `blockingAwait()` calls and deletes all
now-unused files.

---

## Goal

After Phases 1-3, the BLE driver is reactive internally but still presents a synchronous
interface to `OmnipodDashBleManagerImpl` via blocking bridges:

```
OmnipodDashBleManagerImpl.connect()
  → Observable.create { emitter ->
       dashConnection.connect(timeoutMs).blockingAwait()   ← Phase 2 bridge
       session.sendCommand(cmd).blockingGet()              ← Phase 3 bridge
    }
```

Phase 4 eliminates every `blockingGet()` and `blockingAwait()` in the BLE path, making the
full stack reactive from `OmnipodDashBleManagerImpl`'s `Observable<PodEvent>` return types
down to the BLE characteristic writes.

---

## Changes to `OmnipodDashBleManagerImpl`

### `connect()` — before (Phase 3 bridge)

```kotlin
override fun connect(timeoutMs: Long): Observable<PodEvent> = Observable.create { emitter ->
    if (!busy.compareAndSet(false, true)) throw BusyException()
    try {
        emitter.onNext(PodEvent.BluetoothConnecting)
        dashConnection.connect(timeoutMs).blockingAwait()   // ← blocking bridge
        emitter.onNext(PodEvent.BluetoothConnected(address))
        emitter.onNext(PodEvent.EstablishingSession)
        establishSession(1.toByte())                        // ← blocking internal calls
        emitter.onNext(PodEvent.Connected)
        emitter.onComplete()
    } catch (ex: Exception) { ... }
    finally { busy.set(false) }
}
```

### `connect()` — after (Phase 4, fully reactive)

```kotlin
override fun connect(timeoutMs: Long): Observable<PodEvent> {
    if (!busy.compareAndSet(false, true)) return Observable.error(BusyException())
    val address = podState.bluetoothAddress
        ?: return Observable.error(FailedToConnectException("Missing bluetoothAddress"))

    return Observable.just(PodEvent.BluetoothConnecting as PodEvent)
        .concatWith(
            dashConnection.connect(timeoutMs)
                .andThen(Observable.just(PodEvent.BluetoothConnected(address)))
        )
        .concatWith(Observable.just(PodEvent.EstablishingSession))
        .concatWith(establishSessionRx(1.toByte()))
        .concatWith(Observable.just(PodEvent.Connected))
        .doOnError { dashConnection.disconnect() }
        .doOnTerminate { busy.set(false) }
}
```

### `sendCommand()` — after (Phase 4)

```kotlin
override fun sendCommand(cmd: Command, responseType: KClass<out Response>): Observable<PodEvent> {
    if (!busy.compareAndSet(false, true)) return Observable.error(BusyException())
    val session = dashConnection.session ?: return Observable.error(NotConnectedException("Missing session"))

    return Observable.just(PodEvent.CommandSending(cmd) as PodEvent)
        .concatWith(
            session.sendCommandRx(cmd).flatMapObservable { result ->
                when (result) {
                    is CommandSendSuccess         -> Observable.just(PodEvent.CommandSent(cmd))
                    is CommandSendErrorConfirming -> Observable.just(PodEvent.CommandSendNotConfirmed(cmd))
                    is CommandSendErrorSending    -> Observable.error(CouldNotSendCommandException())
                }
            }
        )
        .concatWith(
            session.readAndAckResponseRx().flatMapObservable { result ->
                when (result) {
                    is CommandReceiveSuccess -> Observable.just(PodEvent.ResponseReceived(cmd, result.result))
                    is CommandAckError       -> Observable.just(PodEvent.ResponseReceived(cmd, result.result))
                    is CommandReceiveError   -> Observable.error(MessageIOException("Could not read response"))
                }
            }
        )
        .doOnError { dashConnection.disconnect() }
        .doOnTerminate { busy.set(false) }
}
```

---

## Rx variants added to `Session`

Phase 4 introduces `Single<>` returning variants alongside the existing blocking methods.
The blocking methods either delegate to the reactive ones or are deleted:

```kotlin
// Session.kt additions
fun sendCommandRx(cmd: Command): Single<CommandSendResult>
fun readAndAckResponseRx(): Single<CommandReceiveResult>
```

These use `RxMessageIO.sendMessageRx()` and `receiveMessageRx()` directly, without any
`blockingGet()`.

---

## Blocking bridges removed in this phase

| Location | Bridge call | Replaced with |
|---|---|---|
| `OmnipodDashBleManagerImpl.connect()` | `dashConnection.connect().blockingAwait()` | `Completable` composed in `concatWith` |
| `OmnipodDashBleManagerImpl.pairNewPod()` | `rxPodScanner.scanForPod().blockingGet()` | `Single` composed in reactive chain |
| `OmnipodDashBleManagerImpl.sendCommand()` | `session.sendCommand()` (blocking) | `session.sendCommandRx()` |
| `DashConnection.connect()` doOnNext | `connection.writeCharacteristic().blockingGet()` (HELLO write) | `flatMap` on the connection chain |
| `RxMessageIO.sendMessage()` | `sendMessageRx().blockingGet()` | Deleted — callers use `sendMessageRx()` |
| `RxMessageIO.receiveMessage()` | `receiveMessageRx().blockingGet()` | Deleted — callers use `receiveMessageRx()` |

---

## Dead code deleted in this phase

The following files are **deleted** once no live code references them:

```
driver/comm/callbacks/BleCommCallbacks.kt
driver/comm/callbacks/WriteConfirmation.kt
driver/comm/io/BleIO.kt
driver/comm/io/CmdBleIO.kt
driver/comm/io/DataBleIO.kt
driver/comm/io/IncomingPackets.kt
driver/comm/scan/PodScanner.kt
driver/comm/scan/ScanCollector.kt
driver/comm/session/Connection.kt
driver/comm/session/ConnectionStateChangeHandler.kt
driver/comm/session/DisconnectHandler.kt
driver/comm/session/ServiceDiscoverer.kt
driver/comm/message/BlockingMessageIO.kt   (renamed from MessageIO.kt in Phase 2)
```

**Before deleting each file**: verify with the IDE that it has zero references (Find Usages).
If any reference remains, it is either a test file (update the test) or an oversight (fix it
before deleting).

Also remove the `RxPodScanner.companion` delegations to `PodScanner` constants. At this point
`SCAN_FOR_SERVICE_UUID` and `POD_ID_NOT_ACTIVATED` become standalone values in
`RxPodScanner.companion`.

---

## `Observable.create` → pure composition

The `Observable.create { emitter -> ... }` pattern used throughout `OmnipodDashBleManagerImpl`
is a blocking-bridge pattern — it wraps synchronous operations in an observable. After Phase 4,
none of the BLE operations are synchronous, so `Observable.create` is replaced with standard
RxJava operators (`concatWith`, `flatMap`, `andThen`).

This also eliminates the `busy.set(false)` in `finally` blocks (which exist to handle
exceptions thrown from inside `create`). With reactive chains, `doOnTerminate` (or
`doFinally`) handles cleanup cleanly and always runs, even on disposal.

---

## `IMessageIO` interface fate

After Phase 4, `IMessageIO` has only one implementation (`RxMessageIO`). The interface can
either:
- **Be kept** as an abstraction point for future testing (recommended — keeps `Session` testable)
- **Be collapsed** into `RxMessageIO` directly (simpler but harder to test)

Recommended: keep the interface. It costs nothing and makes `Session` unit-testable by passing
a stub `IMessageIO` without requiring `MockRxAndroidBle`.

---

## Verification Checklist

Before considering Phase 4 complete:

- [ ] Zero `blockingGet()` calls in any file under `driver/comm/`
- [ ] Zero `blockingAwait()` calls in any file under `driver/comm/`
- [ ] Zero `Thread.sleep()` calls in any file under `driver/comm/`
- [ ] Zero references to deleted files (confirmed with IDE "Find Usages")
- [ ] Full pod activation works end-to-end
- [ ] Full pod deactivation works end-to-end
- [ ] Bolus delivery and cancellation work
- [ ] Temp basal set and cancel work
- [ ] Status polling works
- [ ] App survives Bluetooth toggle (off → on) mid-session
- [ ] App survives pod moving out of range and back
- [ ] `busy` flag is always released (no stuck state) — verify with 10 sequential commands

---

## Thread model after Phase 4

All BLE I/O happens on RxAndroidBle's internal thread pool (dispatched from Binder callbacks).
All observable results are observed on `AapsSchedulers.io()` (matching the pre-existing
scheduler use in `OmnipodDashManagerImpl`). The AAPS command-queue thread is never parked
waiting for BLE I/O.
