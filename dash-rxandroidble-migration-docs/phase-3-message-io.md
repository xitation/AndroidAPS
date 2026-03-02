# Phase 3 — Reactive MessageIO

**Status**: Planned (not started)
**Depends on**: Phase 2 complete (`DashConnection` established, `IMessageIO` interface in place,
indication streams as `ReplaySubject<ByteArray>` available)
**Risk level**: High — this phase replaces the innermost BLE read/write loop. A regression
causes every pod command to fail. Requires careful testing of every command type.

---

## Problem Being Solved

`MessageIO` is the class responsible for the RTS/CTS handshake protocol used by the DASH pod
to exchange data over BLE. Every pod command flows through it. Its current implementation is
entirely blocking: every operation parks the AAPS command-queue thread on a `BlockingQueue`.

### The RTS/CTS protocol

```
Send:
  1. Write BleCommandRTS to CMD characteristic    (Request-To-Send)
  2. Wait for CTS indication on CMD              (Clear-To-Send from pod)
  3. Write each data packet to DATA characteristic
  4. Wait for SUCCESS/NACK indication on CMD     (delivery confirmation)

Receive:
  1. Wait for RTS indication on CMD              (pod wants to send)
  2. Write BleCommandCTS to CMD characteristic   (we are ready)
  3. Read each data packet from DATA indications
  4. Write ACK to CMD characteristic             (we received it)
```

### Current blocking implementation pain points

**Every step parks the calling thread**:

```kotlin
// MessageIO.kt (current) — every method blocks
fun sendMessage(msg: MessagePacket): MessageSendResult {
    cmdBleIO.sendAndConfirmPacket(BleCommandRTS.data)       // blocks on BlockingQueue.poll(1000)
    val rsp = cmdBleIO.receivePacket(readTimeoutMs)         // blocks on BlockingQueue.poll(timeout)
    // ... send data packets, each blocking ...
    return cmdBleIO.peekForResponse(receiveTimeoutMs)       // blocks again
}
```

**No composition with timeout at the right granularity**: Each individual `poll(timeout)` has its
own timeout, but there is no overall message-level timeout. If the pod responds slowly to every
individual step but stays within each individual timeout, a single command can take arbitrarily
long.

**NACK retry loop is imperative**: The retry logic for a NACK mid-send is implemented as a
`while` loop that re-sends from the NAKed packet index. This is hard to reason about and hard
to test.

**Thread safety is implicit**: `IncomingPackets` uses a `LinkedBlockingQueue`. Thread safety
is achieved by the contract that only one thread ever calls `sendMessage`/`receiveMessage` at
a time (enforced by the `busy` `AtomicBoolean` in `OmnipodDashBleManagerImpl`). With a reactive
model this is explicit in the single-subscriber contract.

---

## Solution: `RxMessageIO`

Replace `MessageIO` with `RxMessageIO : IMessageIO` that expresses the RTS/CTS protocol as a
reactive chain. The blocking `IMessageIO` bridge methods delegate to the reactive ones via
`.blockingGet()` in Phase 3; those bridges are removed in Phase 4.

### Send flow

```kotlin
fun sendMessageRx(msg: MessagePacket): Single<MessageSendResult> {
    val packets = PayloadSplitter(msg.asByteArray()).splitInPackets()

    return rxBleConnection.writeCharacteristic(CMD_UUID, BleCommandRTS.data)
        // Wait for CTS on CMD indication stream
        .flatMap {
            cmdIndications
                .filter { isCTS(it) }
                .firstOrError()
                .timeout(CMD_INDICATION_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        }
        // Send all data packets sequentially (pod ACKs each before we send the next)
        .flatMap {
            Observable.fromIterable(packets)
                .concatMapSingle { packet ->
                    rxBleConnection.writeCharacteristic(DATA_UUID, packet.toByteArray())
                }
                .toList()
        }
        // Wait for SUCCESS or handle NACK
        .flatMap {
            cmdIndications
                .filter { isSuccessOrNack(it) }
                .firstOrError()
                .timeout(CMD_INDICATION_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        }
        .map { responseBytes ->
            when {
                isSuccess(responseBytes) -> MessageSendSuccess
                isNack(responseBytes)    -> MessageSendErrorConfirming("NACK received")
                else                     -> MessageSendErrorSending("Unexpected CMD response")
            }
        }
        .timeout(MESSAGE_SEND_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        .onErrorReturn { e -> MessageSendErrorSending("Send failed: ${e.message}", e) }
}
```

### Receive flow

```kotlin
fun receiveMessageRx(): Single<MessagePacket?> {
    return cmdIndications
        // Wait for RTS from pod
        .filter { isRTS(it) }
        .firstOrError()
        .timeout(MESSAGE_READ_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        // Send CTS
        .flatMap {
            rxBleConnection.writeCharacteristic(CMD_UUID, BleCommandCTS.data)
        }
        // Accumulate DATA indications until payload is complete
        .flatMap {
            dataIndications
                .scan(PayloadJoiner()) { joiner, bytes ->
                    joiner.accumulate(BlePacket(bytes))
                    joiner
                }
                .filter { it.isComplete() }
                .firstOrError()
                .timeout(DATA_RECEIVE_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        }
        // Send ACK
        .flatMap { joiner ->
            rxBleConnection.writeCharacteristic(CMD_UUID, BleCommandSuccess.data)
                .map { joiner }
        }
        .map { joiner -> MessagePacket.parse(joiner.finalize()) }
}
```

### Bridge methods (Phase 3, removed in Phase 4)

```kotlin
// IMessageIO implementation — blocking bridge, safe because callers are on IO thread
override fun sendMessage(msg: MessagePacket): MessageSendResult =
    sendMessageRx(msg).blockingGet()

override fun receiveMessage(): MessagePacket? =
    receiveMessageRx().blockingGet()
```

---

## Files Changed in This Phase

### Created

| File | Purpose |
|---|---|
| `message/RxMessageIO.kt` | Reactive `IMessageIO` implementation using `RxBleConnection` + indication subjects |

### Modified

| File | Change |
|---|---|
| `session/DashConnection.kt` | Constructs `RxMessageIO` instead of the old `BlockingMessageIO` during connection setup |
| `session/Session.kt` | No change — already uses `IMessageIO` interface from Phase 2 |
| `session/SessionEstablisher.kt` | No change — already uses `IMessageIO` interface from Phase 2 |

### Unchanged (dead code until Phase 4)

`message/BlockingMessageIO.kt` (renamed from `MessageIO.kt` in Phase 2), `io/BleIO.kt`,
`io/CmdBleIO.kt`, `io/DataBleIO.kt`, `io/IncomingPackets.kt`.

---

## `RxMessageIO` — Constructor and Dependencies

```kotlin
class RxMessageIO(
    private val aapsLogger: AAPSLogger,
    private val rxBleConnection: RxBleConnection,       // for writeCharacteristic()
    private val cmdIndications: Observable<ByteArray>,  // hot, from DashConnection.cmdSubject
    private val dataIndications: Observable<ByteArray>, // hot, from DashConnection.dataSubject
) : IMessageIO
```

`RxMessageIO` is constructed by `DashConnection` during connection setup (after indication
subjects are active) and is stored on `DashConnection.msgIO`. `Session` and `SessionEstablisher`
receive it via `DashConnection.establishSession()`.

---

## Timeout Architecture

The current `MessageIO` uses per-operation timeouts (`readTimeoutMs` passed to each `poll()`).
`RxMessageIO` applies timeouts at two levels:

| Level | Purpose | Operator |
|---|---|---|
| Per-indication wait | Detect a stalled pod response to a single step | `.timeout(CMD_INDICATION_TIMEOUT_MS)` on each `firstOrError()` |
| Per-message | Cap the total time for one full send or receive | `.timeout(MESSAGE_SEND_TIMEOUT_MS)` wrapping the full chain |

This gives both fine-grained detection of where the protocol stalled and an absolute ceiling.

---

## NACK Handling

The current `MessageIO` retries from the NAKed packet index in a `while` loop. In `RxMessageIO`,
a NACK from the pod during send causes `sendMessageRx()` to return `MessageSendErrorConfirming`.
The caller (`Session.sendCommand()`) already handles `CommandSendErrorConfirming` — it emits
`PodEvent.CommandSendNotConfirmed` and relies on a subsequent `getStatus()` to determine whether
the command actually executed. This matches the existing recovery logic and requires no change
to `Session`.

If precise packet-level retry is needed in the future, it can be implemented with `retryWhen`
on the `concatMapSingle` packet-send chain, but this is beyond Phase 3 scope.

---

## Indication Subject: `ReplaySubject` vs `PublishSubject`

Phase 2 uses `ReplaySubject.createWithSize(16)` for `cmdIndications` and `dataIndications`.
This is important in Phase 3: when `RxMessageIO.receiveMessageRx()` subscribes to
`cmdIndications.filter { isRTS(it) }`, the `ReplaySubject` replays any buffered items
immediately upon subscription. This prevents a race where an RTS indication arrives from the
pod in the brief window between `DashConnection` receiving it on the subject and `RxMessageIO`
subscribing to the subject's filter.

If `PublishSubject` were used instead, the RTS would be silently dropped and `receiveMessageRx()`
would time out.

---

## Testing This Phase

Manual verification (hardware required):

1. **Status request**: trigger `getStatus()` from the AAPS UI. Observe that the
   `GET_STATUS` command sends and receives a `DefaultStatusResponse` correctly.
2. **Bolus**: deliver a small test bolus (0.05 U). The `PROGRAM_BOLUS` command must complete
   the full RTS/CTS/DATA/SUCCESS sequence within the message timeout.
3. **Basal program**: set a temp basal. Verify `PROGRAM_TEMP_BASAL` response.
4. **Session with re-sync**: this is harder to trigger manually but is covered by the existing
   `SessionEstablisher` unit tests, which are transport-agnostic and will still pass.
5. **Timeout path**: use `adb` to simulate a slow BLE environment (not easily possible on all
   devices, but stepping through with a debugger and adding an artificial `Thread.sleep` before
   `onCharacteristicChanged` delivery can reproduce the condition).

Unit tests using `MockRxAndroidBle` (Phase 5) will cover the reactive chain more thoroughly.
The `PayloadSplitter` / `PayloadJoiner` / packet-framing logic is unchanged and already has
unit tests in the existing test suite.
