# PHASE 4 - Consumer Group ve Offset Yönetimi

## Mimari
Phase 4, mini-kafka'ya Kafka uyumlu consumer group koordinasyon mekanizmasını, partition atama stratejilerini, offset saklama sistemini ve `GroupConsumer` istemcisini ekler.

```
                  +--------------------------+
                  |      GroupConsumer       |
                  +------------+-------------+
                               | JoinGroup / SyncGroup / Heartbeat / OffsetCommit
                               v
                  +--------------------------+
                  |     GroupCoordinator     |
                  +----+----------------+----+
                       |                |
         Assignors     v                v   OffsetStore
     +---------------+            +------------------+
     | Range Assignor|            | In-memory Cache  |
     | RoundRobin    |            | offsets.json     |
     +---------------+            +------------------+
```

## Yapılan Çalışmalar

1. **GroupCoordinator (`internal/coordinator/group.go`)**:
   - `StateEmpty`, `StatePreparingRebalance`, `StateCompletingRebalance`, `StateStable` durumlarını içeren durum makinesi (state machine) geliştirildi.
   - Periyodik oturum zaman aşımı takibi (`sessionTimeoutLoop`) eklendi.
   - `JoinGroup`, `SyncGroup`, `Heartbeat`, `LeaveGroup` API çağrıları bağlandı.

2. **Partition Assignors (`internal/coordinator/assignor.go`)**:
   - **Range Assignor**: Topic başına bitişik blok bölümlendirme.
   - **RoundRobin Assignor**: Tüm topic-partition çiftlerini sırayla üyelere dağıtma.
   - `EncodeAssignment` & `DecodeAssignment` ikili serileştirme metodları implemente edildi.

3. **OffsetStore (`internal/coordinator/offsets.go`)**:
   - Tüketici gruplarının işlediği offset bilgilerini bellek üzerinde saklar ve `meta/offsets.json` dosyasına kalıcı olarak yazar.

4. **GroupConsumer Client (`pkg/client/group_consumer.go`)**:
   - Otomatik `JoinGroup`/`SyncGroup` rebalance yönetimi.
   - Arka plan `heartbeatLoop` ve `autoCommitLoop` mekanizması.
   - `Poll`, `Commit`, `CommitOffset`, `Close` API'leri.

5. **Broker Entegrasyonu (`internal/broker/broker.go`)**:
   - `JoinGroup` (apiKey 4), `SyncGroup` (apiKey 5), `Heartbeat` (apiKey 6), `LeaveGroup` (apiKey 7), `OffsetCommit` (apiKey 8), `OffsetFetch` (apiKey 9), `ListOffsets` (apiKey 10) işleyicileri `server.Mux` üzerine kaydedildi.

## Doğrulama ve Testler
- `internal/coordinator`: `TestAssignors`, `TestOffsetStore`, `TestGroupCoordinator_Join_and_Sync` (PASS)
- `pkg/client`: `TestGroupConsumer_SingleConsumer`, `TestGroupConsumer_Rebalance`, `TestGroupConsumer_Concurrent_Race` (PASS)
