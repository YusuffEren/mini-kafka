# PHASE 5 - Replikasyon ve ISR

## Mimari
Phase 5, mini-kafka'ya çoklu broker ortamında leader-follower replikasyonunu, In-Sync Replica (ISR) takibini, High Watermark (HW) veri tutarlılık sınırını ve Leader Epoch mekanizmasını getirir.

```
                     Leader Partition
                  +--------------------+
                  |  LogEndOffset=100  |
                  +---------+----------+
                            |
           +----------------+----------------+
           |                                 |
           v (ReplicaFetch)                  v (ReplicaFetch)
  +------------------+              +------------------+
  |  Follower 1      |              |  Follower 2      |
  |  LogEndOffset=90 |              |  LogEndOffset=80 |
  +------------------+              +------------------+

  High Watermark (HW) = min(ISR LEOs) = 80
  Consumer'lar sadece HW=80 noktasına kadar okuyabilir.
```

## Yapılan Çalışmalar

1. **Replica State & ISR Tracker (`internal/replication/isr.go`)**:
   - Her replica için `BrokerID`, `LogEndOffset`, `LastCaughtUpTime`, `InSync` durumları izlenir.
   - ISR kriteri: `time.Since(LastCaughtUpTime) < replica.lag.time.max.ms` (varsayılan 30 sn).
   - High Watermark (HW): ISR'daki tüm replica'ların LEO'larının minimum değeri olarak dinamik hesaplanır.
   - HW offset checkpoint bilgisi `replication-offset-checkpoint` dosyasına otomatik kaydedilir.

2. **Purgatory & Follower Fetcher (`internal/replication/replication.go`)**:
   - `acks=all` istekleri için HW ilerlemesini bekleten ve tamamlandığında serbest bırakan `Purgatory` yapısı.
   - Leader ve Follower arasında `ReplicaFetch` (apiKey 11) istek/yanıt ikili serileştirme metodları.

3. **EpochManager & Leader Failover (`internal/replication/epoch.go`)**:
   - Split-brain ve veri tutarsızlıklarını önlemek için leader değişiminde `Leader Epoch` artırımı.
   - Geçersiz epoch isteklerini reddetme (`ValidateEpoch`).

## Doğrulama ve Testler
- `internal/replication`: `TestISRTracker_HW_Calculation`, `TestISRTracker_LagTimeout`, `TestPurgatory`, `TestEpochManager` (PASS)
