# STATUS - mini-kafka

**Aktif Faz:** 5 (Replikasyon ve ISR)
**Aktif Görev:** T5.1 (Replica State + ISR)

## Son Durum
- Faz 0 (Proje İskeleti) ✓
- Faz 1 (Storage Katmanı) ✓
- Faz 2 (Protokol ve TCP Server) ✓
- Faz 3 (Topic ve Partition) ✓
- Faz 4 (Consumer Group ve Offset Yönetimi) ✓ — GroupCoordinator, Range/RoundRobin Assignor, OffsetStore, GroupConsumer client, PHASE_4.md
  - All unit & integration tests PASS
- Sıradaki: Faz 5 — Replikasyon ve ISR

## Açık Hatalar
(Yok)

## Sonraki Adım
Faz 5 — T5.1 Replica State + ISR (`internal/replication/isr.go`) implementasyonu
