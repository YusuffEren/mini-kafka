# PLAN - mini-kafka

## Faz 0: Proje İskeleti
- [x] P0.1 Go modül başlatma, Makefile, .gitignore, golangci-lint config
- [x] P0.2 Dizin yapısı (cmd/, internal/, pkg/, test/, benchmark/, config/)
- [x] P0.3 config/broker.yaml oluşturma

## Faz 1: Storage Katmanı
- [x] T1.1 Record encode/decode (`internal/storage/record.go`)
- [x] T1.2 Index (`internal/storage/index.go`)
- [x] T1.3 Segment (`internal/storage/segment.go`)
- [x] T1.4 Log (`internal/storage/log.go`)
- [x] T1.5 Faz 1 DoD kontrolü (test, coverage, lint, review)

## Faz 2: Protokol ve TCP Server
- [x] T2.1 Codec (`internal/protocol/codec.go`)
- [x] T2.2 Frame (`internal/protocol/frame.go`)
- [x] T2.3 Request/Response tipleri
- [x] T2.4 TCP Server
- [x] T2.5 Broker iskeleti
- [x] T2.6 Client (producer, consumer)
- [x] T2.7 CLI'lar
- [x] T2.8 Faz 2 DoD kontrolü

## Faz 3: Topic ve Partition
- [x] T3.1 Partition (`internal/broker/partition.go`)
- [x] T3.2 Topic (`internal/broker/topic.go`)
- [x] T3.3 Metadata + CreateTopic API
- [x] T3.4 Client metadata cache
- [x] T3.5 Faz 3 DoD kontrolü

## Faz 4: Consumer Group ve Offset Yönetimi
- [x] T4.1 Group Coordinator
- [x] T4.2 Assignor (Range, RoundRobin)
- [x] T4.3 Offset Store
- [x] T4.4 Group Consumer Client
- [x] T4.5 Faz 4 DoD kontrolü

## Faz 5: Replikasyon ve ISR
- [x] T5.1 Replica State + ISR
- [x] T5.2 High Watermark
- [x] T5.3 Follower
- [x] T5.4 Leader
- [x] T5.5 Purgatory
- [x] T5.6 Leader Failover + Leader Epoch
- [x] T5.7 Faz 5 DoD kontrolü

## Faz 6: Benchmark ve Dokümantasyon
- [x] T6.1 Benchmark harness
- [x] T6.2 Latency histogram
- [x] T6.3 Kafka docker-compose
- [x] T6.4 Sonuç görselleştirme
- [x] T6.5 docs/BENCHMARK.md
- [x] T6.6 README
- [x] T6.7 Son güvenlik taraması
- [x] T6.8 Son commit
