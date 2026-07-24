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
- [ ] T2.1 Codec (`internal/protocol/codec.go`)
- [ ] T2.2 Frame (`internal/protocol/frame.go`)
- [ ] T2.3 Request/Response tipleri
- [ ] T2.4 TCP Server
- [ ] T2.5 Broker iskeleti
- [ ] T2.6 Client (producer, consumer)
- [ ] T2.7 CLI'lar
- [ ] T2.8 Faz 2 DoD kontrolü

## Faz 3: Topic ve Partition
- [ ] T3.1 Partition (`internal/broker/partition.go`)
- [ ] T3.2 Topic (`internal/broker/topic.go`)
- [ ] T3.3 Metadata + CreateTopic API
- [ ] T3.4 Client metadata cache
- [ ] T3.5 Faz 3 DoD kontrolü

## Faz 4: Consumer Group ve Offset Yönetimi
- [ ] T4.1 Group Coordinator
- [ ] T4.2 Assignor (Range, RoundRobin)
- [ ] T4.3 Offset Store
- [ ] T4.4 Group Consumer Client
- [ ] T4.5 Faz 4 DoD kontrolü

## Faz 5: Replikasyon ve ISR
- [ ] T5.1 Replica State + ISR
- [ ] T5.2 High Watermark
- [ ] T5.3 Follower
- [ ] T5.4 Leader
- [ ] T5.5 Purgatory
- [ ] T5.6 Leader Failover + Leader Epoch
- [ ] T5.7 Faz 5 DoD kontrolü

## Faz 6: Benchmark ve Dokümantasyon
- [ ] T6.1 Benchmark harness
- [ ] T6.2 Latency histogram
- [ ] T6.3 Kafka docker-compose
- [ ] T6.4 Sonuç görselleştirme
- [ ] T6.5 docs/BENCHMARK.md
- [ ] T6.6 README
- [ ] T6.7 Son güvenlik taraması
- [ ] T6.8 Son commit
