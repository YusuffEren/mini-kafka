# PHASE 3 — Topic ve Partition Dokümantasyonu

## 1. Genel Bakış
Faz 3'te mini-kafka'nın çoklu topic ve çoklu partition altyapısı, Murmur2 partitioner, disk tabanlı metadata kalıcılığı (`meta/topics.json`), `Metadata` (apiKey 2) ve `CreateTopics` (apiKey 3) protokol işleyicileri tamamlanmıştır.

## 2. Gerçekleştirilen Görevler

### T3.1 Partition Katmanı (`internal/broker/partition.go`)
- Partition başına **tek writer goroutine** + buffered channel (`appendCh chan appendRequest`) mimarisi kuruldu.
- Lock contention ortadan kaldırıldı ve append işlemlerinin sıralı (single-writer) yürütülmesi sağlandı.

### T3.2 Topic ve Partitioner (`internal/broker/topic.go`)
- `Topic` yapısı: `Name` ve `Partitions map[int32]*Partition`.
- Kafka ile %100 uyumlu `Murmur2` hash fonksiyonu ve test vektörleri eklendi.
- Key varsa `Murmur2(key) % numPartitions`, key yoksa round-robin dağıtımı yapıldı.

### T3.3 Metadata Yönetimi (`internal/broker/metadata.go`)
- Topic listesi, partition sayıları ve replikasyon faktörleri `meta/topics.json` üzerinde saklanır.
- Atomik dosya güncellemeleri (`.tmp` -> rename) ile crash-safe kalıcılık sağlandı.

### T3.4 API İşleyicileri ve Otomatik Topic Oluşturma (`internal/broker/broker.go`)
- `Metadata` (apiKey 2) ve `CreateTopics` (apiKey 3) handler'ları eklendi.
- Konfigürasyonla kontrol edilen otomatik topic oluşturma (`auto.create.topics.enable`) desteği entegre edildi.

## 3. DoD Kontrol Listesi
- [x] Çoklu topic/partition uçtan uca çalışıyor.
- [x] Metadata persist ve recovery çalışıyor (`metadata_test.go`).
- [x] Partition başına ordering garantisi ve Murmur2 testleri yeşil (`topic_test.go`).
- [x] `go test ./...` temiz geçiyor.
