# PHASE 2 — Protokol ve TCP Server Dokümantasyonu

## 1. Genel Bakış
Faz 2'de mini-kafka'nın custom binary wire protokolü, TCP sunucu mimarisi, Produce ve Fetch istek işleyicileri (long-polling dahil), istemci SDK'sı (`pkg/client`) ve CLI araçları (`cmd/`) tamamlanmıştır.

## 2. Gerçekleştirilen Görevler

### T2.1 & T2.2 Codec ve Frame Katmanı (`internal/protocol/`)
- Primitive encode/decode fonksiyonları (`Int8`, `Int16`, `Int32`, `Int64`, `String`, `Bytes`, `ArrayHeader`).
- `ReadRequestFrame` ve `WriteResponseFrame` ile TCP uzerinde sabit 12-byte header + payload framelenmesi.
- Fuzz testi (`codec_fuzz_test.go`) ile dekoderin güvenliği doğrulandı.

### T2.3 & T2.4 TCP Server ve Request Dispatch (`internal/server/`)
- Accept loop ile bağlantı başına goroutine mimarisi.
- `Mux` dispatçeri: API key'e göre handler eşleme, `UnsupportedVersion` (13) ve `UnknownError` (1) hata dönüşü.
- Context duyarlı graceful shutdown.

### T2.5 Broker & Storage Log Entegrasyonu (`internal/broker/`)
- Single-partition `storage.Log` entegrasyonu.
- `Produce` (apiKey 0) işleyicisi: Gelen record batch'lerini log'a yazar ve atanan `baseOffset` bilgisini döner.
- `Fetch` (apiKey 1) işleyicisi ve **Long-Polling**: `fetchOffset >= LEO` durumunda client busy-wait yapmaz; yeni record append edilene kadar channel dinler veya `maxWaitMs` dolana kadar bekletilir.

### T2.6 Client SDK (`pkg/client/`)
- `Producer`: `Send` ve `SendBatch` metodları, otomatik bağlantı kurma, batch biriktirme (`linger.ms`).
- `Consumer`: `Fetch` metodu ile offset bazlı record okuma.

### T2.7 CLI Binary'leri (`cmd/`)
- `cmd/broker`: YAML konfigürasyonu ile çalışan ve SIGINT/SIGTERM ile durdurulabilen broker sunucusu.
- `cmd/producer`: Stdin veya flag parametresi ile mesaj gönderen CLI üreticisi.
- `cmd/consumer`: Broker'dan sıralı mesaj okuyan CLI tüketicisi.

## 3. DoD Kontrol Listesi
- [x] Uçtan uca produce / fetch / long-polling çalışıyor (`broker_integration_test.go`).
- [x] `pkg/client` unit testleri yeşil (`client_test.go`).
- [x] `docs/PROTOCOL.md`, `docs/PHASE_1.md`, `docs/PHASE_2.md` yazıldı.
- [x] Codec Fuzz testi 8 saniye çalıştırıldı, 0 crash.
- [x] `go test ./...` ve `go vet ./...` temiz geçiyor.
