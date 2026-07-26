# mini-kafka

Go ile yazılmış, Apache Kafka'nın mimarisinden (segment'li log, sparse index, consumer group, ISR replikasyonu) esinlenen bir mesaj kuyruğu. **Kendi binary protokolünü** kullanır — Kafka wire protokolüyle uyumlu *değildir*, resmi Kafka istemcileri bağlanamaz. Protokol `docs/PROTOCOL.md` içinde tanımlıdır. Harici bağımlılığı yok denecek kadar az (`golang.org/x/sys` ve `yaml.v3` hariç), tek binary ile çalışıyor.

## Gereksinimler

- Go 1.25+
- `make` (opsiyonel, `go build` de çalışır)

## Kurulum

```bash
git clone https://github.com/YusuffEren/mini-kafka.git
cd mini-kafka

# make ile
make build

# veya make olmadan
go build -o bin/broker ./cmd/broker
go build -o bin/producer ./cmd/producer
go build -o bin/consumer ./cmd/consumer
```

## Broker çalıştırma

```bash
./bin/broker -config config/broker.yaml
```

Config dosyası (`config/broker.yaml`) üzerinden broker ID, port, data dizini, segment boyutu, retention gibi ayarları değiştirebilirsin.

## Producer / Consumer (CLI)

```bash
# tek mesaj
./bin/producer -brokers 127.0.0.1:9092 -topic test -key "k1" -value "merhaba"

# consumer
./bin/consumer -brokers 127.0.0.1:9092 -topic test -group g1
```

## Go client

```go
import "github.com/YusuffEren/mini-kafka/pkg/client"

// producer
p, _ := client.NewProducer([]string{"127.0.0.1:9092"}, client.DefaultProducerConfig())
offset, _ := p.Send(ctx, "test", 0, []byte("key"), []byte("value"))

// consumer group
gc, _ := client.NewGroupConsumer([]string{"127.0.0.1:9092"}, "grup-1", []string{"test"}, client.DefaultGroupConsumerConfig())
msgs, _ := gc.Poll(ctx, 1*time.Second)
```

## Docker

Tek broker (varsayılan, `docker-compose.yml`):

```bash
docker compose up -d
```

Tek broker, `9092` portunda ayağa kalkar. `config/broker-single.yaml` kullanır;
tüm partition'ların lideri bu broker olduğu için `NotLeaderForPartition` hatası
dönmez.

3 broker'lık cluster (`docker-compose.cluster.yml`):

```bash
docker compose -f docker-compose.cluster.yml up -d
```

Broker'lar `9092`, `9093` ve `9094` portlarında ayağa kalkar. Her biri kendi
config dosyasını (`config/broker1.yaml`, `broker2.yaml`, `broker3.yaml`)
kullanır ve `config/broker.yaml`'daki cluster tanımına göre birbirini bulur.

## Desteklenen API'ler

Produce (0), Fetch (1), Metadata (2), CreateTopics (3), JoinGroup (4), SyncGroup (5), Heartbeat (6), LeaveGroup (7), OffsetCommit (8), OffsetFetch (9), ListOffsets (10), ReplicaFetch (11).

*API isimleri Kafka'daki karşılıklarından esinlenmiştir; key numaraları ve payload düzenleri Kafka ile aynı değildir.

Topic başına birden fazla partition, key-based Murmur2 routing, consumer group rebalance (Range ve RoundRobin), ISR tabanlı replikasyon ve leader epoch desteği var. Offset'ler `__consumer_offsets` dizini altında storage log partition'larına yazılıyor.

## Bilinçli yapılmayanlar

- **Record compression** (LZ4/Snappy/gzip): yok. Tüm kayıtlar ham binary olarak saklanıyor.
- **Zero-copy sendfile**: Go'nun standart kütüphanesiyle uğraşmaya değmedi.
- **Dinamik leader failover**: Şu an statik leader ataması (`partition % broker_sayisi`). Controller seçimi ve otomatik failover planlanmadı.
- **ACL / authentication**: Yok. Herkes her topic'e erişebilir.

## Test ve CI

```bash
# tüm testler
go test ./... -count=1

# race detector ile (Linux, CGO_ENABLED=1)
go test ./... -race -count=1
```

CI'da her push'ta `gofmt`, `go vet`, `go test -race` ve `golangci-lint` çalışıyor.

## Dokümantasyon

Daha detaylı yazılar `docs/` altında:

- [Storage katmanı](docs/STORAGE.md) — segment, index, log yapısı
- [Binary protokol](docs/PROTOCOL.md) — codec, frame, request/response formatları
- [Benchmark sonuçları](docs/BENCHMARK.md)
- Faz 1-6: `docs/PHASE_1.md` ... `docs/PHASE_6.md`

## Lisans

Bu proje [MIT lisansı](LICENSE) altında dağıtılmaktadır.