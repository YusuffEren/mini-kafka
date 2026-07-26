# mini-kafka

Go ile yazılmış, Apache Kafka'nın mimarisinden (segment'li log, sparse index, consumer group, ISR replikasyonu) esinlenen bir mesaj kuyruğu. Kendi binary protokolünü kullanır — Kafka wire protokolüyle uyumlu değildir, resmi Kafka istemcileri bağlanamaz.

## Neler var

**Storage**
- Segment tabanlı log: her partition, boyut veya süre sınırına ulaşınca yeni segment dosyasına döner.
- Sparse index: her `index_interval_bytes` (varsayılan 4 KB) için bir offset girişi yazılır. Arama binary search ile yapılır.
- Retention: `retention_ms` ve `retention_bytes` ile eski segmentler silinir.
- Offset depolama: consumer group offset'leri `__consumer_offsets` dizini altında ayrı bir storage log partition'ına yazılır.

**Protokol**
- Kendi binary frame formatı. Codec, frame, request/response tanımları `docs/PROTOCOL.md` içinde.
- 12 API: Produce (0), Fetch (1), Metadata (2), CreateTopics (3), JoinGroup (4), SyncGroup (5), Heartbeat (6), LeaveGroup (7), OffsetCommit (8), OffsetFetch (9), ListOffsets (10), ReplicaFetch (11). API isimleri Kafka'daki karşılıklarından esinlenmiştir; key numaraları ve payload düzenleri Kafka ile aynı değildir.
- `bufio` tabanlı tamponlama, `slog` ile yapılandırılabilir loglama.

**Replikasyon**
- ISR (In-Sync Replicas) tabanlı replikasyon.
- Leader epoch desteği.
- ReplicaFetch ile follower'lar liderden çeker.
- `min_insync_replicas` ile yazma yeter sayısı kontrol edilir.
- Statik leader ataması: `partition % broker_sayisi`. Dinamik leader failover ve controller seçimi yoktur.

**Client**
- Go client (`pkg/client`): Producer, Consumer, GroupConsumer.
- Gerçek batching: `LingerMs` ve `BatchSize` ile kayıtlar tek Produce isteğinde toplanır.
- Key-based Murmur2 routing.
- Consumer group rebalance: Range ve RoundRobin stratejileri.
- CLI araçları: `cmd/producer`, `cmd/consumer`.

## Hızlı başlangıç

Gereksinimler: Go 1.25+, `make` (opsiyonel).

```bash
git clone https://github.com/YusuffEren/mini-kafka.git
cd mini-kafka

# make ile
make build

# veya make olmadan
go build -o bin/broker   ./cmd/broker
go build -o bin/producer ./cmd/producer
go build -o bin/consumer ./cmd/consumer
```

Broker başlat:

```bash
./bin/broker -config config/broker.yaml
```

Tek mesaj gönder ve consume et:

```bash
./bin/producer -brokers 127.0.0.1:9092 -topic test -key "k1" -value "merhaba"
./bin/consumer -brokers 127.0.0.1:9092 -topic test -group g1
```

Ayarlar `config/broker.yaml` üzerinden değiştirilir: broker ID, port, data dizini, segment boyutu, retention gibi değerler.

## Docker

Tek broker (`docker-compose.yml`, `config/broker-single.yaml` kullanır):

```bash
docker compose up -d
```

Broker `9092` portunda ayağa kalkar. Tüm partition'ların lideri bu broker olduğu için `NotLeaderForPartition` hatası dönmez.

3 broker cluster (`docker-compose.cluster.yml`):

```bash
docker compose -f docker-compose.cluster.yml up -d
```

Broker'lar `9092`, `9093`, `9094` portlarında çalışır. Her biri kendi config dosyasını (`config/broker1.yaml`, `broker2.yaml`, `broker3.yaml`) kullanır ve `config/broker.yaml`'daki cluster tanımına göre birbirini bulur.

## Go client örneği

```go
import "github.com/YusuffEren/mini-kafka/pkg/client"
```

Producer:

```go
p, _ := client.NewProducer([]string{"127.0.0.1:9092"}, client.DefaultProducerConfig())
defer p.Close()

offset, err := p.Send(ctx, "test", 0, []byte("key"), []byte("value"))
```

`LingerMs > 0` ise `Send` kayıtları bir batcher içinde toplar; `BatchSize`'a ulaşınca veya `LingerMs` dolunca tek Produce isteğiyle flush eder. `LingerMs == 0` senkron yol kullanır.

Consumer (tek partition, manuel offset):

```go
c, _ := client.NewConsumer([]string{"127.0.0.1:9092"}, client.DefaultConsumerConfig())
defer c.Close()

msgs, err := c.Fetch(ctx, "test", 0, 0)
```

Consumer group (otomatik rebalance, offset commit):

```go
gc, _ := client.NewGroupConsumer(
    []string{"127.0.0.1:9092"},
    "grup-1",
    []string{"test"},
    client.DefaultGroupConsumerConfig(),
)
defer gc.Close()

msgs, err := gc.Poll(ctx, 1*time.Second)
```

## Konfigürasyon referansı

Önemli ayarlar (`config/broker.yaml`):

| Anahtar | Varsayılan | Açıklama |
|---|---|---|
| `broker.id` | 1 | Broker'ın cluster içindeki benzersiz numarası. |
| `broker.port` | 9092 | Dinlenen TCP portu. |
| `broker.data_dir` | `/var/lib/mini-kafka` | Log ve index dosyalarının tutulduğu dizin. |
| `log.segment_bytes` | 134217728 (128 MB) | Segment dosyası bu boyuta ulaşınca yeni segment açılır. |
| `log.segment_ms` | 604800000 (7 gün) | Segment bu süre dolunca döner. |
| `log.index_interval_bytes` | 4096 | Sparse index için iki giriş arası byte aralığı. |
| `log.retention_ms` | 604800000 (7 gün) | Eski segmentler bu süreden sonra silinir. |
| `log.retention_bytes` | -1 | Boyut bazlı retention; -1 devre dışı. |
| `log.max_message_bytes` | 1048576 (1 MB) | Tek kaydın en fazla byte cinsinden boyutu. |
| `topic.auto_create` | false | Bilinmeyen topice yazınca otomatik oluşturulsun mu. |
| `topic.default_partitions` | 3 | Yeni topic için varsayılan partition sayısı. |
| `topic.default_replication_factor` | 1 | Yeni topic için varsayılan RF. |
| `replication.min_insync_replicas` | 1 | Yazmanın başarılı sayılması için gereken minimum ISR. |
| `replication.replica_lag_time_max_ms` | 30000 | Follower'ın geride kalma süresi eşiği. |
| `group.session_timeout_ms` | 45000 | Member yanıt vermezse gruptan düşme süresi. |
| `group.heartbeat_interval_ms` | 3000 | Heartbeat aralığı. |
| `group.offsets_topic_partitions` | 50 | `__consumer_offsets` topic'inin partition sayısı. |

## Test

```bash
# tüm testler
go test ./... -count=1

# race detector ile (Linux, CGO_ENABLED=1)
go test ./... -race -count=1

# entegrasyon testleri
go test ./test/integration/... -tags=integration -count=1

# benchmark
make bench
```

CI'da her push'ta `gofmt`, `go vet`, `go test -race` ve `golangci-lint` çalışır.

## Bilinçli yapılmayanlar

- **Record compression** (LZ4/Snappy/gzip): yok. Kayıtlar ham binary saklanır.
- **Zero-copy sendfile**: Go standart kütüphanesiyle uğraşılmadı.
- **Dinamik leader failover**: statik leader ataması vardır, controller seçimi ve otomatik failover planlanmadı.
- **ACL / authentication**: yok. Herkes her topice erişebilir.
- **Log compaction**: yok.

## Dokümantasyon

- [Storage katmanı](docs/STORAGE.md) — segment, index, log yapısı.
- [Binary protokol](docs/PROTOCOL.md) — codec, frame, request/response formatları.
- [Benchmark sonuçları](docs/BENCHMARK.md)
- Faz 1-6: `docs/PHASE_1.md` ... `docs/PHASE_6.md`

## Lisans

MIT — bkz. [LICENSE](LICENSE).