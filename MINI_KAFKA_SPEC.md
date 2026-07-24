# mini-kafka — Teknik Spesifikasyon ve Uygulama Planı

> Bu dosya, AI agent'ların bu projeyi baştan sona inşa etmesi için yazılmış **normatif** bir spesifikasyondur.
> Belirsizlik olan yerlerde agent kendi kararını vermemeli, `OPEN_QUESTIONS.md` dosyasına yazıp devam etmelidir.
> "SHOULD/MUST/MUST NOT" ifadeleri bağlayıcıdır.

---

## 0. Projenin Amacı

Apache Kafka'nın çekirdek mekanizmalarını **sıfırdan Go ile** yeniden yazmak. Amaç production'da Kafka'yı değiştirmek değil; **nasıl çalıştığını** kanıtlanabilir şekilde öğrenmek ve göstermek.

### Kapsam İçi (MUST)
- Append-only, segment'lere bölünmüş, disk tabanlı commit log
- Sparse offset index ve offset ile O(log n) arama
- Custom binary protokol (TCP üzerinde)
- Topic / Partition modeli
- Producer ve Consumer client kütüphanesi
- Consumer group, partition assignment, rebalancing
- Offset commit/fetch (broker tarafında persist)
- Leader-follower replikasyon ve ISR (In-Sync Replica) takibi
- Gerçek Kafka'ya karşı benchmark

### Kapsam Dışı (MUST NOT — vakit kaybı)
- Kafka wire protokolü ile uyumluluk (kendi protokolümüz olacak)
- Kafka Streams / Connect / KSQL benzeri katmanlar
- Exactly-once semantics, transactional producer
- SASL/TLS/ACL güvenlik katmanı
- ZooKeeper veya KRaft benzeri tam consensus protokolü (basitleştirilmiş controller yeterli)
- Log compaction (Faz 7'de opsiyonel)
- Web UI

---

## 1. Agent Çalışma Kuralları

Bu bölüm tüm agent'lar için bağlayıcıdır.

### 1.1 Genel Disiplin
1. **Fazları sırayla yap.** Faz N tamamlanmadan Faz N+1'e geçme. "Tamamlandı" tanımı her fazın sonundaki Definition of Done (DoD) listesidir.
2. **Test yazmadan kod yazma.** Her `.go` dosyasının yanında `_test.go` dosyası olacak. Faz DoD'unda belirtilen coverage hedefinin altına düşme.
3. **Dış bağımlılık ekleme.** İzin verilen bağımlılıklar bölüm 2.2'de listelidir. Yeni bağımlılık gerekiyorsa `OPEN_QUESTIONS.md`'ye yaz, standart kütüphane ile çöz.
4. **Var olan dosyayı silme veya baştan yazma.** Değişiklik gerekiyorsa incremental düzenle.
5. **TODO bırakma.** Bir şey eksikse ya bitir ya `OPEN_QUESTIONS.md`'ye yaz. Kod içinde `// TODO` kalmayacak.
6. **Panik etme, hata döndür.** `panic()` sadece programcı hatası (impossible state) için. Runtime hataları `error` olarak dönecek.

### 1.2 Kod Standartları
- Go 1.22+
- `gofmt` ve `go vet` temiz geçecek
- `golangci-lint run` hatasız (config repo'da olacak)
- Exported her fonksiyon/tip için doc comment
- Hata mesajları küçük harfle başlar, noktalama ile bitmez: `fmt.Errorf("segment not found for offset %d", off)`
- Hata sarmalama: `fmt.Errorf("read segment: %w", err)`
- Context alan her fonksiyon ilk parametre olarak `ctx context.Context` alır

### 1.3 Commit Disiplini
Conventional Commits kullan:
```
feat(log): add segment rotation on size limit
fix(index): correct relative offset calculation
test(group): add rebalance race condition test
docs(readme): add architecture diagram
perf(log): use mmap for index reads
```
Her commit **tek bir mantıksal değişiklik** içerecek ve `go test ./...` yeşil bırakacak.

### 1.4 Her Faz Sonunda Yapılacaklar
- `go test ./... -race` yeşil
- `go vet ./...` temiz
- Faz için yazılmış integration test'i geçiyor
- `docs/PHASE_N.md` dosyasına: ne yapıldı, hangi kararlar alındı, hangi zorluklarla karşılaşıldı
- `CHANGELOG.md` güncellendi

### 1.5 Belirsizlik Politikası
Spec'te olmayan bir karar gerekirse:
1. En basit çözümü seç
2. `OPEN_QUESTIONS.md`'ye ekle: soru, seçilen çözüm, alternatifler, neden bu seçildi
3. Kod içinde `// DECISION: ...` yorumu bırak

---

## 2. Teknik Kararlar

### 2.1 Ortam
| Konu | Karar |
|---|---|
| Dil | Go 1.22+ |
| Hedef OS | Linux (macOS dev için çalışmalı) |
| Build | `make` + Makefile |
| Test | standart `testing` paketi |
| CI | GitHub Actions |

### 2.2 İzin Verilen Bağımlılıklar
```
Standart kütüphane           — sınırsız
golang.org/x/sys             — mmap için
gopkg.in/yaml.v3             — config
github.com/stretchr/testify  — sadece test dosyalarında
```
**Başka hiçbir şey yok.** Özellikle: hiçbir hazır network framework'ü, hiçbir hazır consensus kütüphanesi, hiçbir hazır serialization kütüphanesi.

### 2.3 Temel Tasarım Kararları
| Konu | Karar | Gerekçe |
|---|---|---|
| Transport | Raw TCP, custom binary frame | Öğretici, HTTP overhead yok |
| Serialization | Elle yazılmış encoder/decoder, big-endian | Kafka da böyle yapıyor |
| Storage | `os.File` yazma, `mmap` index okuma | Sequential write hızlı, index random access |
| Concurrency | Connection başına goroutine | Go idiomatic |
| Partition yazma | Partition başına tek writer goroutine + channel | Lock contention'ı azaltır |
| Replikasyon | Leader-based, pull tabanlı follower | Kafka modeli |
| Controller | Statik config'ten leader ataması (Faz 5), dinamik seçim opsiyonel | Consensus yazmıyoruz |

---

## 3. Repo Yapısı

```
mini-kafka/
├── cmd/
│   ├── broker/main.go          # Broker binary
│   ├── producer/main.go        # CLI producer (test/demo)
│   └── consumer/main.go        # CLI consumer (test/demo)
├── internal/
│   ├── storage/
│   │   ├── record.go           # Record encode/decode
│   │   ├── segment.go          # Tek segment (log + index)
│   │   ├── index.go            # Sparse offset index
│   │   ├── log.go              # Segment koleksiyonu = partition log
│   │   └── *_test.go
│   ├── broker/
│   │   ├── broker.go           # Ana broker orchestration
│   │   ├── topic.go            # Topic yönetimi
│   │   ├── partition.go        # Partition (log + replication state)
│   │   ├── metadata.go         # Cluster metadata
│   │   └── *_test.go
│   ├── protocol/
│   │   ├── frame.go            # Frame okuma/yazma
│   │   ├── requests.go         # Request tipleri
│   │   ├── responses.go        # Response tipleri
│   │   ├── codec.go            # Primitive encode/decode
│   │   └── *_test.go
│   ├── server/
│   │   ├── server.go           # TCP accept loop
│   │   ├── handler.go          # API dispatch
│   │   └── *_test.go
│   ├── coordinator/
│   │   ├── group.go            # Consumer group state machine
│   │   ├── assignor.go         # Partition assignment stratejileri
│   │   ├── offsets.go          # Offset commit store
│   │   └── *_test.go
│   ├── replication/
│   │   ├── leader.go           # Leader tarafı
│   │   ├── follower.go         # Follower fetch loop
│   │   ├── isr.go              # ISR takibi
│   │   └── *_test.go
│   └── config/
│       └── config.go
├── pkg/client/
│   ├── producer.go             # Producer client
│   ├── consumer.go             # Consumer client
│   ├── group_consumer.go       # Consumer group client
│   └── *_test.go
├── test/
│   ├── integration/            # Uçtan uca testler
│   └── chaos/                  # Hata enjeksiyon testleri
├── benchmark/
│   ├── minikafka_bench.go
│   ├── kafka_bench.go
│   └── docker-compose.yml      # Gerçek Kafka için
├── docs/
│   ├── PROTOCOL.md
│   ├── STORAGE.md
│   ├── PHASE_1.md ... PHASE_6.md
│   └── BENCHMARK.md
├── config/
│   └── broker.yaml
├── Makefile
├── CHANGELOG.md
├── OPEN_QUESTIONS.md
└── README.md
```

---

## 4. Storage Formatı (Normatif)

### 4.1 Record Formatı
Tüm sayısal alanlar **big-endian**.

```
┌──────────────┬────────┬─────────────────────────────────────────┐
│ Alan         │ Boyut  │ Açıklama                                │
├──────────────┼────────┼─────────────────────────────────────────┤
│ length       │ int32  │ Bu alandan SONRAKİ byte sayısı          │
│ offset       │ int64  │ Partition içindeki mutlak offset        │
│ timestamp    │ int64  │ Unix milliseconds                       │
│ crc32        │ uint32 │ attributes'tan sona kadar CRC32C        │
│ attributes   │ int8   │ bit 0: tombstone, 1-7 reserved          │
│ keyLength    │ int32  │ -1 = null key                           │
│ key          │ bytes  │ keyLength byte                          │
│ valueLength  │ int32  │ -1 = null value                         │
│ value        │ bytes  │ valueLength byte                        │
└──────────────┴────────┴─────────────────────────────────────────┘
```

**Kurallar:**
- `crc32` CRC32-Castagnoli (`hash/crc32` + `crc32.Castagnoli`) kullanır
- Okuma sırasında CRC doğrulanır, uyuşmazsa `ErrCorruptRecord`
- Maksimum record boyutu config'ten (`max.message.bytes`, varsayılan 1 MB)

### 4.2 Segment Dosya İsimlendirme
```
{baseOffset zero-padded 20 hane}.log
{baseOffset zero-padded 20 hane}.index
```
Örnek:
```
00000000000000000000.log
00000000000000000000.index
00000000000000016384.log
00000000000000016384.index
```

### 4.3 Index Formatı
Sabit genişlikte kayıtlar, her biri 8 byte:
```
┌────────────────┬────────┬───────────────────────────────────┐
│ relativeOffset │ uint32 │ offset - segment.baseOffset       │
│ position       │ uint32 │ .log dosyasındaki byte pozisyonu  │
└────────────────┴────────┴───────────────────────────────────┘
```

**Kurallar:**
- **Sparse index:** Her record değil, her `index.interval.bytes` (varsayılan 4096) byte'ta bir entry
- Arama: binary search ile en yakın küçük entry bulunur, oradan .log'da sequential tarama
- Index dosyası `mmap` ile okunur (`golang.org/x/sys/unix.Mmap`)
- Aktif segment'in index'i preallocate edilir (`index.max.bytes`, varsayılan 10 MB), segment kapanınca truncate edilir

### 4.4 Segment Rotation
Yeni segment açma koşulları (herhangi biri):
- Aktif segment boyutu >= `segment.bytes` (varsayılan 128 MB, test için 1 MB)
- Aktif segment yaşı >= `segment.ms` (varsayılan 7 gün)
- Index dosyası doldu

### 4.5 Recovery
Broker açılışında her partition için:
1. Dizindeki tüm segment dosyaları listelenir, baseOffset'e göre sıralanır
2. Son segment (aktif) baştan sona taranır, her record'un CRC'si doğrulanır
3. İlk bozuk record'da dosya o noktadan **truncate edilir**
4. Log End Offset (LEO) belirlenir
5. Index yeniden inşa edilir (aktif segment için)

### 4.6 Dayanıklılık
- `flush.messages` (varsayılan 0 = OS'e bırak) ve `flush.ms` (varsayılan 1000)
- `Producer.acks` değerleri: `0` (fire and forget), `1` (leader'a yazıldı), `all` (tüm ISR'a yazıldı)
- `acks=all` durumunda leader ISR'daki tüm replica'ların o offset'i fetch etmesini bekler

---

## 5. Wire Protokolü (Normatif)

Detaylı hali `docs/PROTOCOL.md` dosyasına da yazılacak.

### 5.1 Frame Yapısı
```
┌────────────┬────────┬──────────────────────────────────────┐
│ size       │ int32  │ Bu alandan sonraki toplam byte       │
│ apiKey     │ int16  │ İstek tipi                           │
│ apiVersion │ int16  │ Şu an her zaman 1                    │
│ correlationID │ int32 │ İstek-cevap eşleştirme            │
│ clientID   │ string │ len-prefixed (int16 + bytes)         │
│ payload    │ bytes  │ apiKey'e göre değişir                │
└────────────┴────────┴──────────────────────────────────────┘
```

Response frame:
```
┌───────────────┬────────┬─────────────────────────────┐
│ size          │ int32  │                             │
│ correlationID │ int32  │ İstekteki ile aynı          │
│ errorCode     │ int16  │ 0 = başarılı                │
│ payload       │ bytes  │                             │
└───────────────┴────────┴─────────────────────────────┘
```

### 5.2 Primitive Tipler
| Tip | Encoding |
|---|---|
| int8/16/32/64 | big-endian, sabit boyut |
| string | int16 uzunluk + UTF-8 bytes; -1 = null |
| bytes | int32 uzunluk + raw bytes; -1 = null |
| array\<T\> | int32 eleman sayısı + elemanlar; -1 = null |
| bool | int8, 0 veya 1 |

### 5.3 API Key Tablosu
| Key | İsim | Faz |
|---|---|---|
| 0 | Produce | 2 |
| 1 | Fetch | 2 |
| 2 | Metadata | 3 |
| 3 | CreateTopic | 3 |
| 4 | JoinGroup | 4 |
| 5 | SyncGroup | 4 |
| 6 | Heartbeat | 4 |
| 7 | LeaveGroup | 4 |
| 8 | OffsetCommit | 4 |
| 9 | OffsetFetch | 4 |
| 10 | ListOffsets | 4 |
| 11 | ReplicaFetch | 5 |
| 12 | ApiVersions | 2 |

### 5.4 Hata Kodları
```
0   None
1   UnknownError
2   OffsetOutOfRange
3   CorruptMessage
4   UnknownTopicOrPartition
5   NotLeaderForPartition
6   RequestTimedOut
7   MessageTooLarge
8   NotEnoughReplicas
9   UnknownMemberID
10  RebalanceInProgress
11  IllegalGeneration
12  InvalidGroupID
13  UnsupportedVersion
14  TopicAlreadyExists
15  InvalidPartitionCount
```

### 5.5 İstek/Cevap Şemaları

#### Produce (0)
```
Request:
  acks            int16       // 0, 1, -1(all)
  timeoutMs       int32
  topics          array<{
    name          string
    partitions    array<{
      partitionID int32
      recordSet   bytes       // 4.1 formatında ardışık record'lar
    }>
  }>

Response:
  topics          array<{
    name          string
    partitions    array<{
      partitionID  int32
      errorCode    int16
      baseOffset   int64      // atanan ilk offset
      logAppendTime int64
    }>
  }>
```

#### Fetch (1)
```
Request:
  maxWaitMs       int32       // long-poll süresi
  minBytes        int32
  maxBytes        int32
  topics          array<{
    name          string
    partitions    array<{
      partitionID  int32
      fetchOffset  int64
      maxBytes     int32
    }>
  }>

Response:
  topics          array<{
    name          string
    partitions    array<{
      partitionID       int32
      errorCode         int16
      highWatermark     int64
      logStartOffset    int64
      recordSet         bytes
    }>
  }>
```

**Long-poll davranışı:** Broker `minBytes` kadar veri birikene veya `maxWaitMs` dolana kadar bekler. Boş cevap dönmek yerine bloklar. Bu, busy-wait'i engeller ve MUST implement edilir.

#### Metadata (2)
```
Request:
  topics          array<string>   // null = tüm topic'ler

Response:
  brokers         array<{ nodeID int32, host string, port int32 }>
  topics          array<{
    name          string
    errorCode     int16
    partitions    array<{
      partitionID  int32
      leader       int32
      replicas     array<int32>
      isr          array<int32>
    }>
  }>
```

#### CreateTopic (3)
```
Request:
  topics array<{ name string, numPartitions int32, replicationFactor int16 }>
Response:
  topics array<{ name string, errorCode int16 }>
```

#### JoinGroup (4)
```
Request:
  groupID          string
  sessionTimeoutMs int32
  memberID         string      // ilk katılımda ""
  protocolType     string      // "consumer"
  protocols        array<{ name string, metadata bytes }>

Response:
  errorCode        int16
  generationID     int32
  protocolName     string
  leaderID         string      // group leader'ın memberID'si
  memberID         string      // bu member'ın atanan ID'si
  members          array<{ memberID string, metadata bytes }>  // sadece leader'a dolu gelir
```

#### SyncGroup (5)
```
Request:
  groupID          string
  generationID     int32
  memberID         string
  assignments      array<{ memberID string, assignment bytes }>  // sadece leader gönderir

Response:
  errorCode        int16
  assignment       bytes       // bu member'ın partition listesi
```

`assignment` bytes içeriği:
```
  topics array<{ name string, partitions array<int32> }>
```

#### Heartbeat (6)
```
Request:  groupID string, generationID int32, memberID string
Response: errorCode int16
```

#### LeaveGroup (7)
```
Request:  groupID string, memberID string
Response: errorCode int16
```

#### OffsetCommit (8)
```
Request:
  groupID      string
  generationID int32
  memberID     string
  topics       array<{ name string, partitions array<{ partitionID int32, offset int64, metadata string }> }>
Response:
  topics       array<{ name string, partitions array<{ partitionID int32, errorCode int16 }> }>
```

#### OffsetFetch (9)
```
Request:  groupID string, topics array<{ name string, partitions array<int32> }>
Response: topics array<{ name string, partitions array<{ partitionID int32, offset int64, errorCode int16 }> }>
```

#### ListOffsets (10)
```
Request:  topics array<{ name string, partitions array<{ partitionID int32, timestamp int64 }> }>
          // timestamp: -1 = latest, -2 = earliest
Response: topics array<{ name string, partitions array<{ partitionID int32, errorCode int16, offset int64 }> }>
```

#### ReplicaFetch (11)
```
Request:
  replicaID    int32
  maxWaitMs    int32
  topics       array<{ name string, partitions array<{ partitionID int32, fetchOffset int64 }> }>
Response:
  topics       array<{ name string, partitions array<{
    partitionID int32, errorCode int16, highWatermark int64, recordSet bytes }> }>
```

---

## 6. Faz Faz Uygulama Planı

Her fazın sonunda DoD listesi tamamen işaretlenmeden bir sonrakine geçilmeyecek.

---

### FAZ 1 — Storage Katmanı

**Amaç:** Ağ yok, broker yok. Sadece diske yazan ve offset ile okuyan bir commit log.

#### 1.1 Görevler

**T1.1 — Record encode/decode** (`internal/storage/record.go`)
```go
type Record struct {
    Offset     int64
    Timestamp  int64
    Attributes int8
    Key        []byte
    Value      []byte
}

func (r *Record) Encode(w io.Writer) (int, error)
func DecodeRecord(r io.Reader) (*Record, int, error)
func (r *Record) EncodedSize() int
```
- CRC hesaplama ve doğrulama
- `ErrCorruptRecord` tanımı

**T1.2 — Index** (`internal/storage/index.go`)
```go
type Index struct { /* mmap'lenmiş dosya */ }

func NewIndex(path string, maxBytes int64) (*Index, error)
func (i *Index) Append(relativeOffset uint32, position uint32) error
func (i *Index) Lookup(relativeOffset uint32) (position uint32, found bool, err error)
func (i *Index) Truncate() error   // segment kapanınca gerçek boyuta indir
func (i *Index) Close() error
```
- `Lookup` binary search yapar, tam eşleşme yoksa **en yakın küçük** entry'yi döner
- mmap kullanımı: `unix.Mmap` / `unix.Munmap`

**T1.3 — Segment** (`internal/storage/segment.go`)
```go
type Segment struct {
    BaseOffset int64
    NextOffset int64
    // ...
}

func NewSegment(dir string, baseOffset int64, cfg Config) (*Segment, error)
func (s *Segment) Append(rec *Record) (offset int64, err error)
func (s *Segment) Read(offset int64) (*Record, error)
func (s *Segment) ReadFrom(offset int64, maxBytes int32) ([]*Record, error)
func (s *Segment) IsFull() bool
func (s *Segment) Size() int64
func (s *Segment) Close() error
func (s *Segment) Remove() error
```
- Append sırasında `index.interval.bytes` dolduğunda index'e entry ekle
- Yazma `bufio.Writer` ile buffer'lansın, `Flush()` kontrollü çağrılsın

**T1.4 — Log** (`internal/storage/log.go`)
```go
type Log struct { /* segment listesi */ }

func NewLog(dir string, cfg Config) (*Log, error)
func (l *Log) Append(rec *Record) (int64, error)
func (l *Log) AppendBatch(recs []*Record) (baseOffset int64, err error)
func (l *Log) Read(offset int64) (*Record, error)
func (l *Log) ReadFrom(offset int64, maxBytes int32) ([]*Record, error)
func (l *Log) LowestOffset() int64
func (l *Log) HighestOffset() int64   // LEO
func (l *Log) Truncate(offset int64) error
func (l *Log) Close() error
```
- Açılışta recovery (bölüm 4.5)
- Segment rotation
- Retention: `retention.ms` ve `retention.bytes` dolduğunda eski segment sil (background goroutine)
- Concurrency: `sync.RWMutex`, append yazma kilidi, read okuma kilidi

#### 1.2 Testler (MUST)
- Round-trip: encode → decode aynı record
- Bozuk CRC → `ErrCorruptRecord`
- 100.000 record yaz, rastgele 1000 offset oku, hepsi doğru
- Segment rotation: küçük `segment.bytes` ile 5 segment oluştuğunu doğrula
- Recovery: dosyayı ortadan kes, tekrar aç, LEO doğru mu
- Truncate: offset'ten sonrası silinmiş mi
- Retention: eski segment'ler siliniyor mu
- `-race` ile paralel append/read testi

#### 1.3 DoD
- [ ] `go test ./internal/storage/... -race` yeşil
- [ ] Coverage >= %80
- [ ] `docs/STORAGE.md` yazıldı
- [ ] Benchmark: `BenchmarkLogAppend` ve `BenchmarkLogRead` sonuçları kaydedildi
- [ ] `docs/PHASE_1.md` yazıldı

---

### FAZ 2 — Protokol ve TCP Server

**Amaç:** İki ayrı process konuşsun. Producer yazsın, Consumer okusun.

#### 2.1 Görevler

**T2.1 — Codec** (`internal/protocol/codec.go`)
- Primitive encoder/decoder (bölüm 5.2)
- `Encoder` ve `Decoder` struct'ları, buffer tabanlı
- Decoder MUST bounds check yapsın (kötü niyetli/bozuk paket panik'e yol açmayacak)

**T2.2 — Frame** (`internal/protocol/frame.go`)
```go
func ReadFrame(r io.Reader, maxSize int32) (*RequestHeader, []byte, error)
func WriteFrame(w io.Writer, correlationID int32, errorCode int16, payload []byte) error
```

**T2.3 — Request/Response tipleri** (`requests.go`, `responses.go`)
- Faz 2 için: `ApiVersions`, `Produce`, `Fetch`
- Her tip için `Encode()` / `Decode()` metodu

**T2.4 — TCP Server** (`internal/server/`)
```go
type Server struct { /* ... */ }
func New(broker *broker.Broker, cfg Config) *Server
func (s *Server) ListenAndServe(ctx context.Context) error
func (s *Server) Shutdown(ctx context.Context) error
```
- Accept loop, connection başına goroutine
- Read/write timeout
- Graceful shutdown (SIGTERM → mevcut istekleri bitir, yenilerini reddet)
- Connection limit (`max.connections`, varsayılan 1024)

**T2.5 — Handler** (`internal/server/handler.go`)
- apiKey → handler fonksiyon dispatch tablosu
- Bilinmeyen apiKey → `UnsupportedVersion` hatası, connection kapanmasın

**T2.6 — Broker iskeleti** (`internal/broker/`)
- Tek topic, tek partition, hardcoded (Faz 3'te genişleyecek)
- `Produce` → `Log.AppendBatch`
- `Fetch` → `Log.ReadFrom` + long-poll

**T2.7 — Client** (`pkg/client/`)
```go
type Producer struct{}
func NewProducer(addrs []string, cfg ProducerConfig) (*Producer, error)
func (p *Producer) Send(ctx context.Context, topic string, partition int32, key, value []byte) (int64, error)
func (p *Producer) SendBatch(ctx context.Context, topic string, partition int32, msgs []Message) ([]int64, error)
func (p *Producer) Close() error

type Consumer struct{}
func NewConsumer(addrs []string, cfg ConsumerConfig) (*Consumer, error)
func (c *Consumer) Fetch(ctx context.Context, topic string, partition int32, offset int64) ([]Message, error)
func (c *Consumer) Close() error
```
- Producer'da batching: `linger.ms` ve `batch.size` ile mesajları biriktir
- Correlation ID takibi (in-flight request map'i)
- Otomatik reconnect + exponential backoff

**T2.8 — CLI'lar** (`cmd/`)
```
mini-kafka-broker --config config/broker.yaml
mini-kafka-producer --broker localhost:9092 --topic test
mini-kafka-consumer --broker localhost:9092 --topic test --from-beginning
```

#### 2.2 Long-poll Detayı (kritik)
Fetch isteği geldiğinde:
1. Eğer `fetchOffset < LEO` ise hemen veri dön
2. Değilse `maxWaitMs` süreli bekleme kuyruğuna gir
3. Partition'a yeni append olunca bekleyen fetcher'lar uyandırılır (`sync.Cond` veya channel broadcast)
4. Timeout dolarsa boş recordSet ile dön

#### 2.3 Testler
- Codec fuzz testi: rastgele byte dizileri decoder'ı panic'letmemeli
- Frame round-trip
- Integration: broker başlat, 10.000 mesaj produce et, hepsini consume et, sırayı doğrula
- Long-poll: consumer bekliyorken produce et, 100ms içinde uyandığını doğrula
- Graceful shutdown: shutdown sırasında in-flight istek kaybolmuyor
- Bozuk frame gönder → connection kapanmıyor, hata dönüyor

#### 2.4 DoD
- [ ] Uçtan uca produce/consume çalışıyor
- [ ] `docs/PROTOCOL.md` tamamlandı
- [ ] Fuzz test 1 dakika çalıştırıldı, crash yok
- [ ] `go test ./... -race` yeşil
- [ ] `docs/PHASE_2.md` yazıldı

---

### FAZ 3 — Topic ve Partition

**Amaç:** Çoklu topic, çoklu partition, metadata keşfi.

#### 3.1 Görevler

**T3.1 — Partition** (`internal/broker/partition.go`)
```go
type Partition struct {
    Topic         string
    ID            int32
    log           *storage.Log
    highWatermark int64
    // ...
}
```
- Partition başına **tek writer goroutine** + buffered channel
- `appendCh chan appendRequest` ile serileştirilmiş yazma
- Bu tasarım lock contention'ı kaldırır ve batching'i doğal yapar

**T3.2 — Topic** (`internal/broker/topic.go`)
```go
type Topic struct {
    Name       string
    Partitions map[int32]*Partition
}
func (t *Topic) PartitionFor(key []byte) int32
```
- Key varsa: `murmur2(key) % numPartitions` (Kafka uyumluluğu için murmur2 kullan, elle yaz)
- Key yoksa: round-robin (sticky partitioner: batch dolana kadar aynı partition)

**T3.3 — Metadata** (`internal/broker/metadata.go`)
- Topic listesi, partition sayıları, leader bilgisi
- Disk'te `meta/topics.json` olarak persist

**T3.4 — CreateTopic ve Metadata API'leri**
- Auto-create topic opsiyonu (`auto.create.topics.enable`, varsayılan false)

**T3.5 — Client tarafı metadata cache**
- Producer/Consumer açılışta Metadata isteği atar
- `metadata.max.age.ms` (varsayılan 300000) sonra yeniler
- `NotLeaderForPartition` hatası alınca hemen yeniler

#### 3.2 Testler
- 3 topic, her biri 4 partition, paralel produce, her partition'daki sıra korunuyor
- Aynı key'li mesajlar hep aynı partition'a gidiyor
- murmur2 implementasyonu Kafka'nın referans değerleriyle uyuşuyor (bilinen test vektörleri)
- Broker restart sonrası topic'ler geri geliyor
- Var olan topic'i tekrar create → `TopicAlreadyExists`

#### 3.3 DoD
- [ ] Çoklu topic/partition uçtan uca çalışıyor
- [ ] Metadata persist ve recovery çalışıyor
- [ ] Partition başına ordering garantisi test edildi
- [ ] `docs/PHASE_3.md` yazıldı

---

### FAZ 4 — Consumer Group ve Offset Yönetimi

**Amaç:** Bu projenin en çok "aha" anı barındıran fazı. Dikkatli ol.

#### 4.1 Group State Machine
```
                    ┌─────────┐
                    │  Empty  │
                    └────┬────┘
                         │ ilk JoinGroup
                         ▼
                 ┌───────────────────┐
      ┌─────────▶│ PreparingRebalance│◀─────────┐
      │          └─────────┬─────────┘          │
      │                    │ tüm member join etti│
      │                    ▼                    │ yeni member / member ayrıldı
      │          ┌───────────────────┐          │ / heartbeat timeout
      │          │CompletingRebalance│          │
      │          └─────────┬─────────┘          │
      │                    │ leader SyncGroup gönderdi
      │                    ▼                    │
      │             ┌────────────┐              │
      └─────────────│   Stable   │──────────────┘
                    └────────────┘
```

#### 4.2 Görevler

**T4.1 — Group Coordinator** (`internal/coordinator/group.go`)
```go
type Group struct {
    ID           string
    State        GroupState
    GenerationID int32
    LeaderID     string
    Members      map[string]*Member
    Protocol     string
}

type Member struct {
    ID               string
    SessionTimeoutMs int32
    LastHeartbeat    time.Time
    Metadata         []byte
    Assignment       []byte
}
```

**JoinGroup akışı:**
1. `memberID == ""` ise yeni ID üret: `{clientID}-{uuid}`
2. Group state `Stable` ise → `PreparingRebalance`'a geç, generationID++
3. Tüm mevcut member'ların JoinGroup göndermesini bekle (`rebalance.timeout.ms`, varsayılan 60s)
4. İlk katılan member = group leader
5. Tüm member'lar geldiğinde `CompletingRebalance`'a geç, herkese response dön
6. Leader'a member listesi ve metadata'ları dolu gönder, diğerlerine boş

**SyncGroup akışı:**
1. Leader assignment hesaplar, SyncGroup ile gönderir
2. Coordinator assignment'ları saklar, bekleyen tüm member'lara kendi payını döner
3. State `Stable`'a geçer

**Heartbeat akışı:**
1. Member `heartbeat.interval.ms` (varsayılan 3000) periyodunda heartbeat atar
2. Coordinator `session.timeout.ms` (varsayılan 45000) içinde heartbeat alamazsa member'ı düşürür → rebalance
3. Rebalance sırasında heartbeat'e `RebalanceInProgress` dönülür → client rejoin eder

**T4.2 — Assignor** (`internal/coordinator/assignor.go`)
İki strateji MUST implement edilecek:

```go
type Assignor interface {
    Name() string
    Assign(members []MemberMeta, topics map[string]int32) map[string][]TopicPartition
}
```

**Range:** Her topic için partition'lar sıralanır, member sayısına bölünür, ardışık bloklar dağıtılır.
**RoundRobin:** Tüm topic-partition'lar tek listede sıralanır, member'lara sırayla dağıtılır.

Opsiyonel (bonus): **Sticky** — rebalance sonrası mevcut atamaları mümkün olduğunca koru.

**T4.3 — Offset Store** (`internal/coordinator/offsets.go`)
- Offset'ler `__consumer_offsets` adlı **internal topic'e** yazılır (Kafka gibi)
- 50 partition, key = `{groupID}:{topic}:{partition}`, value = offset
- Bellekte map cache, disk'e compact edilmiş log
- Broker restart'ta topic replay edilerek cache doldurulur

**T4.4 — Group Consumer Client** (`pkg/client/group_consumer.go`)
```go
type GroupConsumer struct{}
func NewGroupConsumer(addrs []string, groupID string, topics []string, cfg Config) (*GroupConsumer, error)
func (c *GroupConsumer) Poll(ctx context.Context, timeout time.Duration) ([]Message, error)
func (c *GroupConsumer) Commit(ctx context.Context) error
func (c *GroupConsumer) CommitOffset(ctx context.Context, tp TopicPartition, offset int64) error
func (c *GroupConsumer) Close() error   // LeaveGroup gönderir
```
- Arka planda heartbeat goroutine
- `enable.auto.commit` (varsayılan true), `auto.commit.interval.ms` (5000)
- Rebalance callback: `OnPartitionsRevoked`, `OnPartitionsAssigned`
- `auto.offset.reset`: `earliest` | `latest` | `none`

#### 4.3 Testler (Bu faz için ekstra sıkı)
- 1 consumer, 6 partition → hepsi ona atanır
- 3 consumer, 6 partition → her birine 2 partition
- 7 consumer, 6 partition → 1 consumer boşta kalır
- Consumer eklendiğinde rebalance tetikleniyor ve dağıtım doğru
- Consumer `Close()` çağırınca partition'ları başkası devralıyor
- Consumer'ı SIGKILL ile öldür → `session.timeout.ms` sonra rebalance
- Offset commit → yeni consumer commit edilen yerden devam ediyor
- Broker restart → offset'ler kayıp değil
- Rebalance sırasında mesaj kaybı yok, duplicate at-least-once sınırında
- **Race testi:** 10 consumer aynı anda join/leave, 60 saniye, `-race` ile crash yok

#### 4.4 DoD
- [ ] Tüm rebalance senaryoları test edildi
- [ ] `__consumer_offsets` persist ve recovery çalışıyor
- [ ] State machine diyagramı `docs/PHASE_4.md`'de
- [ ] Chaos testi: rastgele consumer öldürme, 5 dakika, mesaj kaybı yok
- [ ] `docs/PHASE_4.md` yazıldı

---

### FAZ 5 — Replikasyon ve ISR

**Amaç:** Multi-broker. Bu fazın zorluğu küçümsenmemeli, buffer bırak.

#### 5.1 Basitleştirmeler (bilinçli)
- **Consensus yok.** Leader ataması config'ten statik gelir veya basit bir controller broker tarafından yapılır.
- Controller = en düşük nodeID'li broker (statik).
- Broker listesi config'te sabit, dinamik cluster membership yok.

#### 5.2 Görevler

**T5.1 — Replica state** (`internal/replication/isr.go`)
```go
type ReplicaState struct {
    BrokerID          int32
    LogEndOffset      int64
    LastCaughtUpTime  time.Time
    InSync            bool
}
```
- ISR kriteri: `time.Since(LastCaughtUpTime) < replica.lag.time.max.ms` (varsayılan 30000)
- Follower LEO leader LEO'ya eşit olduğunda `LastCaughtUpTime` güncellenir

**T5.2 — High Watermark**
```
HW = min(ISR'daki tüm replica'ların LEO'su)
```
- Consumer'lar **sadece HW'ye kadar** okuyabilir (MUST)
- HW her ReplicaFetch sonrası yeniden hesaplanır
- HW disk'e periyodik yazılır (`replication-offset-checkpoint` dosyası)

**T5.3 — Follower** (`internal/replication/follower.go`)
- Her follower, her partition için leader'a sürekli ReplicaFetch atar (long-poll)
- Gelen record'ları kendi log'una **offset'leri koruyarak** yazar
- Leader'ın HW'sini alır ve kendi HW'sini günceller

**T5.4 — Leader** (`internal/replication/leader.go`)
- ReplicaFetch isteklerini karşılar
- Her fetch'te ilgili replica'nın LEO'sunu günceller
- `acks=all` producer isteklerini ISR yetişene kadar bekletir (purgatory)

**T5.5 — Purgatory**
```go
type Purgatory struct { /* bekleyen istekler */ }
```
- `acks=all` produce istekleri burada bekler
- HW ilerlediğinde tamamlananlar serbest bırakılır
- Timeout olanlara `RequestTimedOut` dönülür

**T5.6 — Leader Failover**
- Controller broker heartbeat ile diğer broker'ları izler
- Leader düşerse ISR'daki bir replica yeni leader olur
- Yeni leader `Leader Epoch`'u artırır
- Eski leader geri gelirse: kendi log'unu yeni leader'ın HW'sine kadar truncate eder

**T5.7 — Leader Epoch**
- Her leader değişiminde epoch++
- Producer/Consumer eski epoch ile istek atarsa `NotLeaderForPartition`
- Bu, split-brain'de veri kaybını engeller

#### 5.3 Testler
- 3 broker, replicationFactor=3 → tüm replica'lar aynı veriye sahip
- Follower'ı durdur → 30s sonra ISR'dan düşüyor
- Follower'ı geri başlat → yetişince ISR'a giriyor
- Leader'ı öldür → yeni leader seçiliyor, producer devam ediyor
- `acks=all` + 1 replica down + `min.insync.replicas=2` → `NotEnoughReplicas`
- Consumer HW'nin ötesini okuyamıyor
- **Split-brain testi:** Eski leader geri döndüğünde truncate ediyor, divergence yok

#### 5.4 DoD
- [ ] 3 broker'lı cluster docker-compose ile ayağa kalkıyor
- [ ] Leader failover 10 saniyeden kısa sürede tamamlanıyor
- [ ] Failover sırasında `acks=all` ile mesaj kaybı YOK (test ile kanıtla)
- [ ] ISR shrink/expand loglanıyor
- [ ] `docs/PHASE_5.md` yazıldı — bu fazın zorlukları detaylı anlatılmış

---

### FAZ 6 — Benchmark ve Dokümantasyon

**Amaç:** İddiaları rakamla destekle. İçeriğin değerinin çoğu burada.

#### 6.1 Benchmark Metodolojisi

**Ortam standardizasyonu (MUST):**
- Aynı makine, aynı disk, aynı OS
- Her test 3 kez, medyan raporlanır
- Warm-up: her testten önce 30 saniye ısınma, ölçüme dahil değil
- Kafka: `docker-compose.yml` ile, JVM heap 2GB sabit
- mini-kafka: aynı partition/replica konfigürasyonu
- OS page cache her test arası temizlenir (`sync; echo 3 > /proc/sys/vm/drop_caches`)

**Ölçülecek senaryolar:**

| # | Senaryo | Parametreler |
|---|---|---|
| 1 | Tek producer throughput | 1KB mesaj, acks=1, 1M mesaj |
| 2 | Mesaj boyutu etkisi | 100B / 1KB / 10KB / 100KB |
| 3 | acks etkisi | acks=0, 1, all |
| 4 | Batch etkisi | batch.size = 1, 100, 1000, 10000 |
| 5 | Partition sayısı etkisi | 1, 4, 16, 64 partition |
| 6 | Consumer throughput | tek consumer, 1M mesaj |
| 7 | Consumer group throughput | 1, 4, 16 consumer |
| 8 | End-to-end latency | p50, p95, p99, p999 |
| 9 | Rebalance süresi | 1→2, 2→4, 4→8 consumer |
| 10 | Recovery süresi | 10GB log ile broker restart |
| 11 | Bellek kullanımı | RSS, heap, GC pause |
| 12 | Disk kullanımı | aynı veri için dosya boyutu |

**Raporlanacak metrikler:**
- Throughput: msg/sec ve MB/sec
- Latency: p50, p95, p99, p999 (histogram)
- CPU: user + sys time
- Bellek: peak RSS
- GC: pause süreleri (Go için `runtime.ReadMemStats`, Kafka için GC log)

#### 6.2 Görevler
- **T6.1** — Benchmark harness (`benchmark/`), sonuçları JSON'a yazsın
- **T6.2** — Latency histogram (HDR histogram mantığını elle yaz veya basit bucket)
- **T6.3** — Kafka docker-compose (KRaft mode, tek broker + 3 broker varyantı)
- **T6.4** — Sonuç görselleştirme (Go ile SVG üret veya CSV çıkar)
- **T6.5** — `docs/BENCHMARK.md`: metodoloji + sonuçlar + **yorumlama**

#### 6.3 Yorumlama Bölümü (en değerli kısım)
`docs/BENCHMARK.md` şu soruları cevaplayacak:
- Kafka nerede daha hızlı? **Neden?** (zero-copy sendfile, page cache stratejisi, JVM JIT)
- mini-kafka nerede yakın veya daha iyi? (küçük mesaj, düşük partition sayısı, GC farkı)
- Hangi Kafka optimizasyonunu implement etmedik ve maliyeti ne kadar oldu?
- Ölçekleme eğrileri nerede ayrışıyor?

#### 6.4 README İçeriği (MUST)
1. Ne bu, neden yazıldı
2. Mimari diyagram (ASCII veya SVG)
3. 30 saniyede çalıştır (quickstart)
4. Neler destekleniyor / neler desteklenmiyor (dürüst tablo)
5. Benchmark özeti + detaya link
6. "Öğrendiklerim" — en zor 5 problem ve çözümleri
7. Kafka'dan farklar tablosu

#### 6.5 DoD
- [ ] 12 senaryonun tamamı çalıştırıldı, sonuçlar kaydedildi
- [ ] `docs/BENCHMARK.md` yorumlama bölümüyle birlikte tamamlandı
- [ ] README tamamlandı
- [ ] Repo dışarıdan `git clone && make run` ile çalışıyor
- [ ] GitHub Actions CI yeşil

---

## 7. Konfigürasyon Referansı

`config/broker.yaml`:
```yaml
broker:
  id: 1
  host: "0.0.0.0"
  port: 9092
  data_dir: "/var/lib/mini-kafka"
  max_connections: 1024
  request_timeout_ms: 30000

log:
  segment_bytes: 134217728        # 128 MB
  segment_ms: 604800000           # 7 gün
  index_interval_bytes: 4096
  index_max_bytes: 10485760       # 10 MB
  retention_ms: 604800000
  retention_bytes: -1             # sınırsız
  flush_messages: 0
  flush_ms: 1000
  max_message_bytes: 1048576      # 1 MB

topic:
  auto_create: false
  default_partitions: 3
  default_replication_factor: 1

replication:
  replica_lag_time_max_ms: 30000
  replica_fetch_max_bytes: 1048576
  replica_fetch_wait_max_ms: 500
  min_insync_replicas: 1

group:
  session_timeout_ms: 45000
  heartbeat_interval_ms: 3000
  rebalance_timeout_ms: 60000
  offsets_topic_partitions: 50

cluster:
  brokers:
    - id: 1
      host: "broker1"
      port: 9092
    - id: 2
      host: "broker2"
      port: 9092
    - id: 3
      host: "broker3"
      port: 9092
```

---

## 8. Test Stratejisi

### 8.1 Test Piramidi
| Seviye | Kapsam | Oran |
|---|---|---|
| Unit | Tek fonksiyon/struct | %60 |
| Integration | Broker + client, tek process | %30 |
| E2E | Çoklu process/container | %10 |

### 8.2 Zorunlu Test Tipleri
- **Fuzz:** Protokol decoder'ları (`go test -fuzz`)
- **Race:** Tüm testler `-race` ile de koşacak
- **Property-based:** Log invariant'ları (offset monotonluğu, LEO >= HW, ISR ⊆ replicas)
- **Chaos:** `test/chaos/` altında — rastgele broker/consumer öldürme, network gecikmesi simülasyonu
- **Soak:** 1 saat sürekli yük, bellek sızıntısı kontrolü

### 8.3 Invariant'lar (her zaman doğru olmalı)
```
1. Bir partition'daki offset'ler kesintisiz artar: o[i+1] = o[i] + 1
2. HW <= LEO
3. ISR ⊆ Replicas
4. Leader ∈ ISR
5. Committed offset <= HW
6. Bir partition aynı anda en fazla bir consumer'a atanır (aynı group içinde)
7. Consumer HW'nin ötesini okuyamaz
```
Bu invariant'lar için `test/integration/invariants_test.go` yazılacak.

---

## 9. Bilinen Zorluklar (Agent'lar İçin Uyarı)

| Zorluk | Nerede | İpucu |
|---|---|---|
| mmap + dosya büyütme | Faz 1, index | Preallocate et, sonra truncate |
| Index relative offset overflow | Faz 1 | uint32 sınırı → segment 4B record'dan fazla olamaz, kontrol et |
| Partial write recovery | Faz 1 | Son record yarım kalabilir, CRC ile yakala ve truncate et |
| Long-poll goroutine sızıntısı | Faz 2 | Her bekleyen için context iptal yolu bırak |
| Correlation ID yarışı | Faz 2, client | Map'e mutex, response gelmeden connection kapanırsa temizle |
| Rebalance thundering herd | Faz 4 | Tüm consumer aynı anda rejoin eder, coordinator'ı boğmasın |
| Rebalance sırasında commit | Faz 4 | Revoked callback'te commit et, sonra bırak |
| Generation ID yarışı | Faz 4 | Eski generation'dan gelen istekleri reddet |
| HW ilerlemesi ve purgatory | Faz 5 | HW güncellenince bekleyenleri uyandırmayı unutma |
| Log divergence | Faz 5 | Leader epoch olmadan çözülmez, mutlaka implement et |
| Follower truncation | Faz 5 | Yeni leader'ın epoch başlangıç offset'ini sor, oraya truncate et |

---

## 10. Sıralı Yürütme Talimatı

Agent bu sırayla çalışacak:

```
1.  Repo iskeletini oluştur (bölüm 3), Makefile, CI, linter config
2.  FAZ 1 → T1.1 → T1.2 → T1.3 → T1.4 → testler → DoD
3.  FAZ 2 → T2.1 → ... → T2.8 → testler → DoD
4.  FAZ 3 → T3.1 → ... → T3.5 → testler → DoD
5.  FAZ 4 → T4.1 → ... → T4.4 → testler → DoD
6.  FAZ 5 → T5.1 → ... → T5.7 → testler → DoD
7.  FAZ 6 → T6.1 → ... → T6.5 → DoD
```

Her görev bittiğinde:
```bash
gofmt -l . && go vet ./... && go test ./... -race && golangci-lint run
```
Hepsi temiz değilse bir sonraki göreve geçme.

---

## 11. Makefile (başlangıç)

```makefile
.PHONY: build test test-race lint bench clean run

build:
	go build -o bin/broker ./cmd/broker
	go build -o bin/producer ./cmd/producer
	go build -o bin/consumer ./cmd/consumer

test:
	go test ./... -count=1

test-race:
	go test ./... -race -count=1

test-integration:
	go test ./test/integration/... -tags=integration -count=1

lint:
	gofmt -l .
	go vet ./...
	golangci-lint run

bench:
	go test ./benchmark/... -bench=. -benchmem -benchtime=10s

cover:
	go test ./... -coverprofile=coverage.out
	go tool cover -html=coverage.out -o coverage.html

run: build
	./bin/broker --config config/broker.yaml

clean:
	rm -rf bin/ coverage.out coverage.html /tmp/mini-kafka-*
```

---

## 12. Bonus Fazlar (Ana plan bittikten sonra)

| Faz | Konu | Değer |
|---|---|---|
| 7 | Log compaction (key bazlı) | Kafka'nın en az anlaşılan özelliği, yazı potansiyeli yüksek |
| 8 | Transactional producer / exactly-once | Çok zor, ama "EOS'u implement ettim" güçlü |
| 9 | Tiered storage (S3'e soğuk segment) | Güncel trend |
| 10 | Prometheus metrics + Grafana dashboard | Operasyonel olgunluk sinyali |
| 11 | Raft ile gerçek controller | Distributed systems'ın zirvesi |

---

## 13. İçerik Çıktısı Planı

Kod bittikten sonra yazılacaklar (her biri ayrı yazı):

1. **"Kafka'nın commit log'unu yazdım — sparse index neden dahiyane"** (Faz 1)
2. **"Binary protokol tasarlamak: her byte'ın bir sebebi var"** (Faz 2)
3. **"Consumer group rebalancing: bu kadar karmaşık olmasının sebebi"** (Faz 4) ← en çok okunacak olan
4. **"ISR, High Watermark ve leader epoch: veri kaybı nasıl önlenir"** (Faz 5)
5. **"1000 satır Go, Kafka'nın %X'i kadar hızlı — nerede ve neden ayrışıyoruz"** (Faz 6)

---

## 14. Agent İçin Son Kontrol Listesi

Her oturum başında agent şunu doğrulasın:
- [ ] Hangi fazdayım? (`docs/` altındaki en son PHASE dosyasına bak)
- [ ] Önceki fazın DoD'u tamamen işaretli mi?
- [ ] `go test ./... -race` şu an yeşil mi?
- [ ] `OPEN_QUESTIONS.md`'de cevap bekleyen kritik soru var mı?

Her oturum sonunda:
- [ ] Testler yeşil
- [ ] Commit atıldı
- [ ] İlgili PHASE dokümanı güncellendi
- [ ] Yeni açık soru varsa yazıldı
