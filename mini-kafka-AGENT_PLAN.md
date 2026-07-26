# mini-kafka — Düzeltme Planı (AI Agent Ekibi)

**Hedef repo:** `github.com/YusuffEren/mini-kafka` (main @ `9834825`)
**Bu doküman:** kod incelemesi sonucu tespit edilen eksiklerin görev bazlı çalışma planı.

---

## 0. Tüm Agent'lar İçin Bağlayıcı Kurallar

Bu bölüm her görevde geçerlidir. Görev metninde tekrar edilmeyecek.

### 0.1 Kapsam disiplini

- **Bir görev = bir commit.** Görev ID'sini commit mesajına yaz: `fix(storage): FlushWriter tam kilit alsın [T-10]`
- Görev metninde adı geçmeyen dosyayı **değiştirme**. İlgisiz bir bug görürsen düzeltme — `docs/FINDINGS.md`'ye yeni satır ekle ve devam et.
- **Yeni bağımlılık eklemek yasak.** `go.mod` yalnızca `golang.org/x/sys` ve `gopkg.in/yaml.v3` içerecek. Bu projenin ana satış noktası; bir agent "kolay olsun diye" `github.com/stretchr/testify` eklerse görev reddedilir.
- Mevcut public API'yi (`pkg/client` imzaları) kırma. Kırılması gereken bir yer varsa görev metninde açıkça yazılıdır.
- Yorum satırlarını silme. Bu repodaki yorumlar "neden" açıklıyor, değerlidir. Kod değişirse yorumu **güncelle**, kaldırma.

### 0.2 Test-önce zorunluluğu

Her davranış düzeltmesi (T-1x ve T-2x serisi) şu sırayla yapılacak:

1. Hatayı **gösteren** bir test yaz. Testi koştur, **başarısız olduğunu** çıktısıyla birlikte raporla.
2. Düzeltmeyi yap.
3. Aynı testi koştur, geçtiğini raporla.
4. `make test-race` tamamını koştur, regresyon olmadığını göster.

Adım 1'in çıktısı olmadan gelen PR reddedilir. "Düzelttim, testler geçiyor" kabul edilebilir kanıt değildir — düzeltmenin bir şeyi düzelttiğini kanıtlaman gerekir.

### 0.3 Definition of Done (her görev)

```bash
gofmt -l .              # boş çıktı
go vet ./...            # temiz
./kontrol.sh            # temiz (golangci-lint dahil)
go test ./... -race -count=1 -timeout 300s   # tamamı geçer
```

Bunlara ek olarak görevin kendi kabul kriteri sağlanmış olacak.

### 0.4 Protokol değişikliği kırmızı çizgisi

`internal/protocol/` altındaki **wire format**'ı (byte layout) değiştiren herhangi bir değişiklik:

- `docs/PROTOCOL.md`'yi aynı commit içinde güncellemek zorundadır,
- `pkg/client` tarafını da aynı commit içinde güncellemek zorundadır,
- `internal/protocol/*_test.go` içindeki golden byte testlerini güncellemek zorundadır.

Bu üçünden biri eksikse commit reddedilir. Wire format değişikliği gerektirmeyen görevlerde protokol byte'larına **dokunma**.

### 0.5 Raporlama formatı

Her görev bitiminde şu formatta rapor:

```
[T-xx] Başlık
Değişen dosyalar: ...
Önce (kanıt): <başarısız test çıktısı>
Sonra (kanıt): <geçen test çıktısı>
DoD: gofmt ✓ vet ✓ kontrol.sh ✓ race ✓
Kapsam dışı gözlem: (varsa, docs/FINDINGS.md'ye eklendi)
```

---

## 1. Dalga Yapısı ve Sıralama

Dalgalar sıralı. Bir dalga bitmeden sonrakine geçilmez. Dalga içindeki görevler paralel çalıştırılabilir, **aksi belirtilmedikçe**.

| Dalga | İçerik | Neden bu sırada |
|---|---|---|
| **W0** | Doğruluk & build bütünlüğü | Kod mantığına dokunmaz, risk sıfır, en yüksek getiri. Yanlış iddialar en pahalı hata. |
| **W1** | Correctness bug'ları | Performansı optimize etmeden önce doğru çalışmalı. |
| **W2** | Dayanıklılık & hata görünürlüğü | W1 üstüne kurulur. |
| **W3** | Performans | W1/W2 bittikten sonra; yoksa hızlı ama bozuk kod optimize edilmiş olur. |
| **W4** | Gerçek batching + benchmark | W3'ten sonra ölçüm anlamlı olur. |
| **W5** | Temizlik | Son. |

---

# W0 — Doğruluk ve Build Bütünlüğü

Bu dalganın tamamı tek agent'a verilebilir. Kod mantığı değişmiyor.

---

## T-01 — README'deki "Kafka protokol uyumluluğu" iddiasını düzelt

**Dosya:** `README.md`

**Sorun:** README ilk satırda *"Kafka binary protokolüyle uyumlu bir mesaj kuyruğu"* diyor. Bu doğru değil:

| | Gerçek Kafka | mini-kafka |
|---|---|---|
| Request header | `api_key, api_version, correlation_id, client_id` sonra **doğrudan** body | body'nin önüne fazladan `int32` uzunluk öneki var (`frame.go`, `RequestFrame.Payload` = `Bytes()` ile okunuyor) |
| Response header | sadece `correlation_id` | `correlation_id` + **`error_code int16`** (Kafka'da frame seviyesinde yok) |
| API key numaraları | Produce=0, Fetch=1, **ListOffsets=2, Metadata=3** | Produce=0, Fetch=1, **Metadata=2, CreateTopics=3** |
| Kayıt formatı | RecordBatch v2 (varint, batch header, batch-level CRC) | özel format (`internal/storage/record.go`), kayıt başına CRC32C |

Sonuç: `librdkafka`, `sarama`, `kafka-go`, `kcat` bu broker'a bağlanamaz.

**Yapılacak:** İlk paragrafı değiştir. Öneri:

> Go ile yazılmış, Apache Kafka'nın mimarisinden (segment'li log, sparse index, consumer group, ISR replikasyonu) esinlenen bir mesaj kuyruğu. **Kendi binary protokolünü** kullanır — Kafka wire protokolüyle uyumlu *değildir*, resmi Kafka istemcileri bağlanamaz. Protokol `docs/PROTOCOL.md` içinde tanımlıdır.

Ayrıca "Desteklenen Kafka API'leri" başlığını "Desteklenen API'ler" yap ve altına *"API isimleri Kafka'daki karşılıklarından esinlenmiştir; key numaraları ve payload düzenleri Kafka ile aynı değildir"* notu ekle.

**Kabul kriteri:** README'de Kafka istemci uyumluluğu ima eden hiçbir cümle kalmayacak. `grep -in "kafka.*uyumlu\|kafka protokol" README.md` çıktısı ya boş olacak ya da açık bir olumsuzlama içerecek.

**Not agent'a:** Bu görev projeyi küçültmüyor. "Kafka'dan esinlenen kendi protokolü olan sistem" ifadesi, denetlendiğinde çökmeyen bir ifadedir; mevcut ifade 30 saniyede çürütülebilir. Doğruluk burada satış gücünden önce gelir.

---

## T-02 — Silinmiş protokol spec'ini geri getir, kırık referansları onar

**Sorun:** Son commit (`9834825 Delete MINI_KAFKA_SPEC.md`) protokolün normatif kaynağını sildi. Ama 8 yerde ona atıf var:

```
internal/protocol/codec.go:4        "MINI_KAFKA_SPEC.md Section 5.2"
internal/protocol/frame.go:3        "MINI_KAFKA_SPEC.md Section 5.1"
internal/protocol/requests.go:4     "MINI_KAFKA_SPEC.md Section 5.5"
internal/protocol/responses.go:4    "MINI_KAFKA_SPEC.md Section 5.5"
internal/server/handler.go:11       "MINI_KAFKA_SPEC.md"
internal/config/config.go:169       "MINI_KAFKA_SPEC.md"
docs/STORAGE.md:4                   "MINI_KAFKA_SPEC.md Section 4"
docs/OPEN_QUESTIONS.md:5            "MINI_KAFKA_SPEC.md Section 1.5"
```

Yani şu an spec'i olmayan bir binary protokol var ve kod var olmayan bir dosyaya "normatif kaynak" diye atıf yapıyor.

**Yapılacak — sıralı:**

1. `git show 091a05f:MINI_KAFKA_SPEC.md > /tmp/spec.md` ile silinmiş içeriği çıkar. (Commit `9834825`'in parent'ı `091a05f`.)
2. İçeriği `docs/PROTOCOL.md` ile birleştir. `docs/PROTOCOL.md` zaten 99 satır ve aynı konuyu anlatıyor — **iki doküman olmasın**. Birleşik dosya `docs/PROTOCOL.md` olacak ve şu bölümleri içerecek:
   - `## 1. Frame Formatı` (request + response, byte byte tablo)
   - `## 2. Primitif Tipler` (int8/16/32/64, string, bytes, array, bool — null sentinel'lar dahil)
   - `## 3. Kayıt Formatı` (record.go'daki layout, CRC kapsamı dahil)
   - `## 4. API'ler` (her API key için request/response payload düzeni)
   - `## 5. Hata Kodları` (`internal/server/handler.go`'daki sabitlerin tablosu)
3. Yukarıdaki 8 referansı `docs/PROTOCOL.md` ve doğru bölüm numarasına güncelle.
4. `MINI_KAFKA_SPEC.md`'yi geri **ekleme** — tek kaynak `docs/PROTOCOL.md` olacak.

**Kabul kriteri:**
```bash
grep -rn "MINI_KAFKA_SPEC" . --include=*.go --include=*.md   # boş çıktı
```
ve `docs/PROTOCOL.md` içindeki frame tablosu `internal/protocol/frame.go`'daki gerçek okuma/yazma sırasıyla **birebir** uyuşacak. Agent bunu doğrulamak için `frame.go`'yu okuyup tabloyu satır satır karşılaştırdığını raporunda belirtecek.

---

## T-03 — Dockerfile Go sürümü uyumsuzluğu

**Dosya:** `Dockerfile:1`

**Sorun:** `FROM golang:1.23-alpine` ama `go.mod` `go 1.25.0` diyor. CI 1.25, README "Go 1.25+" diyor. Go 1.23 toolchain'i 1.25 gerektiren bir modülü derlemek için build sırasında toolchain indirmeye çalışır — en iyi durumda sessiz ve yavaş, en kötü durumda (`GOTOOLCHAIN=local` veya kısıtlı ağ) build patlar.

**Yapılacak:**
- `FROM golang:1.25-alpine AS builder`
- Ayrıca aynı dosyada: `WORKDIR /root/` yerine `/app` kullan ve binary'leri root home'a koyma. Non-root user ekle:
  ```dockerfile
  RUN adduser -D -u 10001 minikafka
  USER minikafka
  ```
  Data dizini (`/var/lib/mini-kafka`) bu kullanıcı tarafından yazılabilir olmalı — `RUN mkdir -p /var/lib/mini-kafka && chown minikafka /var/lib/mini-kafka` builder sonrası stage'de.
- 3 ayrı `RUN go build` yerine tek `RUN` içinde birleştir (layer sayısı).

**Kabul kriteri:** `docker build -t mini-kafka:test .` başarılı. `docker run --rm mini-kafka:test ./mini-kafka-broker -h` çalışıyor. Konteyner root olarak koşmuyor (`docker run --rm mini-kafka:test id` çıktısında uid=10001).

---

## T-04 — `docker compose up` çalışan bir sistem vermiyor

**Dosyalar:** `config/broker.yaml`, `docker-compose.yml`, yeni `config/broker-single.yaml`, `README.md`

**Sorun — bu en görünür kırıklık:**

`config/broker.yaml` 3 broker'lık bir cluster tanımlıyor:
```yaml
cluster:
  brokers:
    - {id: 1, host: "broker1", port: 9092}
    - {id: 2, host: "broker2", port: 9092}
    - {id: 3, host: "broker3", port: 9092}
```

`docker-compose.yml` ise tek konteyner kaldırıyor ve bu config'i kullanıyor. Sonuç:

1. `clusterReplicaIDs` (`internal/broker/broker_replication.go:21`) → `[1,2,3]`
2. `staticPartitionLeader(p)` (`broker_replication.go:62`) → `replicaIDs[p % 3]`
3. `default_partitions: 3` → partition 0 lideri broker 1, partition 1 lideri broker 2, partition 2 lideri broker 3
4. `handleProduce` (`broker.go:332`) `isLeader` false ise `ErrNotLeaderForPartition` dönüyor
5. **Sonuç: partition 1 ve 2'ye produce eden istemci hata alıyor. Partition'ların 2/3'ü ölü.**
6. Ek olarak `startFollowerFetchers` var olmayan `broker2`/`broker3` host'larına sürekli bağlanmaya çalışıyor → log spam.

README "Tek broker, 9092 portunda ayağa kalkar" diyor. Teknik olarak kalkıyor, pratikte kullanılamıyor.

**Yapılacak:**

1. **`config/broker-single.yaml` oluştur** — `broker.yaml`'ın kopyası, tek fark:
   ```yaml
   cluster:
     brokers:
       - id: 1
         host: "localhost"
         port: 9092
   ```
   `replication.min_insync_replicas: 1` olarak kalsın.

2. **`Dockerfile`** bu dosyayı kopyalasın: `COPY config/broker-single.yaml /etc/mini-kafka/broker.yaml`

3. **`config/broker.yaml`**'ın başına yorum ekle:
   ```yaml
   # 3 broker'lık cluster örneği. Tek broker çalıştırmak için
   # config/broker-single.yaml kullanın; bu dosyayla tek broker
   # başlatılırsa partition'ların 2/3'ü NotLeaderForPartition döner.
   ```

4. **`docker-compose.yml`**: `mini-kafka` servisini `broker-single.yaml` ile çalıştır. Ayrıca `healthcheck` ekle (broker portuna TCP bağlantısı) ve `restart: unless-stopped`.

5. **Ayrı bir compose dosyası: `docker-compose.cluster.yml`** — 3 servisli (`broker1`, `broker2`, `broker3`), her biri kendi `BROKER_ID` ile, `config/broker.yaml`'ı kullanan gerçek cluster. Her broker için ayrı volume. Bu, replikasyon kodunun gerçekten çalıştığını gösteren demo olacak.
   - Broker ID'yi env'den okuma özelliği yoksa: `config/broker1.yaml`, `broker2.yaml`, `broker3.yaml` üret (tek fark `broker.id` ve `broker.port`).

6. **README** Docker bölümünü güncelle: tek broker ve cluster için ayrı komut, ve cluster modunda 3 broker'ın gerektiği notu.

**Kabul kriteri — agent bunu fiilen koşturup çıktı gösterecek:**
```bash
docker compose up -d
sleep 5
# 3 partition'ın HEPSİNE produce başarılı olmalı
./bin/producer -brokers 127.0.0.1:9092 -topic t1 -key k0 -value v0
./bin/producer -brokers 127.0.0.1:9092 -topic t1 -key k1 -value v1
./bin/producer -brokers 127.0.0.1:9092 -topic t1 -key k2 -value v2
docker compose logs | grep -i "not leader"   # boş olmalı
docker compose logs | grep -i "broker2\|broker3"  # boş olmalı
```

---

## T-05 — LICENSE ve repo meta verisi

**Sorun:** LICENSE dosyası yok. Lisanssız repo hukuken "tüm hakları saklı" demektir; kimse kullanamaz, portföy değerini düşürür. GitHub About bölümü de boş ("No description, website, or topics provided").

**Yapılacak:**
1. `LICENSE` ekle — MIT öneriliyor (izin verici, öğrenci projesi için standart). Telif satırı: `Copyright (c) 2026 Yusuf Eren`
2. `README.md` sonuna `## Lisans` bölümü ekle.
3. Repo owner'a not (agent yapamaz, manuel): GitHub'da About → description (`Go ile yazılmış, Kafka mimarisinden esinlenen mesaj kuyruğu`) ve topics (`go`, `kafka`, `message-queue`, `distributed-systems`, `storage-engine`).

**Kabul kriteri:** `LICENSE` mevcut, README'de referanslı.

---

# W1 — Correctness Bug'ları

**Bu dalganın tamamı için ortak ön koşul: T-19 (stres test altyapısı) ilk yapılacak.** Diğer görevler onun üzerine test yazacak.

---

## T-19 — Eşzamanlılık ve sızıntı test altyapısı (İLK YAPILACAK)

**Yeni dosya:** `test/stress/stress_test.go`

**Neden:** W1'deki bug'ların hiçbirini mevcut test seti yakalamıyor. Çünkü hepsi **eşzamanlılık altında** veya **zaman içinde** ortaya çıkıyor. Düzeltmeleri kanıtlamak için önce onları görünür kılacak altyapı gerekiyor.

**Yapılacak — dört yardımcı:**

1. `TestConcurrentReadWrite` — bir `storage.Log` üzerinde N=8 goroutine sürekli `Append`, M=8 goroutine sürekli `Read`/`ReadFrom` yapacak, 2 saniye. `-race` ile koştuğunda **T-10'daki race'i yakalaması gerekiyor.**

2. `TestListenerNoLeak` — broker ayağa kalkacak, hiç produce edilmeyen bir topic'e 200 kez `MaxWaitMs=50` ile Fetch atılacak. Sonra broker'ın `listeners` map'indeki toplam kanal sayısı okunacak (bunun için `internal/broker`'a test-only bir getter gerekecek: `func (b *Broker) listenerCount() int` — **exported olmasın**, `broker_test.go` ile aynı pakette olduğu için erişilebilir; `test/stress` ayrı pakette olduğu için bu test `internal/broker/broker_leak_test.go` içine gidecek). 200 timeout sonrası sayı **0** olmalı. **T-11'i yakalar.**

3. `TestProducerConcurrentSend` — tek `client.Producer` üzerinden 8 goroutine × 200 mesaj eşzamanlı `Send`. Dönen offset'lerin kümesi tam olarak `[0, 1600)` olmalı; tekrar veya boşluk olmamalı. **T-15'i yakalar.**

4. `TestGoroutineLeak` — helper: test başında ve sonunda `runtime.NumGoroutine()`, broker `Shutdown` sonrası sayının başlangıç seviyesine dönmesi (±2 tolerans, 500ms grace ile retry).

**Kabul kriteri:** Dört test de **mevcut kodda başarısız** olacak. Agent her birinin başarısız çıktısını raporlayacak. Bu dalganın geri kalanı bu testleri yeşile çevirmekle ölçülecek.

**Uyarı agent'a:** Bu görevde **hiçbir üretim kodu düzeltmesi yapma.** Sadece testler + gerekli test-only getter'lar. Testler kırmızı kalacak ve bu doğru sonuç.

---

## T-10 — `Segment.FlushWriter` veri yarışı

**Dosya:** `internal/storage/segment.go:480`

**Sorun:**
```go
func (s *Segment) FlushWriter() error {
    s.mu.RLock()          // ← PAYLAŞIMLI kilit
    defer s.mu.RUnlock()
    ...
    if err := s.writer.Flush(); err != nil {   // ← bufio.Writer'ı MUTASYONA UĞRATIYOR
```

`RLock` paylaşımlıdır, yani iki goroutine aynı anda tutabilir. `bufio.Writer.Flush()` writer'ın iç durumunu (`n`, `err`, buffer) değiştirir. Çağrı yolu:

`Log.Read` (`log.go`, `l.mu.RLock()` altında) → `seg.FlushWriter()`
`Log.ReadFrom` (aynı şekilde) → `seg.FlushWriter()`

`l.mu.RLock()` de paylaşımlı olduğu için **iki eşzamanlı okuyucu aynı `bufio.Writer`'ı eşzamanlı flush ediyor.** Bu tanım gereği veri yarışı; sonucu bozuk log dosyası veya kayıp byte olabilir.

**Yapılacak:** `RLock`/`RUnlock` → `Lock`/`Unlock`.

Bu doğru düzeltme çünkü fonksiyon yazıcı durumunu değiştiriyor. `Append` de `Lock` alıyor, yani ikisi karşılıklı dışlanacak — istenen davranış bu.

**Performans notu:** Bu, her okumanın segment üzerinde exclusive kilit alması demek. T-32'de okuma yolu ayrıca ele alınacak; şimdilik **doğruluk hızdan önce gelir**. Bu görevde optimize etmeye çalışma.

**Kabul kriteri:** `TestConcurrentReadWrite` (T-19) `-race` altında geçiyor. Öncesinde `-race` çıktısında `WARNING: DATA RACE` görülmüş ve raporlanmış olacak.

---

## T-11 — Long-poll listener bellek sızıntısı

**Dosya:** `internal/broker/broker.go:238-262` ve `handleFetch` (`broker.go:409` civarı)

**Sorun:**
```go
func (b *Broker) registerListener(topic string, partitionID int32) chan struct{} {
    ch := make(chan struct{}, 10)
    ...
    b.listeners[topic][partitionID] = append(b.listeners[topic][partitionID], ch)
    return ch
}

func (b *Broker) notifyAppended(topic string, partitionID int32) {
    // ... SADECE append olduğunda temizliyor
    delete(pMap, partitionID)
    for _, ch := range list { close(ch) }
}
```

`handleFetch` içindeki long-poll döngüsü:
```go
ch := b.registerListener(topicName, partID)
...
select {
case <-ch:
case <-time.After(deadline.Sub(now)):   // ← timeout yolunda ch KAYITTAN ÇIKMIYOR
}
```

Timeout ile dönen her Fetch, map'te sonsuza kadar kalan bir kanal bırakıyor. Hiç produce edilmeyen bir topic'e 500ms'de bir poll atan bir consumer, saatte 7200 kanal biriktiriyor. Sadece bellek değil: `notifyAppended` sonunda binlerce kanalı kapatmak için döngüye giriyor ve bunu **produce sıcak yolunda `b.mu` write lock'u altında** yapıyor.

Ek sorun: `time.After` her iterasyonda yeni timer yaratıyor ve `Stop` edilmiyor.

**Yapılacak:**

1. `unregisterListener(topic string, partitionID int32, ch chan struct{})` ekle. Slice'tan o kanalı çıkaracak (sırayı korumak gerekmiyor, swap-remove yeterli). Kanalı **kapatmayacak** — kapatma sorumluluğu `notifyAppended`'da; çift kapatma panic olur. Kanal kaydını sildikten sonra kimse kapatmayacağı için sorun yok.
2. `handleFetch` içinde:
   ```go
   ch := b.registerListener(topicName, partID)
   timer := time.NewTimer(deadline.Sub(now))
   select {
   case <-ch:
   case <-timer.C:
   }
   timer.Stop()
   b.unregisterListener(topicName, partID, ch)
   ```
   `unregisterListener` her iki yolda da çağrılacak. `<-ch` yolunda kanal zaten kapatılmış ve map'ten silinmiş olabilir; `unregisterListener` bu durumda no-op olacak şekilde yazılmalı (kanal listede yoksa sessizce dön).
3. Boşalan `pMap[partitionID]` girdisini ve boşalan `b.listeners[topic]` map'ini sil — yoksa topic başına boş map birikir.

**Ayrıca — ayrı bir kilide taşı:** `b.mu` şu an hem `topics` hem `listeners` map'ini koruyor. `notifyAppended` her produce'ta write lock alıyor ve bu her `getTopic` RLock'u ile çekişiyor. `listeners` için ayrı bir `listenersMu sync.Mutex` ekle. Bu, T-11 kapsamında yapılacak çünkü aynı satırlara dokunuluyor.

**Kabul kriteri:** `TestListenerNoLeak` (T-19) geçiyor: 200 timeout'lu Fetch sonrası listener sayısı 0.

---

## T-12 — Retention broker restart'ında sıfırlanıyor

**Dosyalar:** `internal/storage/segment.go`, `internal/storage/log.go`

**Sorun:** `NewSegment` her segment için `CreatedAt: time.Now()` atıyor (`segment.go:~200`). `NewLog` açılışta diskteki tüm segmentleri `NewSegment` ile yeniden açıyor. Yani **6 gün önce yazılmış bir segment, restart'tan sonra "0 saniye önce yaratılmış" sayılıyor.**

`runRetention` (`log.go`) şuna bakıyor:
```go
if now.Sub(seg.CreatedAt) >= time.Duration(l.config.RetentionMs)*time.Millisecond {
```

Sonuç: `retention_ms` 7 gün ve broker her gün restart ediyorsa, **zaman bazlı retention hiç tetiklenmiyor** ve disk sonsuza kadar büyüyor. Sessiz ve üretimde acı verecek bir hata.

**Yapılacak:** `CreatedAt`'i disk gerçeğinden türet.

Tercih sırası:
1. **En doğru:** segmentteki **son kaydın timestamp'i**. Kafka'nın yaptığı bu (`largestTimestamp`). `rebuildSegment` zaten segmenti baştan sona tarıyor — orada son geçerli kaydın `rec.Timestamp`'ini yakalayıp `seg.LastRecordTime`'a yaz. Ama bu sadece **aktif** segment için taranıyor; sealed segmentler taranmıyor.
2. **Pratik ve yeterli:** dosyanın `mtime`'ı. `NewSegment` içinde `logFile.Stat()` zaten çağrılıyor (`bytesWritten` için) — aynı `info`'dan `info.ModTime()` al:
   ```go
   createdAt := time.Now()
   if info != nil && info.Size() > 0 {
       // Var olan bir segment yeniden açılıyor: retention'ın restart'ta
       // sıfırlanmaması için yaşını dosyanın son yazma zamanından türet.
       createdAt = info.ModTime()
   }
   ```

**Seçim: 2 numarayı uygula.** Basit, ek I/O yok, sealed segmentler için de çalışıyor. `CreatedAt` alan adını `LastModified` olarak değiştirmek daha dürüst olur; ama bu public alan, kullanan yerleri (`log.go`, testler) güncelle.

**Kabul kriteri:** Yeni test `TestRetentionSurvivesRestart` (`internal/storage/log_test.go`):
1. `RetentionMs: 100`, `SegmentBytes` küçük olacak şekilde log aç
2. 3 segment oluşacak kadar yaz, `Close()`
3. Segment dosyalarının mtime'ını 1 saat geriye al (`os.Chtimes`)
4. Logu yeniden aç, retention tick'ini bekle (veya `runRetention`'ı doğrudan çağır)
5. `NumSegments() == 1` (sadece aktif kalmış olmalı)

Bu test mevcut kodda başarısız olacak.

---

## T-13 — Hard crash sonrası sealed segment index'leri sessizce çöp

**Dosyalar:** `internal/storage/log.go` (`rotateLocked`), `internal/storage/index.go` (`NewIndex`)

**Sorun — iki parçalı:**

*(a) Rotasyonda index kapatılmıyor.* `rotateLocked` sadece `l.active.Flush()` çağırıyor; `Close()` çağırmıyor. `Index.Close()` dosyayı mantıksal boyuta truncate eden yer. Yani sealed olan segmentin `.index` dosyası diskte **10 MB ön-tahsisli** halde kalıyor.

*(b) Açılışta index doğrulanmıyor.* `NewIndex` mantıksal boyutu dosya boyutundan çıkarıyor:
```go
logical := info.Size()          // 10485760
logical -= logical % entrySize
```
`SIGKILL` sonrası yeniden açılışta bu 10 MB → **1.310.720 adet sıfır entry**. `Lookup` bu sıfırlar üzerinde binary search yapıyor:
- Hedef > 0 için predicate `entry >= target` hiçbir yerde true olmuyor → `idx = n`
- `idx > 0` olduğu için son entry'nin position'ı dönüyor → **0**
- `scanFrom(0, target)` → segmentin tamamı lineer taranıyor

Yani index sessizce işlevsiz kalıyor, sistem "çalışıyor" ama her okuma O(segment). Teşhis edilmesi çok zor bir performans regresyonu. Ayrıca `Entries()` tamamen yalan söylüyor, bu da `IsFull()` mantığını bozuyor.

Not: aktif segment için `rebuildSegment` index'i silip yeniden yaratıyor, o yüzden orada sorun yok. Sorun **yalnızca sealed segmentlerde**.

**Yapılacak:**

1. **`rotateLocked`'da sealed segmentin index'ini truncate et.** Segmentin tamamını kapatamayız (okumalar devam ediyor) ama index dosyasının slack'ini serbest bırakabiliriz. `Index`'e yeni bir metot ekle:
   ```go
   // Seal unmaps the preallocated slack and shrinks the backing file to the
   // logical size in use, then re-maps the (now exactly sized) region
   // read-only. After Seal the index accepts no further Append.
   func (i *Index) Seal() error
   ```
   Alternatif, daha basit ve yeterli: unmap → `file.Truncate(size)` → tekrar `mmapIndex(file, int(size))`. `maxSize = size` olacağı için `Append` doğal olarak `ErrIndexFull` dönecek — sealed segment için istenen davranış.

2. **`NewIndex`'te sağlık kontrolü.** Truncate edilmemiş bir index dosyasıyla karşılaşırsak sessizce kabul etmek yerine gerçek entry sayısını tespit et: sondan geriye doğru sıfır olmayan ilk entry'yi bul.
   ```go
   // Bir hard crash sonrası dosya ön-tahsisli boyutunda kalmış olabilir;
   // kuyruktaki sıfır dolgu gerçek entry değildir. Sondan geriye tarayarak
   // mantıksal boyutu düzelt. (relativeOffset==0 && position==0 yalnızca
   // ilk entry için geçerli olabilir.)
   ```
   Sondan başlayıp `relativeOffset == 0 && position == 0` olan entry'leri at. Bu, `entry[0]` hariç güvenli: bir segmentin ilk entry'si `(0, 0)`'dır ve o meşrudur, dolayısıyla taramayı `idx > 0` ile sınırla.

**Kabul kriteri:** Yeni test `TestIndexRecoveryAfterHardCrash` (`internal/storage/index_test.go`):
1. Index yarat, 5 entry ekle
2. `Close()` **çağırmadan** dosyayı ön-tahsisli boyutta bırakacak şekilde simüle et (unmap etmeden `os.OpenFile` + `Truncate(maxBytes)` ile hazırla, ya da `Seal`/`Close` atlanarak)
3. Yeniden `NewIndex` ile aç
4. `Entries() == 5` (şu an 1.310.720 dönüyor)
5. `Lookup(4)` doğru position'ı buluyor

Ve `TestRotationSealsIndex`: rotasyondan sonra sealed segmentin `.index` dosya boyutu `entries * 8` olacak, 10 MB olmayacak.

---

## T-14 — 4 GiB üstü segment'te sessiz taşma

**Dosya:** `internal/storage/segment.go` (`Append`), `internal/storage/index.go`

**Sorun:** Index entry'si `(relativeOffset uint32, position uint32)`. `Append` içinde:
```go
position := uint32(s.bytesWritten)   // ← sınır kontrolü yok
```
`segment_bytes` 4 GiB üstüne yapılandırılırsa position sessizce sarıyor ve index kalıcı olarak yanlış pozisyonlar gösteriyor. Varsayılan 128 MiB olduğu için pratikte tetiklenmiyor, ama config bunu engellemiyor.

**Yapılacak — iki katmanlı savunma:**

1. **Config doğrulaması** (`internal/config/config.go`, validate fonksiyonu): `log.segment_bytes > math.MaxUint32` ise açılışta hata dön. Mesaj net olsun: `segment_bytes 4 GiB'ı (4294967295) aşamaz: index pozisyonları uint32`.
2. **`Segment.Append`'te guard:**
   ```go
   if s.bytesWritten > math.MaxUint32 {
       return 0, fmt.Errorf("segment: log position %d exceeds uint32 index range", s.bytesWritten)
   }
   ```
   Bu asla tetiklenmemeli (config kontrolü var) ama sessiz bozulma yerine gürültülü hata veriyor.

Aynı mantığı `relativeOffset` için de uygula: segment başına 4,29 milyar kayıt pratikte imkânsız ama guard maliyetsiz.

**Kabul kriteri:** `TestSegmentBytesTooLargeRejected` — `Config{SegmentBytes: 5 << 30}` ile config validate hata dönüyor. Guard için unit test yazmak pahalı (4 GB yazmak gerekir); guard'ın varlığı kod incelemesiyle yeterli.

---

## T-15 — `pkg/client.Producer` eşzamanlı kullanımda bozuluyor

**Dosyalar:** `pkg/client/producer.go`, `pkg/client/consumer.go`, `pkg/client/group_consumer.go`

**Sorun — bu serinin en ciddi bug'ı, çünkü kullanıcıya yanlış veri döndürüyor.**

`SendBatch` (`producer.go:122`):
```go
conn, err := p.getConn()              // ← kilit BURADA alınıp BURADA bırakılıyor
if _, err := frame.Write(conn); ...   // ← kilitsiz
respFrame, err := protocol.ReadResponseFrame(conn)  // ← kilitsiz
```

İki goroutine eşzamanlı `Send` çağırırsa:
- Yazımlar aynı TCP bağlantısında iç içe geçiyor → broker bozuk frame okuyor,
- veya frame'ler sağlam gitse bile **goroutine A, goroutine B'nin yanıtını okuyor.**

Ve kritik olan: `corrID` üretiliyor (`producer.go:170`) ama **yanıtın `CorrelationID`'si hiç kontrol edilmiyor.** Yani hata bir exception olarak değil, **sessizce yanlış offset dönmek** olarak ortaya çıkıyor. Kullanıcı mesaj 5'in offset'i diye mesaj 12'nin offset'ini alıyor.

Aynı desen `consumer.go:136` ve `group_consumer.go:177`'de de var.

Kafka istemcilerinden goroutine-safe olması beklenir (`sarama`, `kafka-go` öyledir). Bir kullanıcı `for i := range items { go producer.Send(...) }` yazacak ve sessizce bozuk veri alacak.

**Yapılacak — iki aşama:**

**Aşama A (bu görev, zorunlu): round-trip'i serileştir.**

`Producer`, `Consumer` ve `GroupConsumer`'a bir `reqMu sync.Mutex` ekle (mevcut `mu`'dan **ayrı** — `mu` bağlantı durumunu koruyor, `reqMu` wire üzerindeki round-trip'i serileştirecek; ikisini birleştirmek `getConn` sırasında gereksiz genişlikte kilit tutmaya yol açar).

```go
p.reqMu.Lock()
defer p.reqMu.Unlock()

conn, err := p.getConn()
if err != nil { return nil, err }
if _, err := frame.Write(conn); err != nil { ... }
respFrame, err := protocol.ReadResponseFrame(conn)
```

**Aşama B (bu görev, zorunlu): correlation ID doğrula.**

Yanıt okunduktan sonra:
```go
if respFrame.CorrelationID != corrID {
    p.closeConn()
    return nil, fmt.Errorf("producer: correlation id mismatch: got %d, want %d",
        respFrame.CorrelationID, corrID)
}
```
Uyumsuzlukta **bağlantıyı kapat** — akış senkronizasyonunu kaybettik, o bağlantı artık güvenilmez.

Bu kontrolü üç istemcinin hepsine ekle.

**Dokümantasyon:** Her üç tipin doc comment'ine ekle: `// Producer is safe for concurrent use by multiple goroutines; requests are serialised over a single connection.`

**Kapsam dışı (yapma):** Gerçek pipelining (correlation ID → bekleyen istek map'i, ayrı okuyucu goroutine). Doğru çözüm o ama bu görevin kapsamı değil. `docs/FINDINGS.md`'ye gelecek iş olarak not düş.

**Kabul kriteri:** `TestProducerConcurrentSend` (T-19) geçiyor — 8 goroutine × 200 mesaj, dönen offset kümesi tam olarak `[0,1600)`, tekrar/boşluk yok. Öncesinde bu testin başarısız çıktısı raporlanmış olacak.

---

# W2 — Dayanıklılık ve Hata Görünürlüğü

---

## T-20 — İstemci bağlantılarında deadline yok

**Dosyalar:** `internal/server/server.go`, `internal/config/config.go` (kullanım)

**Sorun:** `SetDeadline` tüm kod tabanında **tek bir yerde** var: `internal/broker/broker_replication.go:384` (replikasyon fetch'i). İstemci bağlantılarında hiç yok.

`handleConn` (`server.go:126`):
```go
for {
    req, err := protocol.ReadRequestFrame(conn)   // ← süresiz blokluyor
```

Sonuçları:
- **Slowloris:** bağlanıp hiç byte göndermeyen bir istemci, bir goroutine'i sonsuza kadar tutuyor.
- `max_connections: 1024` × süresiz bekleme = 1024 tembel bağlantı ile broker doldurulabilir.
- `MaxFrameSize` 100 MB. Bir istemci `size = 100MB` yazıp sonra yavaş byte gönderirse, `Bytes(lr)` içinde 100 MB'lık bir slice tahsis edilip bekleniyor. 1024 bağlantı × 100 MB = OOM.
- `broker.request_timeout_ms` config'i **hiçbir yerde istemci bağlantısına uygulanmıyor** (grep ile doğrulandı: yalnızca `broker_replication.go:171`'de kullanılıyor).

ACL yokluğu README'de "bilinçli yapılmadı" diye yazılı, sorun değil. Ama deadline yokluğu bir tasarım tercihi değil, eksik.

**Yapılacak:**

1. `NewServer` imzasına timeout parametreleri ekle (veya bir `ServerConfig` struct'ı — tercih edilen, çünkü imza büyümeye devam edecek):
   ```go
   type ServerConfig struct {
       Addr           string
       MaxConnections int
       // IdleTimeout, bir bağlantıda yeni bir istek beklenirken izin verilen
       // azami süre. Süre dolarsa bağlantı kapatılır.
       IdleTimeout    time.Duration
       // WriteTimeout, bir yanıtın yazılması için izin verilen azami süre.
       WriteTimeout   time.Duration
   }
   ```
2. `handleConn` döngüsünde her iterasyonda:
   ```go
   if s.cfg.IdleTimeout > 0 {
       _ = conn.SetReadDeadline(time.Now().Add(s.cfg.IdleTimeout))
   }
   req, err := protocol.ReadRequestFrame(conn)
   ...
   if s.cfg.WriteTimeout > 0 {
       _ = conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout))
   }
   ```
3. **Dikkat — long-poll ve acks=all ile çakışma:** `handleFetch` `MaxWaitMs` kadar bekliyor, `handleProduce` acks=all'da `TimeoutMs` kadar bekliyor. Bunlar okuma deadline'ından **sonra** gerçekleşiyor (istek zaten okundu), o yüzden `ReadDeadline` ile çakışmıyor. Ama `WriteTimeout` yanıt yazılırken geçerli olacak ve bu güvenli. Yine de: `IdleTimeout`, istemcinin `MaxWaitMs`'inden büyük olmalı yoksa long-poll yapan consumer'ın bağlantısı sürekli kopar. Bu yüzden:
   - `IdleTimeout` varsayılanı `request_timeout_ms`'ten türetilecek ama **en az** `group.session_timeout_ms` kadar olacak. Config validate'te bu ilişkiyi kontrol et ve ihlalde uyarı logla.
4. `internal/broker/broker.go` `New()` içinde `server.NewServer` çağrısını yeni config ile güncelle; `IdleTimeout` = `cfg.Broker.RequestTimeoutMs`, `WriteTimeout` = 10s.
5. **`MaxFrameSize`'ı yapılandırılabilir yap ve varsayılanı düşür.** 100 MB savunulamaz. `log.max_message_bytes` 1 MiB. `MaxFrameSize` varsayılanı 16 MiB olsun ve `config.Broker.MaxRequestBytes` ile ayarlanabilsin. `ReadRequestFrame`'e parametre olarak geçir (veya `protocol.FrameLimits` struct'ı).

**Kabul kriteri:** Yeni test `TestIdleConnectionClosed` (`internal/server/server_test.go`): `IdleTimeout: 200ms` ile server aç, bağlan, hiçbir şey gönderme, 500ms sonra `conn.Read` EOF dönüyor. Ve `TestOversizedFrameRejected`: `MaxRequestBytes`'i aşan `size` alanı gönderildiğinde bağlantı kapanıyor ve **büyük tahsis yapılmıyor** (bunu `testing.AllocsPerRun` veya `runtime.MemStats` ile ölçmek zor; frame reddinin `size` okunduktan hemen sonra, payload okunmadan olduğunu kod incelemesiyle doğrula).

---

## T-21 — Handler hataları tek bir `ErrUnknown`'a çöküyor

**Dosya:** `internal/server/handler.go` (`Mux.Dispatch`)

**Sorun:**
```go
out, err := h(req)
if err != nil {
    resp.ErrorCode = ErrUnknown     // ← her hata "1"
    return resp
}
```
Ve panic recover'ında da aynısı. `handleProduce`'ta `produceResp.Encode` hatası, decode hatası, storage hatası — hepsi istemciye `1` olarak gidiyor. İstemci tarafında teşhis imkânsız; log da yok (broker hiçbir şey loglamıyor).

**Yapılacak:**

1. **Sunucu tarafında logla.** Broker'ın hiç logger'ı yok. `log/slog` (stdlib, yeni bağımlılık değil) ile minimal bir yapı kur:
   - `Mux`'a `logger *slog.Logger` alanı
   - Handler hatası ve panic'te: `logger.Error("handler failed", "apiKey", req.ApiKey, "correlationID", req.CorrelationID, "err", err)`
   - Panic'te `debug.Stack()` da logla
   - `cmd/broker/main.go`'da logger'ı kur (seviye config'den: `broker.log_level`)
2. **Tiplenmiş hata kodları.** `internal/server`'da bir `CodedError` arayüzü tanımla:
   ```go
   // CodedError, bir handler hatasının istemciye hangi protokol hata koduyla
   // bildirileceğini belirtir.
   type CodedError interface {
       error
       ErrorCode() int16
   }
   ```
   `Dispatch` bunu `errors.As` ile kontrol edip kodu kullanacak; aksi halde `ErrUnknown`. Handler'lar kendi hatalarını bu tipe sarabilecek.
3. **`ErrorMessage` alanı EKLEMEYİN.** Bu bir wire format değişikliğidir (bkz. §0.4) ve bu görevin kapsamı dışında. Hata mesajı sunucu logunda kalacak.

**Kabul kriteri:** `TestDispatchLogsHandlerError` — hata dönen bir handler kaydedildiğinde logger'a bir Error kaydı gidiyor (test logger'ı ile yakala). `TestDispatchCodedError` — `CodedError` dönen handler'ın kodu yanıta yansıyor.

---

## T-22 — `acks=all` çok partition'da timeout'ları topluyor

**Dosya:** `internal/broker/broker.go` (`handleProduce`, ~satır 385)

**Sorun:** `waitForAcksAll` partition döngüsünün **içinde** çağrılıyor:
```go
for j, pReq := range tReq.Partitions {
    ...
    if produceReq.Acks == -1 {
        if waitErr := b.waitForAcksAll(tReq.Name, pReq.PartitionID, leaderLEO, produceReq.TimeoutMs); ...
```
`TimeoutMs` her partition için **baştan** başlıyor. 3 partition × 30s timeout = istemci 90 saniye bekliyor, oysa `timeout_ms=30000` istedi. Kafka tek ortak deadline kullanır.

**Yapılacak:**

1. Handler başında bir deadline hesapla:
   ```go
   deadline := time.Now().Add(time.Duration(produceReq.TimeoutMs) * time.Millisecond)
   ```
2. `waitForAcksAll` imzasını `timeoutMs int32` yerine `deadline time.Time` alacak şekilde değiştir. İçindeki bekleme bu deadline'a kadar olacak.
3. Deadline geçmişse hiç beklemeden `errProduceTimeout` dön.

**Bonus iyileştirme (bu görev kapsamında, çünkü aynı döngü):** Şu an partition'lar **sırayla** bekliyor. Doğru davranış paralel beklemek: tüm partition'lara append yap, sonra hepsinin HW ilerlemesini eşzamanlı bekle. Bunu bir `errgroup` benzeri desenle (stdlib `sync.WaitGroup` + hata toplama, yeni bağımlılık yok) uygula. 3 partition'lık bir batch 30s'de değil, en yavaş partition kadar sürede tamamlanacak.

**Kabul kriteri:** `TestAcksAllSharedDeadline` (`internal/broker/broker_handlers_test.go`): `min_insync_replicas=2` ama tek broker (dolayısıyla HW asla ilerlemeyecek) senaryosunda, 3 partition'a `TimeoutMs=300` ile produce → **toplam süre 1 saniyenin altında** olacak (şu an ~900ms+). Ölçümü `time.Since` ile yap ve assert et.

---

## T-23 — Long-poll yalnızca ilk partition'a bakıyor

**Dosya:** `internal/broker/broker.go` (`handleFetch`, ~satır 418)

**Sorun:**
```go
if fetchReq.MaxWaitMs > 0 && len(fetchReq.Topics) > 0 && len(fetchReq.Topics[0].Partitions) > 0 {
    topicName := fetchReq.Topics[0].Name
    partID := fetchReq.Topics[0].Partitions[0].PartitionID
```
Çok partition'lı bir Fetch isteğinde yalnızca `Topics[0].Partitions[0]` için bekliyor. Diğer partition'larda veri gelmiş olsa bile ilk partition boşsa `MaxWaitMs` kadar boşuna bekliyor; tersi durumda da erken dönüyor. Kod yorumu bunu kabul ediyor ama bu bir eksik, tasarım değil.

**Yapılacak:** İstenen tüm topic-partition'lar için listener kaydet, **herhangi birinde** veri gelirse veya deadline dolarsa dön.

```go
// İstenen her partition için bir listener kaydet; ilk sinyal veya deadline
// hangisi önce gelirse döngüden çıkılır.
```

Uygulama: tek bir `notify chan struct{}` (buffered, cap 1) yaratıp her partition'ın listener'ını bu kanala fan-in eden goroutine'ler yerine — daha basiti: `registerListener` zaten kanal dönüyor, hepsini bir `reflect.Select` yerine tek bir paylaşılan kanala bağla. En temiz yol: `registerListener`'ı, çağıranın verdiği bir kanalı kaydedecek şekilde değiştir:

```go
func (b *Broker) registerListenerOn(topic string, partitionID int32, ch chan struct{})
```

Böylece N partition için tek kanal kaydedilir, `notifyAppended` non-blocking send yapar (kanal cap 1, dolu ise atla), `select` tek kanalı dinler. **`notifyAppended` artık kanalı `close` etmeyecek** — non-blocking send yapacak, çünkü aynı kanal birden fazla partition'a kayıtlı. Bu, T-11'deki `unregisterListener` mantığıyla birlikte tasarlanmalı.

**Sıralama:** T-23, **T-11'den sonra** yapılacak. Aynı fonksiyonlara dokunuyorlar ve T-23 T-11'in kurduğu unregister mekanizmasını kullanacak.

**Kabul kriteri:** `TestFetchLongPollAnyPartition`: 3 partition'lı topic'e `MaxWaitMs=2000` ile 3 partition'lık Fetch at, sonra **partition 2**'ye produce et → Fetch 100ms içinde dönecek (şu an 2000ms bekliyor).

---

# W3 — Performans

**Ön koşul:** W1 ve W2 tamamlanmış olacak. Bozuk kodu optimize etmek anlamsız.

**Bu dalganın kuralı:** Her görevden önce ve sonra `go run ./benchmark` çalıştırılıp sayılar raporlanacak. Ölçülmeyen optimizasyon kabul edilmez.

---

## T-30 — Sunucu bağlantılarında tamponlama yok (en yüksek getiri)

**Dosya:** `internal/server/server.go` (`handleConn`)

**Sorun:** Tüm kod tabanında `bufio` **hiç kullanılmıyor** (grep ile doğrulandı: `internal/server`, `pkg/client`, `internal/broker`'da sıfır eşleşme).

`ReadRequestFrame(conn)` doğrudan `net.Conn`'dan okuyor ve içinde:
```
Int32(r)   → binary.Read → 4 byte read syscall
Int16(lr)  → 2 byte syscall
Int16(lr)  → 2 byte syscall
Int32(lr)  → 4 byte syscall
String(lr) → 2 byte + N byte syscall
Bytes(lr)  → 4 byte + N byte syscall
```
**İstek başına 8+ read syscall.** Yanıt yazımında da benzer durum (`ResponseFrame.Write` içinde `bytes.Buffer` kullanılıyor, orası daha iyi — kontrol et ve tek `Write`'a indiğinden emin ol).

Benchmark'taki 7.251 msg/s (mesaj başına 138 µs) büyük ölçüde bunun sonucu.

**Yapılacak:**

```go
func (s *Server) handleConn(conn net.Conn) {
    ...
    // Protokol çözücü alanları tek tek okuduğu için tamponsuz bir bağlantı
    // istek başına 8+ read syscall'ı demek. Tamponlama bunu 1'e indiriyor.
    br := bufio.NewReaderSize(conn, 64*1024)
    bw := bufio.NewWriterSize(conn, 64*1024)

    for {
        req, err := protocol.ReadRequestFrame(br)
        if err != nil { return }

        resp := s.mux.Dispatch(req)
        if _, err := resp.Write(bw); err != nil { return }
        if err := bw.Flush(); err != nil { return }   // ← her yanıttan sonra ZORUNLU
    }
}
```

**Kritik uyarı agent'a:** `bw.Flush()` her yanıttan sonra **mutlaka** çağrılacak. Unutulursa yanıt tamponda kalır, istemci sonsuza kadar bekler ve tüm entegrasyon testleri deadlock'a girer. Bu, bu görevde yapılabilecek en olası hata.

`SetReadDeadline` (T-20) `conn` üzerinde çağrılmaya devam edecek — `bufio.Reader` sarmalayıcı, deadline alttaki `conn`'da geçerli. Ama dikkat: `bufio.Reader` içinde tamponlanmış veri varsa deadline'ın süresi dolsa bile o veri okunabilir; bu istenen davranış.

**Kabul kriteri:** Tüm testler geçiyor (özellikle `test/integration`). Benchmark öncesi/sonrası msg/s raporlanacak. Beklenen: **2-4x artış**.

---

## T-31 — İstemci bağlantılarında tamponlama yok

**Dosyalar:** `pkg/client/producer.go`, `consumer.go`, `group_consumer.go`

**Sorun:** T-30'un istemci tarafı. `frame.Write(conn)` ve `ReadResponseFrame(conn)` tamponsuz.

**Yapılacak:** Her istemcide bağlantıyla birlikte `*bufio.Reader` ve `*bufio.Writer` tut. Bunlar bağlantı durumuna bağlı olduğu için `getConn`/`closeConn` ile birlikte yönetilecek:

```go
type Producer struct {
    ...
    mu     sync.Mutex
    conn   net.Conn
    br     *bufio.Reader   // conn ile birlikte yaratılır/sıfırlanır
    bw     *bufio.Writer
}
```

`closeConn` bunları `nil`'a çekecek. `getConn` yeni bağlantıda yeniden yaratacak — **eski Reader'ı yeni bağlantıda tekrar kullanma**, tamponunda eski bağlantıdan kalan byte'lar olabilir.

`bw.Flush()` her istek yazımından sonra zorunlu (T-30'daki aynı uyarı).

**Sıralama:** T-15'ten **sonra** yapılacak (aynı fonksiyonlara dokunuyor, T-15'in `reqMu`'su bu tamponların eşzamanlı erişimini de koruyacak).

**Kabul kriteri:** Testler geçiyor, benchmark öncesi/sonrası raporlanıyor.

---

## T-32 — Okuma yolu ve recovery tamponsuz

**Dosya:** `internal/storage/segment.go`

**Sorun — iki yer:**

*(a) `Segment.ReadFrom` ve `scanFrom`* `DecodeRecord(reader)`'ı doğrudan `*os.File`'dan çağırıyor. `DecodeRecord` önce 4 byte `length` okuyor (bir syscall), sonra `io.ReadFull` ile payload (bir syscall daha). Kayıt başına **2 syscall**. 1000 kayıtlık bir Fetch = 2000 syscall.

*(b) `readHandle()` her okumada `os.Open` yapıyor* — okuma başına yeni file descriptor açma/kapama. Yüksek fetch hızında hem syscall hem fd basıncı.

*(c) `rebuildSegment` (`log.go`)* aynı sorundan muzdarip: `DecodeRecord(readFile)` tamponsuz. 128 MB'lık bir segmentin crash recovery'si kayıt başına 2 syscall ile yapılıyor — açılış süresi kabul edilemez seviyede.

**Yapılacak:**

1. `ReadFrom` ve `scanFrom` içinde okuma handle'ını sar:
   ```go
   // DecodeRecord kayıt başına iki okuma yapıyor (uzunluk + payload); tamponsuz
   // bir dosya handle'ında bu kayıt başına iki syscall demek.
   buf := bufio.NewReaderSize(reader, 64*1024)
   ```
   `Seek` yapıldıktan **sonra** sarmala, yoksa tampon yanlış konumdan doldurulur.
2. `rebuildSegment` içinde aynısı — recovery'nin en çok fayda göreceği yer burası.
3. **`readHandle` için fd yeniden kullanımı:** Her `Read` için `os.Open` yapmak yerine, `Segment`'te tek bir salt-okunur handle tut ve `ReadAt` kullan. `ReadAt` konum-bağımsızdır ve **eşzamanlı çağrılar için güvenlidir** (paylaşılan dosya imlecini kullanmaz), dolayısıyla kilit gerektirmez:
   ```go
   readFile *os.File   // NewSegment'te açılır, Close'da kapanır
   ```
   ve okuma yolunda `io.NewSectionReader(s.readFile, position, size)` üzerinden `bufio` ile oku. Bu, T-10'un getirdiği exclusive kilit maliyetini de azaltır çünkü okuma artık writer durumuna dokunmaz.

   **Dikkat:** `FlushWriter` yine gerekli — tamponlanmış yazımlar diske inmeden `ReadAt` onları görmez.

4. `Remove()` fonksiyonu `readFile`'ı da kapatacak. Windows'ta açık handle dosya silmeyi engeller — bu repoda Windows desteği var (`index_windows.go`), dolayısıyla `Remove` sırasında **tüm** handle'ların kapandığından emin ol.

**Kabul kriteri:**
- Tüm storage testleri geçiyor, `-race` temiz.
- Yeni test `TestRecoverySpeed`: 50.000 kayıtlı bir segment yaz, kapat, yeniden aç, `NewLog` süresini ölç. Bir üst sınır assert etmek kırılgan olur; yerine **öncesi/sonrası ölçümü raporla**. Beklenen: 10x+ iyileşme.
- `TestSegmentRemoveClosesAllHandles`: `Remove()` sonrası dosyalar diskte yok (Windows CI'da da geçmeli — CI'a `runs-on: windows-latest` matrisi eklemeyi düşün, ayrı görev olarak `docs/FINDINGS.md`'ye not düş).

---

## T-33 — `Record.Encode`/`DecodeRecord` tahsis-yoğun

**Dosya:** `internal/storage/record.go`

**Sorun:** `Encode` kayıt başına:
- 1 adet `bytes.Buffer` tahsisi (`crcBuf`)
- 8 adet `binary.Write` çağrısı — her biri `interface{}` parametresi alıyor, reflection yoluna giriyor ve **her çağrıda tahsis yapıyor**
- Sonra `crcBuf.Bytes()` üzerinden bir kopya daha

`DecodeRecord` benzer şekilde `bytes.NewReader` + 6 adet `binary.Read` (aynı reflection maliyeti).

Bu, produce ve fetch yolunun **en sıcak** fonksiyonu. Kayıt başına ~10 tahsis, GC baskısı ve gereksiz CPU.

**Yapılacak:**

1. `Encode`: `EncodedSize()` zaten tam boyutu biliyor. Tek bir `[]byte` tahsis et ve `binary.BigEndian.PutUint32/PutUint64` ile doldur:
   ```go
   buf := make([]byte, total)
   binary.BigEndian.PutUint32(buf[0:4], uint32(length))
   binary.BigEndian.PutUint64(buf[4:12], uint64(r.Offset))
   ...
   ```
   CRC, `buf`'ın ilgili dilimi üzerinden hesaplanacak — ayrı bir tampona ihtiyaç yok. CRC alanı (offset 20-24) CRC hesaplandıktan sonra doldurulacak. Sonra tek `w.Write(buf)`.

   **CRC kapsamı değişmeyecek:** `attributes`'tan `value` sonuna kadar. Bu wire format'ın parçası; bir byte kayarsa diskteki tüm veri okunamaz hale gelir.

2. `DecodeRecord`: `payload` zaten tek parça okunuyor. `bytes.Reader` + `binary.Read` yerine doğrudan slice'tan oku:
   ```go
   rec.Offset = int64(binary.BigEndian.Uint64(payload[0:8]))
   rec.Timestamp = int64(binary.BigEndian.Uint64(payload[8:16]))
   checksum := binary.BigEndian.Uint32(payload[16:20])
   ```
   Her erişimden önce `len(payload)` kontrolü yap — **bounds check atlamak panic demek ve bu fonksiyona ağdan gelen veri besleniyor.** Mevcut fuzz testi (`codec_fuzz_test.go`) bunu yakalayacak; fuzz'ı `record.go` için de genişlet.

3. Opsiyonel: `Encode(w io.Writer)` yanına `AppendTo(dst []byte) []byte` ekle. Batch encode'da tek tampona yazmayı sağlar, `pkg/client`'ta işe yarar.

**Kabul kriteri:**
- Yeni benchmark `BenchmarkRecordEncode` / `BenchmarkRecordDecode` (`internal/storage/record_test.go`). Öncesi/sonrası `-benchmem` çıktısı raporlanacak. Beklenen: allocs/op 10+ → 1-2.
- **Golden test zorunlu:** `TestRecordEncodeGoldenBytes` — bilinen bir kayıt için beklenen byte dizisi hard-code edilecek. Bu test optimizasyonun wire format'ı değiştirmediğini kanıtlayan tek güvence. **Önce yaz** (mevcut kodun çıktısıyla), sonra optimize et.
- Mevcut fuzz testi `go test ./internal/storage -fuzz=FuzzDecodeRecord -fuzztime=60s` ile 60 saniye koşturulacak, crash yok.

---

## T-34 — `Segment.Append`'te gereksiz syscall

**Dosya:** `internal/storage/segment.go` (`Append`)

**Sorun:**
```go
if _, err := s.logFile.Seek(0, io.SeekEnd); err != nil {
```
**Her kayıt için bir `Seek` syscall'ı.** Yorumda gerekçe açıklanıyor (O_APPEND yok çünkü truncate gerekiyor) ama yazıcı tamponlu olduğu için bu seek'in etkisi yok: gerçek yazım tampon dolduğunda oluyor ve o an imleç zaten doğru yerde.

**Yapılacak:** `Seek`'i `Append`'ten kaldır. İmleç konumunu doğru tutma sorumluluğu şu iki yere taşınacak:
- `NewSegment`: açılıştan sonra `logFile.Seek(bytesWritten, io.SeekStart)` (bir kez)
- `rebuildSegment`: zaten truncate sonrası seek yapıyor (`log.go` sonu) — bu kalacak

Yazımlar sıralı olduğu için imleç doğal olarak ilerleyecek.

**Risk:** Bu değişiklik yanlış yapılırsa **veri bozulması** üretir (yazımlar dosyanın başına gider). Bu yüzden:
- `TestSegmentAppendAfterReopen`: segment yaz → kapat → yeniden aç → tekrar yaz → tüm kayıtları oku, hem eskiler hem yeniler doğru sırada olacak.
- `TestSegmentAppendAfterRebuild`: bozuk kuyrukla recovery → sonra append → dosya boyutu ve kayıt sırası doğru.

Bu iki test **düzeltmeden önce** yazılıp mevcut kodda geçtiği gösterilecek (regresyon koruması olarak), sonra düzeltme yapılacak.

**Kabul kriteri:** İki test de geçiyor. `strace -c -e trace=lseek` (Linux) ile 1000 append'te lseek sayısının ~1000'den ~1'e düştüğü gösterilecek — ya da bu ortamda mümkün değilse kod incelemesiyle doğrulanacak.

---

## T-35 — `index_max_bytes` gereğinin 40 katı

**Dosyalar:** `config/broker.yaml`, `config/broker-single.yaml`, `internal/config/config.go`

**Sorun:** `index_max_bytes: 10485760` (10 MiB). Gerçek ihtiyaç:
```
segment_bytes / index_interval_bytes × entrySize
= 134217728 / 4096 × 8 = 262.144 byte ≈ 256 KiB
```
Yani **40 kat fazla**. Her segment 10 MB mmap'liyor. `offsets_topic_partitions: 50` + kullanıcı topic'leri düşünüldüğünde yüzlerce MB gereksiz sanal adres alanı ve (hard crash sonrası, T-13) yüzlerce MB gereksiz disk.

**Yapılacak:**

1. Varsayılanı hesaplanmış değere çek: `index_max_bytes: 524288` (512 KiB — 2x güvenlik payı).
2. **Daha iyisi: otomatik türet.** `Config.withDefaults()` içinde `IndexMaxBytes == 0` ise:
   ```go
   // Index kapasitesini segment boyutu ve indeksleme aralığından türet;
   // 2x pay bırak. Böylece segment_bytes değiştiğinde index elle
   // ayarlanmak zorunda kalmaz.
   needed := (out.SegmentBytes/out.IndexIntervalBytes + 1) * entrySize * 2
   ```
   Alt sınır 64 KiB, üst sınır 10 MiB olacak şekilde clamp'le.
3. Config validate'te tutarlılık uyarısı: `IndexMaxBytes` gerekenden azsa, segment `SegmentBytes`'a ulaşmadan index dolacağı için erken rotasyon olur. Bu bir hata değil ama loglanmalı.

**Kabul kriteri:** `TestIndexMaxBytesDerived` — `Config{SegmentBytes: 128<<20, IndexIntervalBytes: 4096}` ile `withDefaults()` sonrası `IndexMaxBytes` 512 KiB civarı, ve segment `SegmentBytes`'a ulaşana kadar index dolmuyor (küçük ölçekli bir senaryo ile test et: `SegmentBytes: 1<<20`, `IndexIntervalBytes: 64`).

---

# W4 — Gerçek Batching ve Dürüst Benchmark

---

## T-40 — `LingerMs` batching yapmıyor, sadece uyuyor

**Dosya:** `pkg/client/producer.go:128`

**Sorun — bu, dokümantasyon dürüstlüğü açısından en kötü bulgu:**

```go
if p.config.LingerMs > 0 {
    time.Sleep(time.Duration(p.config.LingerMs) * time.Millisecond)
}
```

Bu batching değil. Mesaj başına 5 ms uyumak. Ve `BatchSize` alanı **hiç kullanılmıyor** (grep: yalnızca tanım ve benchmark'ta atama).

Sonuç: `benchmark/main.go:110` "Scenario 5: Batching Impact (LingerMs=5 vs LingerMs=0)" **batching'i ölçmüyor** — uyumanın etkisini ölçüyor ve tabii ki batching'i yavaş gösteriyor. `docs/BENCHMARK.md` bu ölçümü rapor ediyor.

**Yapılacak — gerçek accumulator:**

```go
// batcher, aynı topic-partition'a giden kayıtları LingerMs kadar veya
// BatchSize'a ulaşana kadar biriktirip tek Produce isteğiyle gönderir.
type batcher struct {
    mu       sync.Mutex
    pending  map[topicPartition]*pendingBatch
    ...
}

type pendingBatch struct {
    records  []*storage.Record
    bytes    int
    waiters  []chan batchResult   // her Send çağrısı kendi sonucunu bekler
    timer    *time.Timer
}
```

Davranış:
- `Send` çağrısı kaydı ilgili `topicPartition` batch'ine ekler, bir `chan batchResult` kaydeder ve **bloklar**.
- Batch iki koşuldan biriyle flush edilir: `bytes >= BatchSize` **veya** ilk kayıttan `LingerMs` sonra.
- Flush: tek Produce isteği gönderilir, dönen `baseOffset`'ten her kaydın offset'i türetilir (`baseOffset + i`) ve her waiter'a kendi offset'i gönderilir.
- Hata durumunda hata **tüm** waiter'lara dağıtılır.
- `Close()` bekleyen batch'leri flush edip tüm waiter'ları serbest bırakır. **Sızdırılan goroutine veya bloklanmış waiter kalmayacak.**
- `LingerMs == 0` ise mevcut senkron yol korunacak (batching devre dışı) — bu varsayılan davranışı bozmamak için önemli.

**Zorluk uyarısı:** Bu, plandaki en karmaşık görev. Klasik hata kaynakları:
- Timer yarışı: batch flush edilirken aynı anda yeni kayıt eklenmesi. Flush sırasında batch'i map'ten **çıkar**, sonra kilidi bırak, sonra ağ I/O yap.
- `Close()` sırasında bekleyen waiter'ları unutmak → çağıran sonsuza kadar bloklanır.
- Ağ I/O'yu kilit altında yapmak → tüm producer serileşir.

**Kabul kriteri:**
- `TestBatcherFlushesOnSize`: `BatchSize` küçük, `LingerMs` büyük → boyut sınırına ulaşınca linger beklenmeden gönderiliyor.
- `TestBatcherFlushesOnLinger`: `BatchSize` büyük, `LingerMs=50ms` → ~50ms sonra gönderiliyor.
- `TestBatcherOffsetsCorrect`: 100 eşzamanlı `Send`, dönen offset'ler tam olarak `[0,100)`, her çağrı **kendi** kaydının offset'ini alıyor (kayıtları benzersiz value ile işaretle ve broker'dan okuyup doğrula).
- `TestBatcherCloseReleasesWaiters`: bekleyen `Send` varken `Close()` → tüm çağrılar hata veya sonuçla dönüyor, hiçbiri bloklanmıyor. `TestGoroutineLeak` (T-19) helper'ı ile goroutine sızıntısı olmadığı doğrulanıyor.
- `TestGoroutineLeak` geçiyor.

---

## T-41 — Benchmark harness'ı anlamlı hale getir

**Dosya:** `benchmark/main.go`

**Sorun:** Mevcut harness tek goroutine'de senkron round-trip ölçüyor:
```go
for i := 0; i < count; i++ {
    t0 := time.Now()
    _, err := prod.Send(ctx, topic, int32(i%4), key, payload)
    latencies[i] = float64(time.Since(t0).Microseconds()) / 1000.0
}
```
Bu bir **gecikme** ölçümü; "throughput" olarak raporlanan sayı `1/RTT`. 7.251 msg/s = 138 µs/mesaj. Broker'ın kapasitesi hakkında hiçbir şey söylemiyor.

Ek sorunlar:
- Gecikme milisaniye cinsinden 2 ondalıkla raporlanıyor → p50 `0,00 ms` çıkıyor. Ölçüm çözünürlüğü yetersiz.
- `make bench` → `go test ./benchmark/... -bench=.` ama `benchmark/` bir `package main` ve içinde hiç `_test.go` yok. **`make bench` hiçbir şey ölçmüyor.**
- Ortam bilgisi kaydedilmiyor.

**Yapılacak:**

1. **Eşzamanlı producer desteği:** `-producers N` flag'i. N goroutine paralel üretsin, toplam throughput = toplam mesaj / duvar saati süresi. Gecikme histogramı tüm goroutine'lerden toplansın.
2. **Gecikmeyi mikrosaniye tut.** `latencies []time.Duration` olarak topla, raporda `p50/p95/p99/max` mikrosaniye **ve** milisaniye olarak ver. 3 ondalık.
3. **Ortam bilgisi topla ve JSON'a yaz:**
   ```go
   type Environment struct {
       GoVersion  string  // runtime.Version()
       GOOS, GOARCH string
       NumCPU     int     // runtime.NumCPU()
       Timestamp  time.Time
       CommitSHA  string  // -commit flag ile geçilir
       // Disk tipi ve CPU modeli otomatik alınamıyorsa flag ile:
       Notes      string  // -notes "AMD Ryzen 5600, NVMe SSD"
   }
   ```
4. **Warmup ekle:** ilk 10% ölçüme dahil edilmeyecek (JIT yok ama page cache, segment tahsisi, TCP pencere büyümesi var).
5. **Senaryoları düzelt:**
   - `acks=0`, `acks=1`, `acks=all` (tek broker'da acks=all `min_insync_replicas=1` ile anlamlı)
   - mesaj boyutu: 100 B, 1 KiB, 10 KiB
   - producer sayısı: 1, 4, 16
   - **batching: `LingerMs=0` vs `LingerMs=5 + BatchSize=64KiB`** — T-40 sonrası bu senaryo artık gerçek bir şeyi ölçüyor
   - consumer: `Poll` throughput'u, ayrıca **cold** (page cache dışı, dosya yeniden açılmış) ve **warm** ayrımı
6. **`make bench`'i düzelt:** `go run ./benchmark -out benchmark_results.json` olacak. Ayrıca gerçek Go microbenchmark'ları için `internal/storage/record_test.go` ve `internal/protocol/codec_test.go` içine `Benchmark*` fonksiyonları ekle ve `make bench-micro` hedefi tanımla.

**Kabul kriteri:** `go run ./benchmark -out results.json -producers 8 -notes "test"` çalışıyor, JSON'da ortam bloğu ve mikrosaniye gecikmeler var, p50 artık `0.00` değil. `make bench` gerçekten benchmark koşuyor.

---

## T-42 — `docs/BENCHMARK.md`'yi yeniden yaz

**Dosya:** `docs/BENCHMARK.md`

**Sorun:**
- `İşletim Sistemi: Windows / Linux` — bu bir ortam tanımı değil. CPU, RAM, disk tipi, Go sürümü, çekirdek sayısı yok. Ölçüm tekrarlanabilir değil.
- p50 `0,00 ms` — ölçüm aracının yetersizliğini rapor ediyor.
- Her tablo satırında tekrar eden "Üreten Komut" kolonu — 4 satırda aynı komut, saf gürültü.
- **"Yorumlama ve Karşılaştırma Analizi" bölümünün her maddesi "ölçülmedi" ile bitiyor.** "Kafka zero-copy kullanır. Dış Kafka kıyaslaması ölçülmedi." Bu bölüm hiçbir bilgi taşımıyor ama analiz yapılmış görüntüsü veriyor. Bir okuyucu için bu, olmamasından daha kötü.

**Yapılacak:**

1. **Ortam bölümü** — T-41'in JSON çıktısındaki `Environment` bloğundan doldurulacak, elle yazılmayacak.
2. **Metodoloji bölümü** — açıkça yaz: kaç producer, warmup ne kadar, kaç tekrar, gecikme nasıl ölçüldü (round-trip mi, broker-side mi), throughput nasıl hesaplandı.
3. **"Üreten Komut" kolonunu kaldır**, tek bir kod bloğu olarak metodoloji bölümünde ver.
4. **Karşılaştırma bölümünü ikiye ayır:**
   - `## Ölçülen` — yalnızca gerçekten ölçülmüş şeyler (acks etkisi, mesaj boyutu etkisi, batching etkisi, producer sayısı ölçeklemesi, cold vs warm read).
   - `## Ölçülmeyen ve Neden` — kısa bir liste: *"Apache Kafka ile doğrudan karşılaştırma yapılmadı; farklı wire protokolü nedeniyle aynı istemciyle her ikisine yük bindirilemiyor. Anlamlı bir kıyas için ayrı istemciler yazılması gerekir."* Bu dürüst ve bilgilendirici; mevcut hali ise ne dürüst ne bilgilendirici.
5. **Darboğaz analizi ekle** — W3'te ölçtüğün öncesi/sonrası sayılarla: *"Tamponlama eklenmeden önce istek başına 8 read syscall'ı vardı; `bufio` ile X msg/s'den Y msg/s'ye çıktı."* Bu, bir işverenin gerçekten okumak istediği bölüm ve elinde artık verisi var.

**Kabul kriteri:** Dokümanda "ölçülmedi" ifadesi yalnızca "Ölçülmeyen ve Neden" bölümünde geçiyor. Tüm sayılar `benchmark_results.json`'dan türetilebilir. p50 satırında `0,00` yok.

---

# W5 — Temizlik

Tek agent, tek commit serisi. Her biri küçük.

| ID | Dosya | İş |
|---|---|---|
| T-50 | `internal/storage/segment.go` (`Read`) | `_ = found // found is informational` ölü kodunu kaldır. `Lookup`'ın `found` dönüşü gerçekten kullanılmıyorsa imzadan çıkar veya `Read`'de anlamlı şekilde kullan (exact match'te scan'i atla — küçük bir optimizasyon). |
| T-51 | `internal/storage/log.go` (`AppendBatch`) | Boş slice için `ErrEmptyLog` dönüyor; isim yanlış (`ErrEmptyLog` = "log'da kayıt yok"). Yeni `ErrEmptyBatch` tanımla. |
| T-52 | `internal/storage/log.go` (`runFlush`) | Doc comment "FlushMs geçtiğinde fsync eder" diyor ama son-flush zamanı takip edilmiyor; ticker `min(1s, FlushMs)` olduğu için `FlushMs=5000`'de her 1s'de flush ediyor. Ya `lastFlush time.Time` ekle ya da yorumu gerçeğe uydur. Tercih: `lastFlush` ekle, config'e saygı göster. |
| T-53 | `internal/storage/index.go` | `Truncate()` ve `Close()` neredeyse aynı işi yapıyor; `Truncate` hiç çağrılmıyor gibi (grep ile doğrula). Kullanılmıyorsa kaldır, kullanılıyorsa farkı doc comment'te açıkla. |
| T-54 | `internal/server/handler.go` | `Mux.handlers` map'i kilitsiz okunuyor. Yalnızca başlatmada yazıldığı için pratikte güvenli, ama bu **sözleşme doküman edilmemiş**. `Handle`'ın doc comment'ine ekle: `// Handle is not safe for concurrent use and must only be called during setup, before Dispatch.` |
| T-55 | `internal/server/server.go` | `maxConns` kontrolü `activeConns`'a bakıyor ama sayaç handler goroutine'inin **içinde** artıyor; kabul ile artış arasında yarış penceresi var, limitin üstüne çıkılabilir. Sayacı `Accept`'ten hemen sonra, goroutine başlatmadan **önce** artır; goroutine yalnızca azaltsın. |
| T-56 | `.gitignore` | `*.log` deseni var ve bu projenin **veri dosyaları** `.log` uzantılı. `/data/` zaten ignore'lu ama test artıkları farklı yere düşerse sessizce ignore edilir. `*.log` yerine daha dar bir desen kullan veya kaldır. |
| T-57 | `docs/` | `PHASE_1.md` … `PHASE_6.md` (6 dosya, 260 satır) geliştirme günlüğü. Bir okuyucu için değeri yok, `docs/` dizinini gürültülü gösteriyor. `docs/history/` altına taşı veya `CHANGELOG.md` ile birleştir. `STATUS.md` (19 satır) ve `PLAN.md` (56 satır) da güncelliğini yitirmiş — ya güncelle ya kaldır. |
| T-58 | `docs/FINDINGS.md` (yeni) | W0-W5 boyunca agent'ların "kapsam dışı gözlem" olarak biriktirdiği maddelerin toplandığı dosya. Plan bitiminde bu dosya bir sonraki turun girdisi olacak. |

---

## Ek: CI İyileştirmesi (opsiyonel, W5 sonrası)

`.github/workflows/ci.yml` şu an yalnızca `ubuntu-latest`. Ama repo Windows'a özel kod içeriyor (`internal/storage/index_windows.go`, mmap farkları, `SetEndOfFile` kısıtı, dosya kilitleme davranışı). Bu kod **hiç CI'da test edilmiyor.**

```yaml
strategy:
  matrix:
    os: [ubuntu-latest, windows-latest]
runs-on: ${{ matrix.os }}
```

Not: `-race` Windows'ta CGO gerektirir; matris içinde koşullu yap veya Windows'ta race'siz koş.

Ayrıca commit geçmişinde golangci-lint sürümüyle boğuşan **8 ardışık commit** var (`c1e428b`…`6434124`). `kontrol.sh` zaten bu iş için mevcut — agent'lara kural: **push'tan önce `./kontrol.sh` lokalde koşacak.** CI'ı deneme tahtası olarak kullanmak geçmişi kirletiyor ve geçmiş de portföyün parçası.

---

## Özet Tablo

| Dalga | Görevler | Paralel? | Tahmini ağırlık |
|---|---|---|---|
| W0 | T-01 … T-05 | Evet | Hafif, yüksek getiri |
| W1 | **T-19 ilk**, sonra T-10, T-11, T-12, T-13, T-14, T-15 | T-19 sonrası evet | Orta; T-15 ve T-13 en zor |
| W2 | T-20, T-21, T-22, **T-23 (T-11'den sonra)** | Kısmen | Orta |
| W3 | T-30, **T-31 (T-15'ten sonra)**, T-32, T-33, T-34, T-35 | Kısmen | Orta; T-32 ve T-34 riskli (veri bozulması potansiyeli) |
| W4 | **T-40 → T-41 → T-42** (sıralı) | Hayır | T-40 en karmaşık görev |
| W5 | T-50 … T-58 | Evet | Hafif |

**En riskli üç görev** (ekstra inceleme gerektirir): T-34 (append seek kaldırma → veri bozulması riski), T-33 (record encode → wire format riski), T-40 (batcher → deadlock/goroutine sızıntısı riski). Bu üçünde golden test ve goroutine sızıntı testi pazarlık konusu değil.
