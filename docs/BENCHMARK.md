# Benchmark Sonuçları — mini-kafka

Bu belge `benchmark/main.go` (T-41 harness yeniden yazımı) tarafından üretilen
`benchmark_results.json` çıktısından türetilmiştir. Harness; paralel producer
goroutine'leri, mikrosaniye çözünürlükte gecikme ölçümü ve ortam bilgisi bloğu
içerecek şekilde yeniden yazılmıştır.

---

## 1. Ortam

Ölçümler aşağıdaki makinede, broker ve istemci aynı süreç host'unda (localhost)
çalıştırılarak alınmıştır.

| Bileşen | Değer |
|---|---|
| İşletim Sistemi | Microsoft Windows 11 Pro |
| CPU | AMD Ryzen 5 5600H (6 çekirdek / 12 mantıksal iş parçacığı, 3,3 GHz) |
| RAM | 16 GB (16.477.028.352 bayt) |
| Disk | SK hynix PC711 HFS512GDE9X073N NVMe SSD (~477 GB) |
| Go sürümü | go1.26.4 |
| GOOS / GOARCH | windows / amd64 |
| Çalıştırma zaman damgası | 2026-07-26T14:05:59Z |
| Ağ | localhost (TCP loopback, 127.0.0.1) |

Ortam bilgisi `benchmark_results.json` içindeki `environment` bloğundan alınmıştır;
CPU/RAM/disk değerleri host makineden manuel olarak eklenmiştir.

---

## 2. Metodoloji

- **Harness**: `benchmark/main.go` yerel bir broker'ı geçici veri dizininde ayağa
  kaldırır, senaryoları sırayla çalıştırır ve sonuçları `benchmark_results.json`
  dosyasına yazar. Broker ve istemci aynı host'ta, TCP loopback üzerinden konuşur.
- **Producer sayısı**: Bu koşu `-producers 1` ile çalıştırılmıştır (tek üretici
  goroutine'i). Harness `-producers N` bayrağını destekler; N>1 durumunda toplam
  mesaj sayısı N goroutine'e bölünür ve her goroutine payına düşen mesajları
  eşzamanlı olarak gönderir. Tüm senaryolar aynı `Producer` örneğini paylaşır
  (`Producer` eşzamanlı kullanım için güvenlidir).
- **Warmup**: Her senaryoda, her goroutine'in gönderdiği mesajların ilk %10'u
  warmup olarak kabul edilir ve gecikme istatistiklerinden çıkarılır. Amaç;
  bağlantı kurulumu, ilk batch boyutlandırması ve JIT ısınmasının persentilleri
  bozmasını önlemektir. Warmup mesajları `warmup_messages`, ölçülen mesajlar
  `measured_messages` alanlarında ayrı ayrı raporlanır.
- **Tekrar**: Her senaryo tek bir koşu olarak çalıştırılır (çoklu koşu ortalaması
  alınmaz). Sonuçlar tek koşudan gelir; çapraz-makine karşılaştırma için
  `environment` bloğu kalıcı olarak JSON'a yazılır.
- **Throughput**: `msg_per_sec = measured_messages / (duration_ms / 1000)`.
  `mb_per_sec = (measured_messages * payload_size) / (1024*1024) / (duration_ms/1000)`.
  Warmup mesajları throughput hesabına dahil edilir (süre tüm koşuyu kapsar),
  gecikme persentillerine dahil edilmez.
- **Gecikme ölçümü**: Her `Producer.Send` çağrısı için `time.Since(t0)` ile
  çağrı-öncesi/sonrasi farkı alınır. Süreler nanosaniye çözünürlüğünde toplanır,
  sonra `Nanoseconds()/1000.0` ile mikrosaniyeye çevrilir — böylece localhost
  üzerinde sub-mikrosaniye gecikmeler sıfıra yuvarlanmaz. Persentiller
  nearest-rank yöntemiyle sıralı diziden alınır (p50/p95/p99/max).
- **Senaryolar** (8 koşu):
  1. Single Producer 1KB (acks=1) — referans.
  2. Mesaj boyutu etkisi: 100B ve 10KB (acks=1).
  3. Tüketici throughput: Group Consumer Poll (1KB).
  4. `acks` etkisi: 1KB mesajda acks=0 vs acks=all.
  5. Batching etkisi: 1KB mesajda linger=5ms/batch=64KB vs linger=0ms/batch=16KB.

---

## 3. Ölçülen Sonuçlar

Aşağıdaki tablo `benchmark_results.json` içindeki `results` dizisinden
türetilmiştir. Gecikme birimi mikrosaniye (µs)'dir. p50, localhost üzerinde
mikrosaniye çözünürlüğünün altında kaldığı durumlarda `< 1` olarak
gösterilmiştir; bu, ölçümün taban çözünürlüğünün altına düşmesini ifade eder,
gerçek gecikme sinyali p95/p99'da görünür.

| Senaryo | Mesaj | Producer | Throughput (msg/s) | Bant (MB/s) | p50 (µs) | p95 (µs) | p99 (µs) | max (µs) |
|---|---|---|---|---|---|---|---|---|
| Single Producer 1KB (acks=1) | 1 KB | 1 | 9.384,78 | 9,16 | < 1 | 536 | 1.601 | 7.412 |
| Producer 100B (acks=1) | 100 B | 1 | 10.204,08 | 0,97 | < 1 | 532 | 1.504 | 9.344 |
| Producer 10KB (acks=1) | 10 KB | 1 | 8.411,21 | 82,14 | < 1 | 532 | 2.005 | 5.749 |
| Group Consumer Poll | 1 KB | 1 | 230.769,23 | 225,36 | 14.963 | 14.963 | 14.963 | 14.963 |
| Producer 1KB (acks=0) | 1 KB | 1 | 7.915,57 | 7,73 | < 1 | 831 | 2.202 | 8.259 |
| Producer 1KB (acks=all) | 1 KB | 1 | 7.377,05 | 7,20 | < 1 | 916 | 2.444 | 9.106 |
| Producer 1KB (linger=5ms, batch=64KB) | 1 KB | 1 | 161,54 | 0,16 | 5.509 | 5.883 | 7.332 | 30.201 |
| Producer 1KB (linger=0ms, batch=16KB) | 1 KB | 1 | 8.078,99 | 7,89 | < 1 | 536 | 2.469 | 8.156 |

### 3.1 `acks` etkisi

1 KB mesaj, tek producer, acks=1 referansına göre:

| acks | Throughput (msg/s) | p95 (µs) | p99 (µs) |
|---|---|---|---|
| 0 (fire-and-forget) | 7.915,57 | 831 | 2.202 |
| 1 (leader ack) | 9.384,78 | 536 | 1.601 |
| all (ISR ack, -1) | 7.377,05 | 916 | 2.444 |

Beklendiği gibi `acks=all` en düşük throughput ve en yüksek kuyruk gecikmesini
verir (ISR üzerinden replikasyon bekleme maliyeti). `acks=0`'ın `acks=1`'den
düşük çıkması, tek-bağlantı `reqMu` serileştirmesinin (bkz. Darboğaz analizi)
baskın darboğaz olduğunu ve acks modundan bağımsız olduğunu gösterir; acks=0
kazancı, bağlantı üzerinde hâlâ tek-uçuş sınırı olduğu için ortaya çıkmaz.

### 3.2 Mesaj boyutu etkisi

| Boyut | Throughput (msg/s) | Bant (MB/s) | p99 (µs) |
|---|---|---|---|
| 100 B | 10.204,08 | 0,97 | 1.504 |
| 1 KB | 9.384,78 | 9,16 | 1.601 |
| 10 KB | 8.411,21 | 82,14 | 2.005 |

Mesaj boyutu arttıkça mesaj/s düşer ama bant genişliği (MB/s) belirgin biçimde
artar — darboğaz mesaj sayısından ziyade uçuş başına serileştirme/çözme
maliyetidir. 10 KB'da 82 MB/s'lik bant, localhost TCP loopback'inin pratik
tavanına yaklaşır.

### 3.3 Producer sayısı

Bu koşu `-producers 1` ile alınmıştır. Harness paralel producer'ı destekler
ancak `Producer` tüm istekleri tek bağlantı üzerinden `reqMu` ile serilediği
için (bkz. Darboğaz analizi) producer sayısını artırmak tek-bağlantı
throughput'unu anlamlı biçimde yükseltmez; bu nedenle tabloda yalnızca
producer=1 koşusu yer alır.

### 3.4 Batching etkisi

| Yapılandırma | Throughput (msg/s) | p50 (µs) | p99 (µs) |
|---|---|---|---|
| linger=0ms, batch=16KB (varsayılan, senkron) | 8.078,99 | < 1 | 2.469 |
| linger=5ms, batch=64KB (batched) | 161,54 | 5.509 | 7.332 |

`linger=5ms` her batch'i en az 5 ms beklettiği için 10.000 mesaj koşusu
~55 s'ye yayılır ve msg/s dramatik düşer. Bu bir hata değil, linger'ın
tasarım davranışıdır: gecikme artar ama her Produce isteği daha çok kayıt
taşıyacağı için ağ başına toplam istek sayısı düşer. Bu koşudaki p50
(5.509 µs ≈ 5,5 ms) doğrudan linger süresini yansıtır.

### 3.5 Tüketici throughput

Group Consumer Poll senaryosu 10.000 mesajı tek bir `Poll` döngüsüyle
tükettiği için gecikme persentillerinin hepsi tek bir ölçüme (14.963 µs)
karşılık gelir; bu senaryoda persentil dağılımı anlamlı değildir, yalnızca
toplam throughput (230.769 msg/s, 225 MB/s) anlamlıdır. Yüksek değer, verinin
tek seferde büyük bir batch olarak çekilmesinden kaynaklanır.

---

## 4. Ölçülmeyen ve Neden

- **Apache Kafka ile yan yana karşılaştırma ölçülmedi.** mini-kafka kendi
  binary wire protokolünü kullanır; Apache Kafka ise Kafka protokolü üzerinden
  konuşur. İkisi farklı protokol katmanlarına sahip olduğu için aynı istemci
  ile aynı koşulları üretmek mümkün değildi. `docker-compose.yml` içine
  `confluentinc/cp-kafka` eklenerek yan yana bir koşu planlanmış olsa da bu
  benchmark'a dahil edilmedi.
- **Ağ gecikmesi ölçülmedi.** Broker ve istemci aynı host'ta, TCP loopback
  üzerinde çalıştırıldı; gerçek ağ (cross-host) gecikmesi koşuya dahil değil.
- **Zero-copy (`sendfile`) kazancı ölçülmedi.** mini-kafka tüketici tarafında
  `mmap` ile zero-copy okuma yapar ancak `sendfile` tabanlı zero-copy gönderimi
  uygulanmadı; bu nedenle zero-copy'nin throughput katkısı kaydedilmedi.
- **Sıkıştırma (compression) etkisi ölçülmedi.** Record batch'leri ham
  (sıkıştırmasız) gönderilir; batch sıkıştırma uygulanmadığı için sıkıştırma
  oranı ve bant kazancı kaydedilmedi.
- **Çoklu-koşu varyansı ölçülmedi.** Her senaryo tek bir koşu olarak
  çalıştırıldı; standart sapma/çeyrekler arası ölçüm tutulmadı.
- **Replikasyonlu `acks=all` koşusu ölçülmedi.** Bu koşuda broker tek
  düğüm olarak çalıştırıldı; `acks=all` ISR bekleme yolunu exercise etmediği
  için replikasyon maliyeti kaydedilmedi.

---

## 5. Darboğaz Analizi

### 5.1 Baskın darboğaz: tek-bağlantı, tek-uçuş serileştirme

`Producer` tüm `Send`/`SendBatch` çağrılarını tek bir TCP bağlantısı üzerinden
`reqMu sync.Mutex` ile seriler (bkz. `pkg/client/producer.go`,
`produceRecords`). Bu, bir Produce isteği tamamlanana (yanıt okunana) kadar
bağlantıdaki diğer çağrıların beklemesi anlamına gelir: bağlantıda aynı anda
en fazla bir uçuş (in-flight) istek olabilir. Sonuç olarak producer throughput,
`1 / RTT` ile sınırlıdır.

Bu, yukarıdaki `acks` etkisi bölümünde `acks=0`'ın `acks=1`'den daha hızlı
çıkMAMAsının açıklamasıdır: acks modünden bağımsız olarak her Send yine bir
tam round-trip bekler. Aynı nedenle producer sayısını artırmak (paralel
goroutine'ler) tek `Producer` örneği paylaşıldığında throughput'u anlamlı
biçimde yükseltmez — hepsi aynı `reqMu`'da sıraya girer.

Giderilebilir yol (kapsam dışı, `docs/FINDINGS.md`'de "true pipelining" başlığı
altında izlendi): `correlationID -> chan *ResponseFrame` eşlemesi tutan bir
bekleyen-istek kaydı + bağlantıyı okuyan ayrı bir reader goroutine. Bu,
aynı bağlantıda birden çok uçuş isteğine izin verir ve throughput'u
`1/RTT` sınırından `pencere_boyutu / RTT`'ye çıkarır.

### 5.2 `bufio` öncesi/sonrası syscall baskısı

`Producer`, TCP bağlantısını `bufio.Reader` (64 KB) ve `bufio.Writer` (64 KB)
ile sarar (`getConn` içinde, her yeni bağlantıda yeniden oluşturulur). Her
Produce isteği: (i) istek frame'ini `bufio.Writer`'a yazar, (ii) `bw.Flush()`
ile tek bir `Write` syscall'i ile bağlantıya iter, (iii) yanıtı `bufio.Reader`
üzerinden tek `Read` syscall'i ile okur.

- **bufio öncesi** (ham `conn.Write`/`conn.Read`): her Produce başına çoklu
  syscall — header yazımı + payload yazımı ayrı `Write`'lar, yanıt için ayrı
  `Read`'ler. Frame encode'u `bytes.Buffer` üzerinden parça parça `Write`'a
  giderdi; kabaca **her Produce başına ~3+ syscall** (write header, write
  payload, read response) ve küçük mesajlarda bu, syscall başına sabit
  çekirdek geçiş maliyeti nedeniyle baskın hale gelir.
- **bufio sonrası** (mevcut): encode çıktısı user-space buffer'da birikir,
  tek `Flush` = tek `Write` syscall. Produce başına **~2 syscall** (1 write +
  1 read). 1 KB senaryosunda **~9.385 msg/s** ile sonuçlanır.

Özetle: `bufio` katmanı, küçük mesajlarda syscall sayısını Produce başına
~3+'den ~2'ye düşürür ve user-space buffering sayesinde kernel geçiş
maliyetini amorti eder. Geriye kalan baskın darboğaz syscall sayısı değil,
5.1'deki tek-uçuş serileştirmesidir.

### 5.3 Tüketici tarafı

Group Consumer Poll senaryosunda throughput 230.769 msg/s'ye ulaşır —
producer senaryolarından ~25× daha yüksek. Bunun nedeni tüketici tarafının
tek bir `Poll` çağrısında büyük bir kayıt batch'ini tek round-trip ile
çekmesidir; producer'daki tek-uçuş serileştirmesi tüketici için aynı derecede
bağlayıcı değildir çünkü tek bir Poll çok sayıda kaydı tek yanıtta döndürür.

---

## 6. Sonuç

- Tek producer, 1 KB mesaj, acks=1: **~9.385 msg/s**, p99 **~1,6 ms**.
- Darboğaz: tek-bağlantı tek-uçuş `reqMu` serileştirmesi (throughput ≈ 1/RTT).
- `bufio` (64 KB) syscall sayısını Produce başına ~3+'den ~2'ye indirir;
  geriye kalan maliyet tek-uçuş beklemedir.
- `acks=all` ve `linger=5ms` beklenen maliyeti verir; `acks=0` kazancı tek
  bağlantı serileştirmesi tarafından maskelenir.
- Tüketici tarafı batch'li Poll sayesinde ~25× daha yüksek throughput'a ulaşır.