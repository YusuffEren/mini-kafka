# DECISIONS - mini-kafka

## Teknoloji Seçimleri
- **Dil:** Go 1.22+ (spec kararı)
- **Transport:** Raw TCP, custom binary frame (HTTP overhead yok, öğretici)
- **Serialization:** Elle yazılmış encoder/decoder, big-endian (Kafka ile aynı)
- **Storage:** os.File yazma, mmap index okuma
- **Concurrency:** Connection başına goroutine, partition başına tek writer goroutine + channel
- **Replikasyon:** Leader-based, pull tabanlı follower
- **Controller:** Statik config'ten leader ataması (consensus protokolü yok)

## Bağımlılıklar
- Standart kütüphane: sınırsız
- golang.org/x/sys: mmap için
- gopkg.in/yaml.v3: config parsing
- github.com/stretchr/testify: sadece test dosyalarında
- **Başka bağımlılık YOK**

## Mimari Varsayımlar
- Tek binary broker (Faz 2), sonra multi-broker (Faz 5)
- Partition başına sıralı yazma garantisi
- Consumer group offset'leri __consumer_offsets internal topic'inde
- Long-poll fetch: busy-wait yok, sync.Cond ile bekletme
- ISR takibi: leader-based, statik config
- Benchmark: aynı makine, aynı disk, page cache temizlenmiş
