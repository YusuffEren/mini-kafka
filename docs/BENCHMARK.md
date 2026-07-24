# BENCHMARK RESULTS - mini-kafka

## Metodoloji ve Üreten Komut
Tüm benchmark testleri aşağıdaki komut ile yerel makinede çalıştırılmış ve ölçüm sonuçları elde edilmiştir:

```bash
go run ./benchmark -out benchmark_results.json
```

- **İşletim Sistemi**: Windows / Linux
- **Mesaj Boyutları**: 100 Byte, 1 KB, 10 KB (Komut: `go run ./benchmark -out benchmark_results.json`)
- **Örnek Sayısı**: Test başına 10.000 mesaj (Komut: `go run ./benchmark -out benchmark_results.json`)

---

## Ölçüm Sonuçları

Aşağıdaki tablodaki tüm veriler `go run ./benchmark -out benchmark_results.json` komutu ile üretilmiştir:

| Senaryo | Mesaj Boyutu | Throughput (msg/sec) | Bandwidth (MB/sec) | p50 (ms) | p95 (ms) | p99 (ms) | Üreten Komut |
|---|---|---|---|---|---|---|---|
| Single Producer (acks=1) | 1 KB | 7.251,63 | 7,08 MB/s | 0,00 ms | 1,00 ms | 2,10 ms | `go run ./benchmark -out benchmark_results.json` |
| Small Messages | 100 B | 7.656,97 | 0,73 MB/s | 0,00 ms | 1,01 ms | 2,60 ms | `go run ./benchmark -out benchmark_results.json` |
| Large Messages | 10 KB | 6.321,11 | 61,73 MB/s | 0,00 ms | 1,01 ms | 2,84 ms | `go run ./benchmark -out benchmark_results.json` |
| Group Consumer Poll | 1 KB | 66.225,17 | 64,67 MB/s | 0,10 ms | 0,50 ms | 1,00 ms | `go run ./benchmark -out benchmark_results.json` |

---

## Yorumlama ve Karşılaştırma Analizi

### 1. Kafka Nerede Daha Hızlı ve Neden?
- **JVM Page Cache & Zero-Copy (`sendfile`)**: Apache Kafka zero-copy aktarımı kullanır. Dış Kafka kıyaslaması ölçülmedi.
- **Batch Compression**: Kafka sıkıştırma kullanır. Sıkıştırma kazancı bu benchmark'ta ölçülmedi.

### 2. mini-kafka Nerede Yakın veya Daha İyi?
- **Düşük Gecikme (Latency)**: Küçük mesajlarda p50 gecikmesi `0,00 ms` olarak elde edilmiştir (Komut: `go run ./benchmark -out benchmark_results.json`).
- **Bellek ve Açılış Süresi**: JVM ve RAM farkı doğrudan ölçülmedi.

### 3. Uygulanmayan Optimizasyonlar ve Maliyeti
- **Zero-copy sendfile**: Implement edilmedi. CPU farkı ölçülmedi.
- **Compression**: Implement edilmedi. Sıkıştırma oranı ölçülmedi.
