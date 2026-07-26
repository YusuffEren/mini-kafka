# PHASE 1 — Storage Katmanı Dokümantasyonu

## 1. Genel Bakış
Faz 1, mini-kafka'nın temel saklama ortamını (`internal/storage`) inşa eder. Ağ bağlantısı ve broker mantığı bulunmaz; sadece disk üzerinde append-only commit log, sparse index, segment yonetimi ve log recovery fonksiyonları bulunur.

## 2. Bileşenler

### Record (`internal/storage/record.go`)
- Record yapısı: CRC32-Castagnoli doğrulamalı big-endian ikili format.
- Tombstone desteği (`Attributes` bit 0).
- Null key ve null value desteği (length = -1).

### Sparse Index (`internal/storage/index.go`)
- Her record için değil, her `IndexIntervalBytes` (varsayılan 4 KiB) veri için 8-byte entry (`relativeOffset uint32`, `position uint32`).
- Binary search (`Lookup`) ile aranacak offset'e en yakın küçük entry bulunur ve `.log` dosyasında sıralı tarama yapılır.
- Index dosyası `mmap` (`unix.Mmap`) ile sıfır kopyalama ve yüksek rastgele erişim performansı ile okunur.

### Segment (`internal/storage/segment.go`)
- Tek bir `.log` ve `.index` çiftini temsil eder.
- Boyut veya zaman sınırı dolduğunda yenisine rotasyon yapılır.

### Log (`internal/storage/log.go`)
- Sıralı segment dizisini tutar.
- broker açılışında son segment recovery edilir, CRC hatası varsa bozuk record noktasına kadar dosya truncate edilir ve LEO hesaplanır.
- Retention: Arka planda yaslanan veya boyut sınırını aşan eski segment'ler temizlenir.

## 3. Kazanımlar ve Metrikler
- Unit testler %80+ coverage ile tamamlandı.
- Segment rotation, recovery, retention ve `-race` paralel yazma testleri basariyla gecti.
