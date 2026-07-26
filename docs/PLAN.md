# PLAN - mini-kafka (AGENT_PLAN duzeltmeleri)

Kaynak: mini-kafka-AGENT_PLAN.md — kod incelemesi sonucu tespit edilen eksikler.

## W0 — Dogruluk ve Build Butunlugu
- [x] T-01 README protokol uyumlulugu iddiasini duzelt
- [x] T-02 Silinmis protokol spec referanslarini onar
- [x] T-03 Dockerfile Go surumu + non-root
- [x] T-04 docker compose calisan sistem
- [x] T-05 LICENSE ekle

## W1 — Correctness Bug'lari (T-19 ilk, sonra paralel)
- [x] T-19 Eszamanlilik ve sizinti test altyapisi
- [x] T-10 Segment.FlushWriter veri yarisi
- [x] T-11 Long-poll listener bellek sizintisi
- [x] T-12 Retention broker restart'inda sifirlaniyor
- [x] T-13 Hard crash sonrasi sealed segment index'leri
- [x] T-14 4 GiB ustu segment'te sessiz tasma
- [x] T-15 pkg/client.Producer eszamanli kullanimda bozuluyor

## W2 — Dayaniklilik ve Hata Gorunurlugu
- [x] T-20 Istemci baglantilarinda deadline yok
- [x] T-21 Handler hatalari tek ErrUnknown'a cokuyor
- [x] T-22 acks=all cok partition'da timeout'lari topluyor
- [x] T-23 Long-poll yalnizca ilk partition'a bakiyor (T-11'den sonra)

## W3 — Performans
- [x] T-30 Sunucu baglantilarinda tamponlama
- [x] T-31 Istemci baglantilarinda tamponlama (T-15'ten sonra)
- [x] T-32 Okuma yolu ve recovery tamponsuz
- [x] T-33 Record.Encode/DecodeRecord tahsis-yogun
- [x] T-34 Segment.Append'te gereksiz syscall
- [x] T-35 index_max_bytes gereginin 40 kati

## W4 — Gercek Batching ve Benchmark (sirali)
- [x] T-40 LingerMs batching yapmiyor
- [x] T-41 Benchmark harness'i anlamli hale getir
- [x] T-42 docs/BENCHMARK.md'yi yeniden yaz

## W5 — Temizlik (paralel)
- [x] T-50 segment.go olu kod
- [x] T-51 log.go hata ismi
- [x] T-52 runFlush zaman takibi
- [x] T-53 index.go Truncate/Close
- [x] T-54 handler.go map guvenligi doc
- [x] T-55 server.go maxConns yarisi
- [x] T-56 .gitignore *.log deseni
- [x] T-57 docs/ temizligi
- [x] T-58 docs/FINDINGS.md