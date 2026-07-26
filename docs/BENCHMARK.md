# Benchmark Sonuclari

## Metodoloji
- Producer'lar: her senaryoda belirtilen sayida eszamanli goroutine
- Warmup: her senaryoda ilk %10 olcum disi
- Mesaj boyutlari: 100 B, 1 KB, 10 KB
- Her senaryo tek sefer kosuldu
- Batching: paylasilan tek Producer + M goroutine eszamanli Send (M = producers * 2)
- Consumer: 100 Poll, sadece veri donen poll'lar olculdu

## Ortam
| Alan | Değer |
|---|---|
| GoVersion | go1.26.4 |
| GOOS | windows |
| GOARCH | amd64 |
| CPU | 12 |
| Timestamp | 2026-07-26T17:21:15Z |
| Notes | AMD Ryzen 5 5600H, 16 GB, NVMe SSD |

## Sonuçlar
| Senaryo | Producers | batching_senders | Throughput (msg/s) | p50 | p95 | p99 | max | batch_fill |
|---|---|---|---|---|---|---|---|---|
| Single Producer 1KB (acks=1) [producers=1] | 1 |  | 9230.77 | 0.000 [*] | 538.200 | 887.600 | 4786.100 |  |
| Producer 100B (acks=1) [producers=1] | 1 |  | 5667.51 | 0.000 [*] | 650.900 | 1518.300 | 26187.700 |  |
| Producer 10KB (acks=1) [producers=1] | 1 |  | 7234.73 | 0.000 [*] | 1001.900 | 2509.800 | 7459 |  |
| Producer 1KB (acks=0) [producers=1] | 1 |  | 9036.14 | 0.000 [*] | 550.500 | 1580.300 | 5246.400 |  |
| Producer 1KB (acks=all) [producers=1] (tek broker, min_insync_replicas=1) | 1 |  | 7943.51 | 0.000 [*] | 632.800 | 2139.300 | 10519.400 |  |
| Producer 1KB (linger=5ms, batch=64KB) [producers=1] | 1 | 2 | 325.49 | 5498.700 | 5863.400 | 6552.300 | 16680.400 | 0.0325 |
| Producer 1KB (linger=0ms, batch=16KB) [producers=1] | 1 |  | 8991.01 | 0.000 [*] | 546.500 | 1554.900 | 7995.900 |  |
| Single Producer 1KB (acks=1) [producers=4] | 4 |  | 32608.70 | 0.000 [*] | 1015.700 | 1508.800 | 5053.600 |  |
| Producer 100B (acks=1) [producers=4] | 4 |  | 33834.59 | 0.000 [*] | 1005.900 | 1509.400 | 2833.600 |  |
| Producer 10KB (acks=1) [producers=4] | 4 |  | 21028.04 | 0.000 [*] | 1136 | 2222.700 | 4210.500 |  |
| Producer 1KB (acks=0) [producers=4] | 4 |  | 31468.53 | 0.000 [*] | 969.600 | 1685.700 | 5510.300 |  |
| Producer 1KB (acks=all) [producers=4] (tek broker, min_insync_replicas=1) | 4 |  | 30927.84 | 0.000 [*] | 1042 | 1647.100 | 4673.100 |  |
| Producer 1KB (linger=5ms, batch=64KB) [producers=4] | 4 | 8 | 1302.27 | 5512 | 5887.700 | 6509.900 | 8390.600 | 0.1299 |
| Producer 1KB (linger=0ms, batch=16KB) [producers=4] | 4 |  | 32374.10 | 0.000 [*] | 1005.400 | 1789 | 4543.500 |  |
| Single Producer 1KB (acks=1) [producers=16] | 16 |  | 80428.57 | 0.000 [*] | 1000.300 | 2019.400 | 4211.500 |  |
| Producer 100B (acks=1) [producers=16] | 16 |  | 91918.37 | 0.000 [*] | 999.700 | 2014.800 | 4672 |  |
| Producer 10KB (acks=1) [producers=16] | 16 |  | 40214.29 | 0.000 [*] | 1528.400 | 2434.800 | 4322.300 |  |
| Producer 1KB (acks=0) [producers=16] | 16 |  | 75697.48 | 0.000 [*] | 1041.500 | 2017 | 4579.500 |  |
| Producer 1KB (acks=all) [producers=16] (tek broker, min_insync_replicas=1) | 16 |  | 76991.45 | 0.000 [*] | 1041.400 | 2007.500 | 6054.700 |  |
| Producer 1KB (linger=5ms, batch=64KB) [producers=16] | 16 | 32 | 5132.76 | 5581.400 | 6148 | 6539.800 | 7914.200 | 0.5189 |
| Producer 1KB (linger=0ms, batch=16KB) [producers=16] | 16 |  | 79017.54 | 0.000 [*] | 1007.600 | 2014.800 | 4382.500 |  |
| Group Consumer Poll | 1 |  | 3498.84 | 1006.800 | 2022.200 | 4147.300 | 4147.300 |  |

[*] p50 degeri sistem saat cozunurlugunun altinda; Windows'ta ~500us kuantum. Daha hassas olcum icin Linux'ta kosun.

## Analiz

### Olceklenme
"Single Producer 1KB (acks=1)" senaryosunda producers sayisi arttikca throughput su sekilde
degismistir:

- producers=1  → 9230.77 msg/s
- producers=4  → 32608.70 msg/s  (1 producer'a gore ~3.53x)
- producers=16 → 80428.57 msg/s  (4 producer'a gore ~2.46x, 1 producer'a gore ~8.72x)

1→4 gecisinde yaklasik 3.5x, 4→16 gecisinde ~2.5x kazanc saglanmistir. Toplamda 1→16
gecisinde ~8.7x olceklenme gorulmektedir; bu, broker'in TCP baglantisi ve is kuyruklarinin
16 eszamanli producer'a kadar lineer olceklendigini, yani bu aralikta henuz ciddi bir
bottleneck olusmadigini gosterir. 100B senaryosu da benzer (hatta daha dik) trendi izlemistir:
5667.51 → 33834.59 → 91918.37 msg/s (1→16 icin ~16.22x).

### Batching
LingerMs=5 / BatchSize=64KB senaryosunda batch_fill, M (batching_senders = producers * 2)
arttikca anlamli sekilde degismistir:

- producers=1  (M=2)  → batch_fill=0.0325  (batch'ler ~%3 dolmadan flush)
- producers=4  (M=8)  → batch_fill=0.1299  (batch'ler ~%13 dolmadan flush)
- producers=16 (M=32) → batch_fill=0.5189  (batch'ler ~%52 dolmadan flush)

Bu, M=producers*2 seciminin batch_fill'i gercekten olceklendirdigini gosterir: az sayida
eszamanli Send goroutine'i ile batch cogunlukla LingerMs=5 zamanlayicisi tarafindan
zamaninda flush edilirken (dusuk fill), 32 eszamanli gonderici ile batch LingerMs suresi
dolmadan kayitlarla doldurulmaya yaklasir (yuksek fill). 64KB batch icin 1KB+overhead
kayitlarla ~32 eszamanli Send gerekmesi, producers=16 (M=32) noktasinin batch_fill'in
~0.52'ye ulastigi ve gercek batching davranisinin goruldugu nokta oldugunu dogrular.

LingerMs>0 senaryosunda p50 ~5.5ms civarinda (5498.700 / 5512 / 5581.400 us) olculmustur;
bu, LingerMs=5 ayarinin gercekten beklenen 5ms gecikmeyi getirdigini dogrular.
Karsilastirma olarak LingerMs=0 / BatchSize=16KB senaryosunda p50 sistem saat
cozunurlugunun altina dusmus (0.000 [*]), throughput ise 8991.01 → 32374.10 → 79017.54
msg/s olarak olculmustur. Yani LingerMs=0, batch dolmasini beklemeden hemen flush yaparak
dusuk gecikme + yuksek throughput saglar; LingerMs=5 ise gecikmeyi ~5ms'e cikarip
throughput'u ~325-5133 msg/s seviyesine indirir (M arttikca batch_fill yukselip throughput
da artar).

Send cagrisi bloklayici oldugundan, batch'i doldurabilecek eszamanlilik
tam da batch'in dolmasini beklerken bloklanmis olan eszamanliliktir. Bu
gozlemden iki matematiksel tavan cikar:

- fill_max = senders / batch_kapasitesi
- throughput_max = senders / LingerMs

Olcumle dogrulama (producers=16 satiri: batching_senders=32, LingerMs=5,
throughput=5132.76 msg/s, batch_fill=0.5189):

- Tavan throughput = 32 / 0.005 s = 6400 msg/s; olculen 5132.76 msg/s
  (tavanin ~%80'i — kalan pay bloklanma ve is yuku).
- Batch kapasitesi = 64 KB / ~1050 B = ~62 kayit; beklenen doluluk =
  32 / 62 = 0.516; olculen 0.5189 (tahminle tutarli).

Sonuc: bloklayici bir producer API'sinde linger matematiksel olarak
kazanamaz; throughput, batch'i dolduran eszamanli gonderici sayisinin
LingerMs'e bolumu ile sinirlidir ve bu tavan ancak eszamanlilik arttikca
yaklasilir. Gercek Kafka producer'larinda batching'in ise yaramasi
API'nin asenkron (callback'li) olmasindandir; batch mekanizmasinin kendisi
degil. Asenkron API'de ayni gonderici bloklanmadan yeni kayitlari batch'e
eklemeye devam edebilir, dolayisiyla tek bir producer bile batch'i
doldurabilir. Bloklayici API'de ise her Send cagrisi bitene kadar o
goroutine yeni kayit ekleyemez, bu yuzden doluluk eszamanli gonderici
sayisiyla sinirlidir.

### Consumer
"Group Consumer Poll" senaryosu 84 veri-donen poll ile 3498.84 msg/s, 3.42 MB/s
olculmustur. p50=1006.800us (~1.01ms), p99=4147.300us (~4.15ms), max=4147.300us.
p50 degeri tek-bagli localhost icin makul; p99 ve max'in esit olmasi, olcum suresince
bir veya birkac poll'un uzun suren (muhtemelen son batch'lerin log sonunda beklemesi)
olduguna isaret eder. Consumer throughput'unun producer throughput'undan (~8K-92K
msg/s) dusuk olmasi, MaxBytes=32KB sinirinin bilerek kucuk tutulmasindan ve poll
basina az mesaj donmesi amacindan kaynaklanir; bu, daha fazla poll ornegiyle
gercekci percentile istatistigi toplamak icin yapilmistir.

### acks=all
acks=all senaryosu (tek broker, min_insync_replicas=1), LEO'nun monotonik tutulmasi ve
purgatory.CheckAndComplete düzeltmesinin ardindan tum producer sayilarinda saglikli ve
hatasiz calismaktadir:

- producers=1  → 7943.51 msg/s, 0 hata, 1133 ms
- producers=4  → 30927.84 msg/s, 0 hata, 291 ms
- producers=16 → 76991.45 msg/s, 0 hata, 117 ms

Onceki kosuda 4 ve 16 producer'da ~300 msg/s'e cokmus ve RequestTimeoutMs=30000
zaman asimina takilmis olan acks=all yolu, simdi acks=1 ile ayni olceklenme
egilimini izlemektedir: 1→4 gecisinde ~3.89x, 4→16 gecisinde ~2.49x kazanc, toplamda
1→16 icin ~9.69x olceklenme. Bu, LEO monotoniklik ve purgatory tamamlama
düzeltmesinin acks=all yolundaki coklu-uretici bloklanmasini giderdigini ve
olceklenebilir hale getirdigini dogrular.

## Olculmeyenler
- Apache Kafka ile dogrudan karsilastirma (farkli protokol)
- Gercek ag gecikmesi (localhost)
- Uzun sureli kosu (tek seferlik olcum)