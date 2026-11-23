# FlowPipeline Türkçe Belgeleri

`flowpipeline`, Python uygulamaları için sıralı işleme hatları (`Pipeline`, `AsyncPipeline`) ve geri basınç (backpressure) kontrollü asenkron veri akışları (`AsyncStream`) sunan sıfır bağımlılıklı bir kütüphanedir. Aynı dönüşüm mantığını katmanlı ara yazılımlar (middleware), yerleşik ölçümler (`PipelineStats`) ve sızıntısız kaynak temizliği ile hem senkron hem asenkron kodda kurmanı sağlar.

> **Kapsam Notu:** `flowpipeline` tek bir Python süreci içindeki (`in-process`) veri dönüşümü ve akış koordinasyonu içindir. Dağıtık iş kuyruklarının (Celery, Temporal) veya makineler arası akış broker'larının (Kafka) yerine geçmez.

---

## Konu Dizini

- [Nereden Başlamalısın?](#nereden-başlamalısın)
- [Doküman Haritası](#doküman-haritası)
- [Hangi Yapıyı Seçmelisin?](#hangi-yapıyı-seçmelisin)
- [Ekosistemdeki Yeri](#ekosistemdeki-yeri)

---

## Nereden Başlamalısın?

- İlk defa kullanıyorsan [01 · Başlangıç ve Mimari Felsefe](getting-started.md) ile başla. Kurulumu, düz fonksiyon zincirlerinin neden zamanla kırıldığını ve ilk senkron/asenkron örneği görürsün.
- Aşama sıralama (`before` / `after`), ara yazılım (middleware) soğan modeli, hata ayıklama ve ölçüm toplama için [02 · Pipeline, Middleware ve Gözlemlenebilirlik](pipelines-and-middleware.md) rehberine geç.
- Büyük veya ağdan gelen veri akışlarını parça parça işlemek, gruplamak (`batch` / `batch_within`) veya önden okumak (`buffer`) istiyorsan [03 · AsyncStream, Backpressure ve Yaşam Döngüsü](async-streams-and-backpressure.md) rehberini oku.
- Kesin kurucu parametreleri, metot imzaları ve veri sınıfları için [04 · API Başvuru Kartı](api-reference.md) sayfasına bak.

[Yukarı dön](#konu-dizini)

---

## Doküman Haritası

| Doküman | İçerik |
| --- | --- |
| **[01 · Başlangıç ve Mimari Felsefe](getting-started.md)** | Kurulum, "Neden FlowPipeline?", tek-tip (`Pipeline[T]`) dönüşüm modeli, senkron/asenkron ayrımı ve mimari sınırlar. |
| **[02 · Pipeline, Middleware ve Gözlemlenebilirlik](pipelines-and-middleware.md)** | Topolojik aşama sıralaması (`before` / `after`), çalışma anı snapshot derlemesi, middleware devamlılık kuralları, `PipelineExecutionError` ve `PipelineStats`. |
| **[03 · AsyncStream, Backpressure ve Yaşam Döngüsü](async-streams-and-backpressure.md)** | Tembel (lazy) çekme modeli, operatör birleştirme, `batch` / `batch_within` / `buffer` takasları, tek-tüketici kuralları ve `async with` temizliği. |
| **[04 · API Başvuru Kartı](api-reference.md)** | Tüm public sınıflar, metot imzaları, gözlem veri tipleri, istisnalar ve teknik limitler. |

[Yukarı dön](#konu-dizini)

---

## Hangi Yapıyı Seçmelisin?

| İhtiyacın | Doğru Araç | Neden? |
| --- | --- | --- |
| Tek bir girdi üzerinde senkron dönüşüm adımları çalıştırmak | `Pipeline[T]` | Arka planda gizli `asyncio` döngüsü veya thread başlatmaz; derlenmiş snapshot üzerinde doğrudan çalışır. |
| A/B aşamalarını isimlendirip çalışma anında değiştirmek/kapatmak ve etrafına log/süre sarmalamak | `Pipeline` + `middleware` + `PipelineStats` | Kodun çağrı zincirini dağıtmadan `replace()`, `disable()`, `before`/`after` ve `PipelineObserver` eklemeni sağlar. |
| Coroutine (`async def`) aşamalarıyla I/O ağırlıklı dönüşüm yapmak | `AsyncPipeline[T]` | İptali (`asyncio.CancelledError`) doğrudan aktif aşamaya iletir; aynı anda birden fazla `run()` çağrısı birbirinin middleware durumunu bozmaz. |
| Sonsuz veya büyük asenkron akışları belleğe yüklemeden dönüştürmek, filtrelemek ve gruplamak | `AsyncStream[T]` | Tembel (pull-tabanlı) çalışır; `map`/`filter` için ekstra görev açmaz, `buffer` ve `batch_within` ile belleği sınırlar. |
| Çok küçük, hiç değişmeyen 2–3 saf fonksiyonu birleştirmek | Düz Python fonksiyonu `f(g(x))` | Çalışma anı yeniden yapılandırması, middleware veya ölçüm gerekmiyorsa ekstra soyutlama eklemeye gerek yoktur. |

[Yukarı dön](#konu-dizini)

---

## Ekosistemdeki Yeri

Flow üç bağımsız paketten oluşur. `flowpipeline` diğer iki pakete **bağımlı değildir**; ancak büyük uygulamalarda şu şekilde birlikte çalışırlar:

```mermaid
flowchart LR
    FE["flowevent\nOlay Otobüsü & Köprüler"] -->|"gelen olayları işle"| FP["flowpipeline\nPipeline & AsyncStream"]
    FP -->|"aşamaları retry / rate limit ile koru"| FC["flowcoord\nDayanıklılık & Eşzamanlılık"]
```

- Olay tabanlı dağıtım (pub/sub) gerekiyorsa: **FlowEvent**
- Yeniden deneme (retry), devre kesici (circuit breaker), anahtar bazlı hız/eşzamanlılık sınırlama gerekiyorsa: **FlowCoord**
- Veri akışı, katmanlı aşama ve ara yazılım gerekiyorsa: **FlowPipeline**

[Yukarı dön](#konu-dizini)

---

**Sonraki:** [01 · Başlangıç ve Mimari Felsefe →](getting-started.md)
