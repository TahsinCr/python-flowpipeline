[← Belge Haritası](README.md) · [Önceki: 02 · Pipeline ve Middleware](pipelines-and-middleware.md) · [Sonraki: 04 · API Başvuru Kartı →](api-reference.md)

# 03 · AsyncStream, Backpressure ve Yaşam Döngüsü

Bu rehber `AsyncStream` sınıfının tembel (lazy pull) modelini, operatörlerin (`map`, `filter`, `batch`, `batch_within`, `buffer`) nasıl çalıştığını, neden her zaman `buffer()` kullanılmaması gerektiğini ve erken çıkış/iptal anlarında sızıntısız kapatma garantilerini anlatır.

---

## Konu Dizini

- [Problem: Push Akışlarda Bellek Şişmesi ve Yarım Kapanma](#problem-push-akışlarda-bellek-şişmesi-ve-yarım-kapanma)
- [Tembel Çekme (Lazy Pull) Modeli ve Operatör Birleştirme](#tembel-çekme-lazy-pull-modeli-ve-operatör-birleştirme)
- [Operatörler ve Mühendislik Takasları](#operatörler-ve-mühendislik-takasları)
  - [`map` ve `filter`](#map-ve-filter)
  - [`batch(size, *, partial=True)`](#batchsize--partialtrue)
  - [`batch_within(max_items, max_delay, *, partial=True)`](#batch_withinmax_items-max_delay--partialtrue)
  - [`buffer(capacity)` — Ne Zaman Kazandırır, Ne Zaman Yavaşlatır?](#buffercapacity--ne-zaman-kazandırır-ne-zaman-yavaşlatır)
- [Tek-Tüketici Sahipliği ve Yaşam Döngüsü (`async with`)](#tek-tüketici-sahipliği-ve-yaşam-döngüsü-async-with)
- [Üretici Hataları ve Temizlik Sırası](#üretici-hataları-ve-temizlik-sırası)

---

## Problem: Push Akışlarda Bellek Şişmesi ve Yarım Kapanma

Veritabanından satır okuyan, mesaj kuyruğundan event çeken veya LLM token'larını işleyen bir tüketici yavaşladığında, üretici taraf bunu bilmeden üretmeye devam ederse bellekte sınırsız birikme (OOM) yaşanır. İkinci klasik hata ise `async for item in gen:` döngüsünden `break` veya hata ile erken çıkıldığında `gen.aclose()` çağrılmadan çıkılması ve dosya/soket/task'ların açık kalmasıdır.

`AsyncStream`, **tüketici çekmedikçe ilerlemeyen** (pull-driven) akış kurar; tampon (`buffer`) veya zamanlı grup (`batch_within`) kullanıldığında bile kapasiteyi sıkı şekilde sınırlar.

[Yukarı dön](#konu-dizini)

---

## Tembel Çekme (Lazy Pull) Modeli ve Operatör Birleştirme

`AsyncStream(source).map(...).filter(...)` yazdığında **hiçbir arka plan görevi (task) oluşturulmaz**:

```mermaid
flowchart LR
    Src["AsyncIterable Kaynak"] --> Read["__anext__() okur"]
    Read --> Op1["operator 1: map"]
    Op1 --> Op2["operator 2: filter"]
    Op2 --> Consumer["Tüketici (async for)"]
```

- Ardışık `map` ve `filter` çağrıları tek bir kök kaynak üzerinde operatör listesi olarak birleştirilir. Ara akışlar için ekstra `asyncio.Queue` veya ekstra coroutine sarmalayıcı açılmaz; döngü maliyeti düşük kalır.
- Hem senkron fonksiyonlar (`lambda x: x * 2`) hem de asenkron fonksiyonlar (`async def fetch(x): ...`) doğrudan `map` ve `filter` içine verilebilir. (`filter` her zaman tam `bool` döndürmelidir; ör. `1` veya `"ok"` gibi truthy değerler `TypeError` ile reddedilir.)

[Yukarı dön](#konu-dizini)

---

## Operatörler ve Mühendislik Takasları

### `map` ve `filter`

- Görev (task) oluşturmaz, doğrudan okuyan görevin içinde sırayla çalışır.
- Hızlı yol, tam `int` ve `bool` sonuçları için awaitable denetimini atlar. Diğer sonuçlar normal biçimde denetlenir.

### `batch(size, *, partial=True)`

- En fazla `size` öğe biriktirip `tuple` olarak verir; görev oluşturmaz.
- Kaynak bittiğinde elde kalan eksik grubun üretilip üretilmeyeceğini `partial` belirler (`True` varsayılan).

### `batch_within(max_items, max_delay, *, partial=True)`

Veritabanına toplu yazma (bulk insert) gibi işlerde hem **"en az N öğe biriksin"** hem de **"akış yavaşsa sonsuza kadar beklemeyelim, ilk öğeden itibaren en fazla `T` saniye sonra eldekini verelim"** ihtiyacını karşılar:

- Arka planda tek bir üretici görev açar ve tek bir talep-kontrollü (demand-gated) bekleme yuvası kullanır. Böylece iç tamponlama yalnızca bir aktif grup ve uçuş halindeki bir kaynak öğesiyle sınırlanır.
- Öğe başına değil, **grup başına tek bir monotonik zamanlayıcı (`loop.call_at`)** kurar.

### `buffer(capacity)` — Ne Zaman Kazandırır, Ne Zaman Yavaşlatır?

`buffer(capacity)`, üretici ile tüketiciyi `asyncio.Queue(maxsize=capacity)` üzerinden ayırır:

- Kuyruk dolduğunda üretici `await queue.put(item)` noktasında bloklanır (**backpressure**). Hiçbir öğe düşürülmez (no drop).
- **Ne zaman kullanmalı?** Kaynak ağ/disk gecikmeli (`await` içeren I/O) ve tüketici de ayrıca I/O yapıyorsa, bir sonraki öğe arka planda önceden hazırlanabileceği (prefetch) için toplam süreyi düşürür.
- **Ne zaman kullanmamalı?** Kaynak zaten bellek içi bir liste/generator ise ve tüketici hızlıysa, her öğe için görevler arası `asyncio.Queue` geçişi yapmak işlemi hızlandırmak yerine **yavaşlatabilir**.

```python
import asyncio
from flowpipeline import AsyncStream


async def fetch_pages():
    for page in range(1, 5):
        await asyncio.sleep(0.01)  # Ağ I/O simülasyonu
        yield page


async def main() -> None:
    stream = AsyncStream(fetch_pages()).buffer(2).batch_within(3, 0.05)
    async with stream:
        batches = [b async for b in stream]
    assert sum(len(b) for b in batches) == 4


asyncio.run(main())
```

[Yukarı dön](#konu-dizini)

---

## Tek-Tüketici Sahipliği ve Yaşam Döngüsü (`async with`)

`AsyncStream` bilerek **tek-tüketici (single-consumer)** ve **tek-task (single-task)** olarak tasarlanmıştır:

- Bir akış yalnızca bir kez `async for` ile tüketilebilir; ikinci kez `aiter` çağrısı `RuntimeError("AsyncStream supports one consumption only")` verir.
- Bir `AsyncStream` nesnesini farklı task'ların eşzamanlı okuması (`asyncio.gather(anext(s), anext(s))`) `RuntimeError` ile reddedilir.
- Her akış yaratıldığı veya ilk okunduğu event loop'a bağlanır; başka loop'tan erişilemez.
- **Her zaman son akışı `async with stream:` ile sarmala:** Döngü içinden `break` ile çıksan bile `__aexit__` otomatik `await stream.aclose()` çağırır; bu da arka plan üretici görevini iptal edip temizliğin bitmesini bekler ve kaynak nesnenin `aclose()` metodunu yukarı doğru zincirleme kapatır.
- Aktif olarak `__anext__` bekleyen bir task, kendisini kapatmaya çalışırsa (`await stream.aclose()`) deadlock oluşmaması için `RuntimeError` fırlatılır; kapatma başka bir task veya `async with` bloğundan çıkışla yapılmalıdır.

[Yukarı dön](#konu-dizini)

---

## Üretici Hataları ve Temizlik Sırası

- `buffer()` veya `batch_within()` kullanırken üretici kaynak hata fırlatırsa, **kuyrukta zaten hazır bekleyen öğeler önce tüketiciye teslim edilir**, ardından sıra hataya geldiğinde kaynak hatası yükseltilir.
- Akış doğal olarak bittiğinde (`StopAsyncIteration`) bile kaynak temizliği otomatik tetiklenir.
- Kaynak hatası sırasında kapatma (`aclose()`) aşamasında ikinci bir hata oluşursa:
  - Asıl kaynak hatası korunur ve temizlik hatası zincirlenir (eğer temizlik `CancelledError` gibi bir `BaseException` ise iptal öncelik alır).
- `aclose()` tekrarlayan iptallere (`asyncio.CancelledError`) dayanıklıdır; iç temizlik görevini tamamlanana kadar korur.

[Yukarı dön](#konu-dizini)

---

**Önceki:** [02 · Pipeline, Middleware ve Gözlemlenebilirlik](pipelines-and-middleware.md) · **Sonraki:** [04 · API Başvuru Kartı →](api-reference.md)
