[← Belge Haritası](README.md) · [Önceki: 03 · AsyncStream ve Backpressure](async-streams-and-backpressure.md)

# 04 · FlowPipeline API Başvuru Kartı

Bu sayfa `flowpipeline` paketinden dışa aktarılan tüm public sınıf, metot, veri yapısı, protokol ve istisnaları özetler.

---

## Konu Dizini

- [Paket Dışa Aktarımları](#paket-dışa-aktarımları)
- [`Pipeline[T]` ve `AsyncPipeline[T]`](#pipelinet-ve-asyncpipelinet)
- [`AsyncStream[T]`](#asyncstreamt)
- [Gözlemlenebilirlik Türleri](#gözlemlenebilirlik-türleri)
- [Hata Sınıfı (`PipelineExecutionError`)](#hata-sınıfı-pipelineexecutionerror)

---

## Paket Dışa Aktarımları

```python
from flowpipeline import (
    AsyncPipeline,
    AsyncStream,
    ExecutionMetrics,
    Pipeline,
    PipelineExecutionError,
    PipelineObservation,
    PipelineObserver,
    PipelineStats,
    PipelineStatsSnapshot,
    __version__,
)
```

[Yukarı dön](#konu-dizini)

---

## `Pipeline[T]` ve `AsyncPipeline[T]`

Her iki sınıf da aynı kayıt, sıralama ve gözlem sözleşmesini paylaşır; yalnızca yürütme modu (`def` vs `async def` / `run()` vs `await run()`) farklıdır.

### Kurucu

| Parametre | Tip | Varsayılan | Açıklama |
| --- | --- | --- | --- |
| `observer` | `PipelineObserver \| None` | `None` | Her run, stage ve middleware sonunda çağrılan hızlı **senkron** gözlemci. Coroutine olamaz (`TypeError`). |
| `correlation_provider` | `Callable[[], str \| None] \| None` | `None` | Her `PipelineObservation.correlation_id` alanı için çağrılan **senkron** fonksiyon. |

### Özellikler ve Metotlar

| Üye | İmza / Tip | Açıklama |
| --- | --- | --- |
| `stage_names` | `tuple[str, ...]` | Devre dışı olanlar dahil tüm aşama isimleri (yürütme sırasıyla). |
| `middleware_names` | `tuple[str, ...]` | En dıştan en içe tüm middleware isimleri (devre dışı olanlar dahil). |
| `add` | `(name: str, callback, *, before: str \| None = None, after: str \| None = None) -> None` | İsimli aşamayı atomik olarak ekler. |
| `stage` | `(name: str, *, before: str \| None = None, after: str \| None = None) -> Callable` | Aşama kayıt dekoratörü; orijinal fonksiyonu döndürür. |
| `replace` | `(name: str, callback) -> None` | Aşama fonksiyonunu konumunu ve aktiflik durumunu koruyarak değiştirir. |
| `enable` / `disable` | `(name: str) -> None` | Aşamayı sonraki çalıştırmalar için açar / kapatır. |
| `remove` | `(name: str) -> callback` | Aşamayı ve ona olan sıralama referanslarını siler; eski callback'i döndürür. |
| `add_middleware` | `(name: str, callback, *, before=None, after=None) -> None` | En fazla **64** ara yazılım ekler; `callback(context, next_)` imzasını alır. |
| `middleware` | `(name: str, *, before=None, after=None) -> Callable` | Ara yazılım kayıt dekoratörü. |
| `replace_middleware` | `(name: str, callback) -> None` | Konumu koruyarak ara yazılımı değiştirir. |
| `enable_middleware` / `disable_middleware` | `(name: str) -> None` | Ara yazılımı açar / kapatır. |
| `remove_middleware` | `(name: str) -> callback` | Ara yazılımı silip eski callback'i döndürür. |
| `run` | `Pipeline.run(context: T) -> T` / `AsyncPipeline.run(context: T) -> Awaitable[T]` | Güncel derlenmiş planı çalıştırır. |

[Yukarı dön](#konu-dizini)

---

## `AsyncStream[T]`

`AsyncIterator[T]` arayüzünü uygular; `async with` ve `async for` destekler.

| Üye | İmza / Tip | Açıklama |
| --- | --- | --- |
| `AsyncStream(source)` | `source: AsyncIterable[T]` | Yeni akış oluşturur (`AsyncIterable` değilse `TypeError`). |
| `closed` | `bool` | Akışın kapatılıp yeni okumaya kapalı olup olmadığı. |
| `map(callback)` | `(Callable[[T], U \| Awaitable[U]]) -> AsyncStream[U]` | Her öğeyi dönüştürür (task oluşturmaz). |
| `filter(predicate)` | `(Callable[[T], bool \| Awaitable[bool]]) -> AsyncStream[T]` | Yalnızca tam `bool` `True` döndüren öğeleri geçirir. |
| `batch(size, *, partial=True)` | `size: int > 0, partial: bool` | Sabit boyutlu `tuple` gruplar üretir (task oluşturmaz). |
| `batch_within(max_items, max_delay, *, partial=True)` | `max_items: int > 0, max_delay: float > 0` | Grup dolduğunda veya ilk öğeden itibaren `max_delay` sn geçince demet yayınlar. |
| `buffer(capacity)` | `capacity: int > 0` | En fazla `capacity` kuyruk + 1 uçuş öğesiyle arka planda ön-okuma yapar. |
| `aclose()` | `async def aclose() -> None` | Aktif okumayı iptal eder, üretici görevi ve kaynak zincirini kapatır. |

[Yukarı dön](#konu-dizini)

---

## Gözlemlenebilirlik Türleri

| Tür | Alanlar / Metotlar | Açıklama |
| --- | --- | --- |
| `PipelineObservation` | `kind` (`"run" \| "stage" \| "middleware"`), `name: str \| None`, `elapsed: float`, `status` (`"success" \| "error" \| "cancelled"`), `correlation_id: str \| None`, `error: BaseException \| None` | Tek bir tamamlanmış yürütme olayının donmuş (`frozen=True, slots=True`) kaydı. |
| `PipelineObserver` | `__call__(observation: PipelineObservation) -> None` | Senkron gözlemci protokolü. |
| `ExecutionMetrics` | `calls`, `successful`, `failed`, `cancelled`, `total_seconds`, `max_seconds` | Toplu sayım ve süre istatistikleri (`frozen=True, slots=True`). |
| `PipelineStatsSnapshot` | `runs: ExecutionMetrics`, `stages: Mapping[str, ExecutionMetrics]`, `middleware: Mapping[str, ExecutionMetrics]` | Değiştirilemez (`MappingProxyType`) anlık görünüm. |
| `PipelineStats` | `PipelineStats()`, `.snapshot`, `.reset()` | Thread-safe dahili metrik toplayıcı. |

[Yukarı dön](#konu-dizini)

---

## Hata Sınıfı (`PipelineExecutionError`)

`RuntimeError` alt sınıfıdır. Bir `stage`, `middleware`, `run` veya `observer` yürütmesi standart bir `Exception` fırlattığında yükselir (orijinal hata `__cause__` olarak korunur).

| Alan | Tip | Açıklama |
| --- | --- | --- |
| `name` | `str \| None` | Hatanın çıktığı stage/middleware adı (`run` aşamasında `None`). |
| `phase` | `Literal["stage", "middleware", "run", "observer"]` | Hatanın gerçekleştiği katman. |
| `context` | `object` | Hata anında işlenmekte olan bağlam nesnesi. |
| `observer_errors` | `tuple[Exception, ...]` | Ana hata işlenirken gözlemcinin ayrıca fırlattığı hatalar (orijinal hatayı gizlemez). |

[Yukarı dön](#konu-dizini)

---

**Önceki:** [03 · AsyncStream, Backpressure ve Yaşam Döngüsü](async-streams-and-backpressure.md) · **Harita:** [Belge Haritası](README.md)
