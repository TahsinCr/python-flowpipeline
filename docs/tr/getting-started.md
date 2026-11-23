[← Belge Haritası](README.md) · [Sonraki: 02 · Pipeline, Middleware ve Gözlemlenebilirlik →](pipelines-and-middleware.md)

# 01 · Başlangıç ve Mimari Felsefe

Bu rehber `flowpipeline` kurulumunu, kütüphanenin hangi gerçek mühendislik problemlerini çözmek için tasarlandığını, mimari tercihlerini ve ilk senkron/asenkron örnekleri anlatır.

---

## Konu Dizini

- [Kurulum](#kurulum)
- [Problem: Düz Fonksiyon Zincirleri Neden Zamanla Kırılır?](#problem-düz-fonksiyon-zincirleri-neden-zamanla-kırılır)
- [Mimari Tercihler ("Neden Böyle Tasarlandı?")](#mimari-tercihler-neden-böyle-tasarlandı)
  - [Tek-Tip Dönüşüm (`Pipeline[T]`)](#tek-tip-dönüşüm-pipelinet)
  - [Derlenmiş Değişmez Plan (Snapshot Compilation)](#derlenmiş-değişmez-plan-snapshot-compilation)
  - [Senkron ve Asenkron Ayrımı](#senkron-ve-asenkron-ayrımı)
  - [Ne Zaman Kullanmamalısın?](#ne-zaman-kullanmamalısın)
- [İlk Senkron Pipeline Örneği](#i%CC%87lk-senkron-pipeline-örneği)
- [İlk Asenkron Pipeline + Stream Örneği](#i%CC%87lk-asenkron-pipeline--stream-örneği)

---

## Kurulum

`flowpipeline` tamamen saf Python ile yazılmıştır; **Python 3.10 veya üzeri** gerektirir ve harici bir çalışma zamanı bağımlılığı yoktur.

```bash
# PyPI üzerinden kurulum
python -m pip install flowpipeline

# Depo kökünden geliştirme kurulumu
python -m pip install -e .

# Test ve kalite araçlarıyla geliştirme kurulumu
python -m pip install -e ".[dev]"
```

[Yukarı dön](#konu-dizini)

---

## Problem: Düz Fonksiyon Zincirleri Neden Zamanla Kırılır?

Küçük bir uygulamada veriyi işlemek genellikle masum bir fonksiyon zinciriyle başlar:

```python
def process(payload: dict) -> dict:
    payload = validate(payload)
    payload = enrich(payload)
    payload = redact(payload)
    return payload
```

Uygulama büyüdükçe şu dört sorun ortaya çıkar:

1. **Çapraz kesen kaygılar (cross-cutting concerns) her yere bulaşır:** Her adımın süresini ölçmek, korelasyon kimliği eklemek, belirli bir koşulda kısa devre (cache hit) yapmak veya hata olduğunda **hangi adımın neden çöktüğünü** bilmek için fonksiyon içini `try/except` ve `perf_counter()` bloklarıyla boğarsın.
2. **Sıralama kırılganlaşır:** Bir eklenti veya yeni özellik "validasyon sonrası, zenginleştirme öncesi" araya girmek istediğinde tüm ana fonksiyonu açıp elle yer değiştirmek gerekir.
3. **Eşzamanlı yapılandırma değişikliği risklidir:** Bir istek çalışırken bir adım geçici olarak devre dışı bırakılırsa veya değiştirilirse, yarıda kalmış bir yürütme tutarsız bir aşama listesi görebilir.
4. **Asenkron akışlarda sızıntı olur:** Ham `async def` generator'ları `async for` ile okunurken erken `break` yapıldığında veya arka plan ön-okuma (prefetch) görevi başlatıldığında, üretici görevler ve ağ kaynakları kapatılmadan asılı kalabilir.

`flowpipeline` tam olarak bu dört soruna **ekstra bir framework yüklemeden** çözüm getirir.

[Yukarı dön](#konu-dizini)

---

## Mimari Tercihler ("Neden Böyle Tasarlandı?")

### Tek-Tip Dönüşüm (`Pipeline[T]`)

`Pipeline[T]` ve `AsyncPipeline[T]` bilerek **`T -> T`** (aynı tipte bağlam/payload dönüşümü) sözleşmesiyle tasarlanmıştır:

- Her aşama (`stage`) tek bir `context: T` nesnesi alır ve yine bir `T` döndürür (örneğin bir `OrderContext` dataclass'ı veya `dict` ya da `str`).
- **Neden her aşamadan farklı dönüş tipi (`A -> B -> C`) yok?** Çünkü çalışma anında `disable("enrich")` dediğinde veya araya yeni bir `before`/`after` adımı ya da `middleware` soktuğunda, zincirin tip uyumu bozulmamalıdır. Bir adım devre dışı bırakıldığında girdi güvenle bir sonraki adıma akabilmelidir.
- Farklı bir çıktı tipine dönüşüm gerekiyorsa, bunu akış seviyesinde `AsyncStream.map(...)` ile yapmak çok daha doğal ve tip güvenlidir.

### Derlenmiş Değişmez Plan (Snapshot Compilation)

Aşama veya ara yazılım eklediğinde (`add`, `remove`, `replace`, `enable`, `disable`), kütüphane kilidi (`RLock`) altında topolojik sıralamayı hesaplar ve o anki aktif zincir için **yeni bir immutable yürütme planı** derler:

- `run()` çağrısı kilidi beklemek yerine o anki derlenmiş planı okur ve çalışır.
- Çalışma sırasındaki (`replace`, `disable`) değişiklikler **yalnızca bir sonraki `run()` çağrılarını** etkiler; o anda yürümekte olan çalıştırmaların sırası yarıda değişmez.

### Senkron ve Asenkron Ayrımı

- `Pipeline` yalnızca senkron callback'ler kabul eder; arka planda asla gizli bir `asyncio` event loop veya thread başlatmaz. Yanlışlıkla `async def` bir fonksiyon verirsen kayıt veya çalıştırma anında `TypeError` ile erken yakalar.
- `AsyncPipeline` ise yalnızca awaitable döndüren callback'ler kabul eder ve ilk çalıştığı event loop'a bağlanır (`_loop`). Yanlışlıkla başka bir event loop'tan çalıştırılırsa `RuntimeError` fırlatır.

### Ne Zaman Kullanmamalısın?

- **Dağıtık DAG / iş akışı motoru değildir:** Adımların makine çökse bile diskten devam etmesi (durable execution) gerekiyorsa Temporal, Prefect veya Celery kullanmalısın.
- **Basit tek satır:** Çalışma anı değişikliği, middleware veya gözlem gerektirmeyen 2 sabit fonksiyonu birleştirmek için doğrudan Python fonksiyon çağrısı daha hızlı ve okunaklıdır.

[Yukarı dön](#konu-dizini)

---

## İlk Senkron Pipeline Örneği

```python
from dataclasses import dataclass, replace
from flowpipeline import Pipeline, PipelineStats


@dataclass(frozen=True)
class RequestContext:
    text: str
    normalized: bool = False


stats = PipelineStats()
pipeline = Pipeline[RequestContext](observer=stats)


@pipeline.stage("trim")
def trim(ctx: RequestContext) -> RequestContext:
    return replace(ctx, text=ctx.text.strip())


@pipeline.stage("lowercase", after="trim")
def lowercase(ctx: RequestContext) -> RequestContext:
    return replace(ctx, text=ctx.text.lower(), normalized=True)


@pipeline.middleware("fallback")
def handle_empty(ctx: RequestContext, next_):
    if not ctx.text.strip():
        return RequestContext(text="boş", normalized=True)
    return next_(ctx)


result = pipeline.run(RequestContext("  Merhaba Flow  "))
assert result == RequestContext(text="merhaba flow", normalized=True)
assert stats.snapshot.runs.successful == 1
assert stats.snapshot.stages["lowercase"].calls == 1
```

[Yukarı dön](#konu-dizini)

---

## İlk Asenkron Pipeline + Stream Örneği

`AsyncPipeline.run` doğrudan `AsyncStream.map(...)` içine geçirilebilir:

```python
import asyncio
from flowpipeline import AsyncPipeline, AsyncStream


async def incoming_events():
    for i in range(1, 6):
        await asyncio.sleep(0)
        yield i


async def main() -> None:
    pipeline = AsyncPipeline[int]()

    @pipeline.stage("square")
    async def square(value: int) -> int:
        return value * value

    stream = AsyncStream(incoming_events()).map(pipeline.run).filter(lambda x: x > 5).batch(2)

    async with stream:
        batches = [batch async for batch in stream]

    assert batches == [(9, 16), (25,)]


asyncio.run(main())
```

[Yukarı dön](#konu-dizini)

---

**Önceki:** [Belge Haritası](README.md) · **Sonraki:** [02 · Pipeline, Middleware ve Gözlemlenebilirlik →](pipelines-and-middleware.md)
