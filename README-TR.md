[![CI durumu][ci-shield]][ci]
[![PyPI sürümü][pypi-shield]][pypi]
[![Python 3.10+][python-shield]][python]
[![MIT Lisansı][license-shield]][license]
<br>
[English][readme-en] | **Türkçe**

# FlowPipeline

[Repository][repository] · [Changelog][changelog] · [MIT License][license]

Python uygulamaları için birleştirilebilir işleme aşamaları ve asenkron akışlar. Dönüşümleri sıraya koyup senkron veya asenkron çalıştırabilirsiniz.

> [Türkçe belgeler](docs/tr/README.md) mimari tercihleri, aşama sıralamayı, middleware kurallarını, `AsyncStream` geri basıncını ve API başvurusunu kapsar. [English documentation](docs/en/README.md) bağlantısı da mevcuttur.

## İçindekiler

- [Başlangıç](#getting-started)
- [Temel kavramlar](#core-concepts)
- [Pipeline ve middleware](#pipelines)
- [Asenkron pipeline ve akışlar](#streams)
- [Geliştirme ve lisans](#development)

<a id="getting-started"></a>

## Başlangıç

### Kurulum

Proje kökünde Python 3.10 veya üzeriyle kurun.

~~~sh
python -m pip install .
~~~

### İsimli aşamaları sırayla çalıştırın

Her aşama bir önceki aşamanın sonucunu alır. Fonksiyonları dekoratörle kaydedebilir veya mevcut fonksiyonları isim vererek ekleyebilirsiniz.

~~~python
from flowpipeline import Pipeline

pipeline = Pipeline[str]()


@pipeline.stage("trim")
def trim(value: str) -> str:
    return value.strip()


pipeline.add("normalize", str.lower)
assert pipeline.run("  Ready  ") == "ready"
~~~

<a id="core-concepts"></a>

## Temel kavramlar

Senkron aşamalar için `Pipeline`, coroutine aşamaları için `AsyncPipeline` kullanın. Veri akış hâlinde geliyorsa veya sınırlı bir tamponla parça parça işlenmesi gerekiyorsa `AsyncStream` tercih edin.

<a id="pipelines"></a>

## Pipeline ve middleware

### Yürütmeyi sarmalayın ve ölçüm toplayın

Middleware, kalan aşamaların öncesinde ve sonrasında çalışabilir. Bu örnekte normalleştirme öncesinde varsayılan bir değer kullanılır ve gözlemci yürütmeyi kaydeder.

~~~python
from flowpipeline import Pipeline, PipelineStats

stats = PipelineStats()
pipeline = Pipeline[str](observer=stats)
pipeline.add("normalize", str.lower)


@pipeline.middleware("default")
def supply_default(value, next_):
    return next_(value or "UNKNOWN")


assert pipeline.run("") == "unknown"
assert stats.snapshot.runs.successful == 1
~~~

<a id="streams"></a>

## Asenkron pipeline ve akışlar

### Veriyi işleyip gruplar hâlinde tüketin

Akış, veriyi parça parça okur. Her öğeyi asenkron bir pipeline’dan geçirebilir, sonuçları filtreleyebilir ve kalan öğeleri gruplar hâlinde sunabilir.

~~~python
import asyncio

from flowpipeline import AsyncPipeline, AsyncStream


async def source():
    for value in range(4):
        yield value


async def main() -> None:
    pipeline = AsyncPipeline[int]()

    @pipeline.stage("double")
    async def double(value: int) -> int:
        return value * 2

    stream = AsyncStream(source()).map(pipeline.run).filter(lambda value: value > 0).batch(2)
    async with stream:
        assert [batch async for batch in stream] == [(2, 4), (6,)]


asyncio.run(main())
~~~

Öğeleri sayıya göre gruplamak için `batch(size)` kullanın. Grubun dolmasını en fazla belirli bir süre beklemek istiyorsanız `batch_within(size, delay)` kullanın. Üretici ile tüketicinin bağımsız ilerlemesi fayda sağlıyorsa `buffer(capacity)` ekleyin. Kaynak bellekteyse ve tüketici hızlıysa kuyruk üzerinden aktarım ek maliyet getirebilir.

<a id="development"></a>

## Geliştirme ve lisans

~~~sh
python -m pip install -e ".[dev]"
python run_tests.py
~~~

Proje [MIT Lisansı][license] ile yayımlanır.

## Diğer projeler

[FlowEvent][repo-flowevent] · [FlowCoord][repo-flowcoord]

[ci-shield]: https://img.shields.io/github/actions/workflow/status/TahsinCr/python-flowpipeline/ci.yml?branch=main&event=push&label=CI&style=for-the-badge&logo=github&logoColor=white
[pypi-shield]: https://img.shields.io/pypi/v/flowpipeline?label=PyPI&style=for-the-badge&logo=pypi&logoColor=white
[python-shield]: https://img.shields.io/badge/Python-3.10%2B-3776AB.svg?style=for-the-badge&logo=python&logoColor=white
[license-shield]: https://img.shields.io/badge/license-MIT-blue.svg?style=for-the-badge
[ci]: https://github.com/TahsinCr/python-flowpipeline/actions/workflows/ci.yml?query=branch%3Amain+event%3Apush
[pypi]: https://pypi.org/project/flowpipeline/
[python]: https://www.python.org/downloads/
[readme-en]: https://github.com/TahsinCr/python-flowpipeline/blob/main/README.md
[repository]: https://github.com/TahsinCr/python-flowpipeline
[changelog]: https://github.com/TahsinCr/python-flowpipeline/blob/main/CHANGELOG.md
[license]: https://github.com/TahsinCr/python-flowpipeline/blob/main/LICENSE
[repo-flowevent]: https://github.com/TahsinCr/python-flowevent
[repo-flowcoord]: https://github.com/TahsinCr/python-flowcoord
