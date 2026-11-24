# Changelog

## [Unreleased]

### Added

- Measure Python line and branch coverage and run the test suite with the GIL disabled on Linux.
- Publish verified release distributions to PyPI with GitHub Actions Trusted Publishing.

### Changed

- Expand the reviewed public API contract.

### Fixed

- Correct the bilingual guides for stream batching, awaitable checks, and pipeline observers.

## [v1.0.0] - 2026-11-23

After FlowEvent's [v1.4.2](https://github.com/TahsinCr/python-flowevent/releases/tag/v1.4.2) release, its pipelines and async streams became FlowPipeline. The [source at v1.4.2](https://github.com/TahsinCr/python-flowevent/tree/v1.4.2) is in the `TahsinCr/python-flowevent` repository, where these APIs shipped under the `eventflow` import. `flowpipeline` is developed and versioned independently.

### Added

- Expose the package version as `flowpipeline.__version__`.
- Provide synchronous and asynchronous pipelines with named stages, middleware, and execution observers in `flowpipeline`.
- Retain async stream transformations, buffering, and batching in the same package.
- Allow applications to supply a correlation ID provider without depending on another library.
- Add a standalone test runner, reproducible benchmarks, and GitHub Actions checks for the package.

### Changed

- Explain when to use count-based batches, timed batches, and bounded stream buffering.
