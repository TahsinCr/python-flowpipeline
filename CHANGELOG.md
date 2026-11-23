# Changelog

## [Unreleased]

After FlowEvent's [v1.4.2](https://github.com/TahsinCr/python-flowevent/releases/tag/v1.4.2) release, its pipelines and async streams became FlowPipeline. The [source at v1.4.2](https://github.com/TahsinCr/python-flowevent/tree/v1.4.2) is in the `TahsinCr/python-flowevent` repository, where these APIs shipped under the `eventflow` import. `flowpipeline` is developed and versioned independently.

### Added

- Provide synchronous and asynchronous pipelines with named stages, middleware, and execution observers in `flowpipeline`.
- Retain async stream transformations, buffering, and batching in the same package.
- Allow applications to supply a correlation ID provider without depending on another library.
- Add a standalone test runner, reproducible benchmarks, and GitHub Actions checks for the package.

### Changed

- Explain when to use count-based batches, timed batches, and bounded stream buffering.
