# Contributing to Batch Fabric

Thank you for contributing to Batch Fabric.

## Build

Prerequisites: a C++20 compiler (MSVC, GCC, or Clang), CMake 3.24+, Ninja, and
optionally CUDA 12.9+ for the GPU executor.

```
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build
ctest --test-dir build
```

On Windows, configure from a Visual Studio developer prompt (or source
`vcvars64.bat`) so the MSVC toolchain and CUDA are on the path.

## Tests

Run the full suite with:

```
ctest --test-dir build --output-on-failure
```

The multiprocess validation harness runs as a CTest test and launches real
coordinator and worker OS processes over framed TCP.

## Contribution guidance

- Batch Fabric accepts contributions from individuals and organizations on the
  terms of the Apache License 2.0. No Contributor License Agreement (CLA) is
  required.
- Keep the core runtime vendor-neutral and workload-neutral. Batch Fabric owns
  dynamic inference batch formation and batch lifecycle; it is not a model
  server, scheduler, tokenizer, or CUDA kernel library.
- Prefer explicit `Result`/`Error` flow over exceptions for normal control flow.
- Strong types over raw strings. Add or extend typed identities rather than
  scattering ad hoc string comparisons.
- New public API must be documented, tested, and kept deterministic.
- All new code must build clean under `/W4 /WX` on MSVC and the configured
  warning-as-error settings on other compilers.
- Add a test for every new behavior. No test may rely on a wall-clock timeout;
  use the simulated monotonic clock for deterministic timing tests.

## Submitting changes

Open a pull request describing the change, the problem it solves, and the
validation run. Please keep the change focused and coherent with the existing
design.
