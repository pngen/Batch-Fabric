# Examples: each is a small, self-contained program demonstrating a Batch Fabric
# capability. All link BatchFabric and runnable when built.
set(BF_EXAMPLES
    basic_batching
    compatibility
    latency_bounded_batch
    tenant_fairness
    prefill_decode
    split_merge
    cancellation
    partial_failure_retry
    persistence_recovery
    explain_batch)
foreach(e ${BF_EXAMPLES})
  add_executable(${e} ${e}/main.cpp)
  target_link_libraries(${e} PRIVATE BatchFabric)
  target_include_directories(${e} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
endforeach()
