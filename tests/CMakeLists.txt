add_executable(test_core test_core.cpp)
target_link_libraries(test_core PRIVATE BatchFabric)
add_test(NAME test_core COMMAND test_core)

add_executable(test_randomized test_randomized.cpp)
target_link_libraries(test_randomized PRIVATE BatchFabric)
add_test(NAME test_randomized COMMAND test_randomized)

add_executable(test_adversarial test_adversarial.cpp)
target_link_libraries(test_adversarial PRIVATE BatchFabric)
add_test(NAME test_adversarial COMMAND test_adversarial)

add_executable(test_cuda test_cuda.cpp)
target_link_libraries(test_cuda PRIVATE BatchFabric)
if(BATCH_FABRIC_BUILD_CUDA)
  add_test(NAME test_cuda COMMAND test_cuda)
endif()

add_executable(test_multiprocess test_multiprocess.cpp)
target_link_libraries(test_multiprocess PRIVATE BatchFabric)
add_test(NAME test_multiprocess COMMAND test_multiprocess)
add_executable(test_persistence test_persistence.cpp)
target_link_libraries(test_persistence PRIVATE BatchFabric)
add_test(NAME test_persistence COMMAND test_persistence)

add_executable(test_bench_smoke test_bench_smoke.cpp)
target_link_libraries(test_bench_smoke PRIVATE BatchFabric)
add_test(NAME test_bench_smoke COMMAND test_bench_smoke)