cmake_minimum_required(VERSION 3.24)

project(BatchFabric VERSION 1.0.0 LANGUAGES CXX CUDA
        DESCRIPTION "Open-source, vendor-neutral runtime for dynamic inference batching")

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Blackwell RTX 5090 is compute capability 12.0 (sm_120). Default to it; the
# operator may override with -DBF_CUDA_ARCHITECTURES=...
set(BF_CUDA_ARCHITECTURES "120" CACHE STRING "CUDA architectures to target")

option(BATCH_FABRIC_BUILD_TESTS "Build Batch Fabric tests" ON)
option(BATCH_FABRIC_BUILD_EXAMPLES "Build Batch Fabric examples" ON)
option(BATCH_FABRIC_BUILD_BENCHMARKS "Build Batch Fabric benchmarks" ON)
option(BATCH_FABRIC_BUILD_CUDA "Build the CUDA-backed executor" ON)
option(BATCH_FABRIC_BUILD_CLI "Build the batchfabric CLI" ON)
option(BATCH_FABRIC_BUILD_MULTIPROCESS "Build the multiprocess validation harness" ON)

if(BATCH_FABRIC_BUILD_CUDA)
  message(STATUS "Batch Fabric: CUDA executor enabled (architectures=${BF_CUDA_ARCHITECTURES})")
endif()

# Core library sources. Globbed so incremental development is frictionless;
# public headers are installed from include/batch_fabric by explicit directory.
file(GLOB BATCH_FABRIC_CORE_SOURCES CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")
file(GLOB BATCH_FABRIC_CUDA_SOURCES CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/src/executor/*.cu")

add_library(BatchFabric STATIC ${BATCH_FABRIC_CORE_SOURCES})

target_include_directories(BatchFabric PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

target_compile_definitions(BatchFabric PUBLIC BATCH_FABRIC_VERSION="${PROJECT_VERSION}")

if(MSVC)
  # Host (CXX) warnings-as-errors, restricted to CXX so they are never passed
  # to nvcc as bare host flags; the CUDA unit gets its own -Xcompiler options.
  target_compile_options(BatchFabric PRIVATE $<$<COMPILE_LANGUAGE:CXX>:/W4 /WX /EHsc>)
  target_compile_options(BatchFabric PUBLIC $<$<COMPILE_LANGUAGE:CXX>:/W4>)
  add_compile_definitions(_CRT_SECURE_NO_WARNINGS)
else()
  target_compile_options(BatchFabric PRIVATE -Wall -Wextra -Wpedantic -Werror)
  target_compile_options(BatchFabric PUBLIC -Wall -Wextra)
endif()

# CUDA executor.
if(BATCH_FABRIC_BUILD_CUDA AND BATCH_FABRIC_CUDA_SOURCES)
  target_sources(BatchFabric PRIVATE ${BATCH_FABRIC_CUDA_SOURCES})
  set_target_properties(BatchFabric PROPERTIES
      CUDA_STANDARD 17
      CUDA_ARCHITECTURES ${BF_CUDA_ARCHITECTURES})
  find_package(CUDAToolkit REQUIRED)
  target_link_libraries(BatchFabric PUBLIC CUDA::cudart)
  target_compile_options(BatchFabric PRIVATE
      $<$<COMPILE_LANGUAGE:CUDA>:-Xcompiler=/W4;-Xcompiler=/WX;-Xcompiler=/EHsc;-Xcompiler=/wd4211>)
endif()

# Threads / sockets.
if(WIN32)
  target_link_libraries(BatchFabric PUBLIC ws2_32)
else()
  find_package(Threads REQUIRED)
  target_link_libraries(BatchFabric PUBLIC Threads::Threads)
endif()

# CLI
# Distributed control-plane binaries.
add_executable(bf_coordinator app/coordinator.cpp)
target_link_libraries(bf_coordinator PRIVATE BatchFabric)
target_include_directories(bf_coordinator PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)

add_executable(bf_worker app/worker.cpp)
target_link_libraries(bf_worker PRIVATE BatchFabric)
target_include_directories(bf_worker PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)

if(BATCH_FABRIC_BUILD_CLI AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/app/cli.cpp")
  add_executable(batchfabric app/cli.cpp)
  target_link_libraries(batchfabric PRIVATE BatchFabric)
  target_include_directories(batchfabric PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
endif()

# Sub-builds (guarded so configure succeeds while modules are built up)
if(BATCH_FABRIC_BUILD_TESTS AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/CMakeLists.txt")
  enable_testing()
  add_subdirectory(tests)
endif()
if(BATCH_FABRIC_BUILD_EXAMPLES AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/examples/CMakeLists.txt")
  add_subdirectory(examples)
endif()
if(BATCH_FABRIC_BUILD_BENCHMARKS AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/CMakeLists.txt")
  add_subdirectory(benchmarks)
endif()

# Install / export / package
include(GNUInstallDirs)
install(TARGETS BatchFabric
        EXPORT BatchFabricTargets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(EXPORT BatchFabricTargets
        FILE BatchFabricTargets.cmake
        NAMESPACE BatchFabric::
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/BatchFabric)

include(CMakePackageConfigHelpers)
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/BatchFabricConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion)
configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/BatchFabricConfig.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/BatchFabricConfig.cmake"
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/BatchFabric)
install(FILES
        "${CMAKE_CURRENT_BINARY_DIR}/BatchFabricConfig.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/BatchFabricConfigVersion.cmake"
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/BatchFabric)