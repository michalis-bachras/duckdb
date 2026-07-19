add_library(scheduler_workload_lib STATIC
            scheduler_workload_config.cpp
            scheduler_workload_schedule.cpp
            scheduler_workload_runner.cpp
            scheduler_workload_output.cpp)

target_include_directories(scheduler_workload_lib
                           PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
                           PRIVATE ${PROJECT_SOURCE_DIR}/third_party/mbedtls/include)

add_executable(scheduler_workload scheduler_workload.cpp)
target_link_libraries(scheduler_workload
                      scheduler_workload_lib
                      duckdb_static
                      duckdb_mbedtls
                      ${DUCKDB_EXTRA_LINK_FLAGS})
link_extension_libraries(scheduler_workload "")

add_executable(scheduler_workload_tests test_scheduler_workload.cpp)
target_link_libraries(scheduler_workload_tests
                      scheduler_workload_lib
                      duckdb_static
                      duckdb_mbedtls
                      ${DUCKDB_EXTRA_LINK_FLAGS})
link_extension_libraries(scheduler_workload_tests "")
