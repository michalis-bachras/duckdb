include_directories(../../src/include)
include_directories(../../extension/tpch/include)

add_executable(scheduler_eval scheduler_eval.cpp)
target_link_libraries(scheduler_eval duckdb_static ${DUCKDB_EXTRA_LINK_FLAGS})
link_threads(scheduler_eval "")
link_extension_libraries(scheduler_eval "")
