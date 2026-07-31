set(DUCKDB_ENERGY_ATTRIBUTION_SOURCES energy_attribution.cpp
                                      energy_attribution_base_power.cpp
                                      process_activity.cpp)

if(BUILD_EBPF_PROCESS_ATTRIBUTION)
  if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "BUILD_EBPF_PROCESS_ATTRIBUTION requires Linux")
  endif()
  find_program(DUCKDB_BPF_CLANG clang REQUIRED)
  find_program(DUCKDB_BPFTOOL bpftool REQUIRED)
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(DUCKDB_LIBBPF REQUIRED IMPORTED_TARGET libbpf)
  find_package(ZLIB REQUIRED)
  find_library(DUCKDB_ELF_LIBRARY elf REQUIRED)

  set(DUCKDB_BPF_GENERATED_DIR
      "${CMAKE_CURRENT_BINARY_DIR}/process_activity_generated")
  set(DUCKDB_BPF_VMLINUX "${DUCKDB_BPF_GENERATED_DIR}/vmlinux.h")
  set(DUCKDB_BPF_OBJECT "${DUCKDB_BPF_GENERATED_DIR}/process_activity.bpf.o")
  set(DUCKDB_BPF_SKELETON
      "${DUCKDB_BPF_GENERATED_DIR}/process_activity.skel.h")

  add_custom_command(
    OUTPUT "${DUCKDB_BPF_VMLINUX}"
    COMMAND ${CMAKE_COMMAND} -E make_directory "${DUCKDB_BPF_GENERATED_DIR}"
    COMMAND /bin/sh -c
            "${DUCKDB_BPFTOOL} btf dump file /sys/kernel/btf/vmlinux format c > '${DUCKDB_BPF_VMLINUX}'"
    DEPENDS /sys/kernel/btf/vmlinux
    VERBATIM)

  add_custom_command(
    OUTPUT "${DUCKDB_BPF_OBJECT}"
    COMMAND
      "${DUCKDB_BPF_CLANG}" -g -O2 -target bpf -D__TARGET_ARCH_x86
      "-I${DUCKDB_BPF_GENERATED_DIR}" "-I${CMAKE_CURRENT_SOURCE_DIR}"
      -I/usr/include -I/usr/include/${CMAKE_LIBRARY_ARCHITECTURE} -c
      "${CMAKE_CURRENT_SOURCE_DIR}/process_activity.bpf.c" -o
      "${DUCKDB_BPF_OBJECT}"
    DEPENDS "${DUCKDB_BPF_VMLINUX}"
            "${CMAKE_CURRENT_SOURCE_DIR}/process_activity.bpf.c"
            "${CMAKE_CURRENT_SOURCE_DIR}/process_activity_shared.h"
    VERBATIM)

  add_custom_command(
    OUTPUT "${DUCKDB_BPF_SKELETON}"
    COMMAND /bin/sh -c
            "${DUCKDB_BPFTOOL} gen skeleton '${DUCKDB_BPF_OBJECT}' > '${DUCKDB_BPF_SKELETON}'"
    DEPENDS "${DUCKDB_BPF_OBJECT}"
    VERBATIM)

  add_custom_target(duckdb_process_activity_bpf
                    DEPENDS "${DUCKDB_BPF_SKELETON}")
endif()

add_library_unity(duckdb_energy_attribution OBJECT
                  ${DUCKDB_ENERGY_ATTRIBUTION_SOURCES})

if(BUILD_EBPF_PROCESS_ATTRIBUTION)
  add_dependencies(duckdb_energy_attribution duckdb_process_activity_bpf)
  target_include_directories(
    duckdb_energy_attribution PRIVATE "${DUCKDB_BPF_GENERATED_DIR}"
                                      "${CMAKE_CURRENT_SOURCE_DIR}")
  target_compile_definitions(
    duckdb_energy_attribution PRIVATE DUCKDB_BUILD_EBPF_PROCESS_ATTRIBUTION=1)
  set(DUCKDB_SYSTEM_LIBS
      ${DUCKDB_SYSTEM_LIBS} ${DUCKDB_LIBBPF_LIBRARIES}
      ${DUCKDB_ELF_LIBRARY} ${ZLIB_LIBRARIES}
      PARENT_SCOPE)
endif()

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_energy_attribution>
    PARENT_SCOPE)
