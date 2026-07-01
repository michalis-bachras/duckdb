add_library_unity(duckdb_energy_attribution OBJECT energy_attribution.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_energy_attribution>
    PARENT_SCOPE)
