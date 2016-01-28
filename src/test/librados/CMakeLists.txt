# unittest_librados
add_executable(unittest_librados EXCLUDE_FROM_ALL
  librados.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_ceph_unittest(unittest_librados ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_librados)
target_link_libraries(unittest_librados librados global ${BLKID_LIBRARIES})

# unittest_librados_config
add_executable(unittest_librados_config EXCLUDE_FROM_ALL
  librados_config.cc
  $<TARGET_OBJECTS:heap_profiler_objs>
  )
add_ceph_unittest(unittest_librados_config ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_librados_config)
target_link_libraries(unittest_librados_config
  librados
  global
  ${BLKID_LIBRARIES}
  )

