cmake_minimum_required(VERSION 3.9)

set(PATCH_VERSION "1" CACHE INTERNAL "Patch version")
set(PROJECT_VERSION 0.0.${PATCH_VERSION})

project(mapreduce VERSION ${PROJECT_VERSION})

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -static")

add_executable(mapreduce_cli client.cpp MapReduce.cpp FilePool.cpp ShufflerFilePool.cpp)

add_subdirectory(googletest)
add_executable(tests tests.cpp MapReduce.cpp FilePool.cpp ShufflerFilePool.cpp)
target_link_libraries(tests gtest_main)

set_target_properties(mapreduce_cli tests PROPERTIES
        CXX_STANDARD 17
        CXX_STANDARD_REQUIRED ON
        )

if (MSVC)
    target_compile_options(mapreduce_cli PRIVATE
            /W4
            )
    target_compile_options(tests PRIVATE
            /W4
            )
else ()
    target_compile_options(mapreduce_cli PRIVATE
            -Wall -Wextra -pedantic -Werror
            )
    target_compile_options(tests PRIVATE
            -Wall -Wextra -pedantic -Werror
            )
endif()

install(TARGETS mapreduce_cli RUNTIME DESTINATION bin)

set(CPACK_GENERATOR DEB)

set(CPACK_PACKAGE_VERSION_MAJOR "${PROJECT_VERSION_MAJOR}")
set(CPACK_PACKAGE_VERSION_MINOR "${PROJECT_VERSION_MINOR}")
set(CPACK_PACKAGE_VERSION_PATCH "${PROJECT_VERSION_PATCH}")

set(CPACK_PACKAGE_CONTACT example@example.com)

include(CPack)

enable_testing()
add_test(tests tests)
