include(vcpkg_common_functions)

# set(VCPKG_BUILD_TYPE release)

vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO raid-7/libetcd-cpp
    REF 20db6e3a941c421e3d50d1ad5dd66197bd8f7e4c
    SHA512 06aaa6b8737b4df5af97345669c12fa5288003958a7591840d31ce0b506ba3a415b0491d392033a967b7fbc44b7a8ec33e442baa3cd0a2ec97c03697ad581123
)

# file(COPY ${CMAKE_CURRENT_LIST_DIR}/CMakeLists.txt DESTINATION ${SOURCE_PATH})

vcpkg_configure_cmake(
    SOURCE_PATH ${SOURCE_PATH}
    PREFER_NINJA
)
vcpkg_install_cmake()

file(WRITE ${CURRENT_PACKAGES_DIR}/share/etcdpp/copyright "")

vcpkg_copy_pdbs()
