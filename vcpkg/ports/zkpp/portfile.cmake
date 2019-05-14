include(vcpkg_common_functions)

set(VCPKG_BUILD_TYPE release) # this string must be somewhere else, not here

vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO tgockel/zookeeper-cpp
    REF v0.2.3
    SHA512 086f31d4ca53f5a585fd8640caf9f2f21c90cf46d9cfe6c0e8e5b8c620e73265bb8aebec62ea4328f3f098a9b3000280582569966c0d3401627ab8c3edc31ca8
    HEAD_REF master
)

file(READ "${CMAKE_CURRENT_LIST_DIR}/CMakeInstall.txt" INSTALLATION_CODE)
file(WRITE "${SOURCE_PATH}/CMakeLists.txt" "${INSTALLATION_CODE}")

vcpkg_configure_cmake(
    SOURCE_PATH ${SOURCE_PATH}
    PREFER_NINJA
)

vcpkg_install_cmake()

file(INSTALL ${SOURCE_PATH}/COPYING DESTINATION ${CURRENT_PACKAGES_DIR}/share/zkpp RENAME copyright)

vcpkg_copy_pdbs()
