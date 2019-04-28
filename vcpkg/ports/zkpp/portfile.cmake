include(vcpkg_common_functions)

set(VCPKG_BUILD_TYPE release) # this string must be somewhere else, not here

vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO tgockel/zookeeper-cpp
    REF v0.2.2
    SHA512 64302e69e106cf314aa1861e79aaad175cec8f821b7b2ead17075b5db06a8a7484d723dae482ebf6c3185a0a8d57a915081dfbf399cd72eeeb90f58f01d94b3c
    HEAD_REF master
)

file(READ "${CMAKE_CURRENT_LIST_DIR}/CMakeInstall.txt" INSTALLATION_CODE)
file(APPEND "${SOURCE_PATH}/CMakeLists.txt" "${INSTALLATION_CODE}")

vcpkg_configure_cmake(
    SOURCE_PATH ${SOURCE_PATH}
    PREFER_NINJA
)

vcpkg_install_cmake()

file(INSTALL ${SOURCE_PATH}/COPYING DESTINATION ${CURRENT_PACKAGES_DIR}/share/zkpp RENAME copyright)

vcpkg_copy_pdbs()
