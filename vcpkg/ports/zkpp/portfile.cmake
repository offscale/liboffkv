include(vcpkg_common_functions)

set(VCPKG_BUILD_TYPE release) # this string must be somewhere else, not here

vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO tgockel/zookeeper-cpp
    REF v0.2.2
    SHA512 64302e69e106cf314aa1861e79aaad175cec8f821b7b2ead17075b5db06a8a7484d723dae482ebf6c3185a0a8d57a915081dfbf399cd72eeeb90f58f01d94b3c
    HEAD_REF master
)

vcpkg_configure_cmake(
    SOURCE_PATH ${SOURCE_PATH}
    PREFER_NINJA
)

vcpkg_build_cmake(LOGFILE_ROOT install TARGET all ${ARGN})

file(INSTALL ${SOURCE_PATH}/COPYING DESTINATION ${CURRENT_PACKAGES_DIR}/share/zkpp RENAME copyright)

file(GLOB libraries "${CURRENT_BUILDTREES_DIR}/${TARGET_TRIPLET}-rel/*.so*")
file(COPY ${libraries} DESTINATION ${CURRENT_PACKAGES_DIR}/lib/)

file(GLOB zk "${SOURCE_PATH}/src/zk/*.hpp")
file(GLOB zk_server "${SOURCE_PATH}/src/zk/server/*.hpp")
file(COPY ${zk} DESTINATION ${CURRENT_PACKAGES_DIR}/include/zk/)
file(COPY ${zk_server} DESTINATION ${CURRENT_PACKAGES_DIR}/include/zk/server/)

vcpkg_copy_pdbs()
