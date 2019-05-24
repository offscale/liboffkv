include(vcpkg_common_functions)

set(VCPKG_BUILD_TYPE release)

vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO shdown/ppconsul
    REF 39520bdc5d5910cc63a10ab6187821dd11e97bea
    SHA512 d65cc382c2328651fb7409316365ec0ab6675e6a73e3217f0fdfc6d3439fd40faf6b92ef4aa2464063e01f9697d1bff2d196d69f3fc2c9afc296d6f786db6bfa
    HEAD_REF master
    PATCHES "cmake_build.patch"
)

vcpkg_configure_cmake(
    SOURCE_PATH ${SOURCE_PATH}
    PREFER_NINJA
)
vcpkg_install_cmake()

vcpkg_fixup_cmake_targets(CONFIG_PATH cmake)


file(INSTALL ${SOURCE_PATH}/LICENSE_1_0.txt DESTINATION ${CURRENT_PACKAGES_DIR}/share/ppconsul RENAME copyright)
# file(REMOVE_RECURSE ${CURRENT_PACKAGES_DIR}/debug)
# file(REMOVE ${CURRENT_PACKAGES_DIR}/share/ppconsul/ppconsulConfig-debug.cmake)

vcpkg_copy_pdbs()
