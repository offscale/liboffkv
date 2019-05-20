include(vcpkg_common_functions)

set(VCPKG_BUILD_TYPE release)

vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO shdown/ppconsul
    REF 422c8a2a6220e0695a445a9e2b2ffdf97d72f1cc
    SHA512 98c3d886e8ff1b71dc3a0f49afeff5b98dbfe411490fe6ffecbcd8e116fd53834f13440ec81cd7e99456d7f3d4b0375475ba40e8e34bc44366d9e2ea949316b3
    HEAD_REF master
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
