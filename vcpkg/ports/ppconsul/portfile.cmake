include(vcpkg_common_functions)

set(VCPKG_BUILD_TYPE release)

vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO shdown/ppconsul
    REF 38fe6987e8c64ebb0f07838ed9bec8688d988885
    SHA512 a664f8c49603099ba8f37b87191c6f71fb331aac7e900f84f737112c6160dafb7e37f43887a440b8e0472a44faf409242e2b860ed29e3edbe493134483450dae
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
