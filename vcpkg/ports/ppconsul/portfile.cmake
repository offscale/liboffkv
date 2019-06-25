include(vcpkg_common_functions)

vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO oliora/ppconsul
    REF 6a1a8aa5d59e559723c91bd2eb5edf1fe7268ef9
    SHA512 fdbe7e3a87250e7e2560968ebc5963257f877e31711651b1038ea34379329758ce1e0bfe7e10b2d08c8237e1412be469b0bdbcecaab8b61651177bcfbb49a793
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
file(REMOVE_RECURSE ${CURRENT_PACKAGES_DIR}/debug/include)


vcpkg_copy_pdbs()
