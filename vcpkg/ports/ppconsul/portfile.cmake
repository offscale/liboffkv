include(vcpkg_common_functions)

vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO oliora/ppconsul
    REF 65c1399e4c965cc5df483262f826bd8660c6289e
    SHA512 a0832aec9b6f0b90fd0e26deeb8d13e7f8a00938af57890b0868ff5852226165e861d5dfcf4ac39e39b3af32a4c1d09004d40814a02bea3ada311e2ff0e8b844
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
