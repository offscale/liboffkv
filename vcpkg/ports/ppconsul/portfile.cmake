include(vcpkg_common_functions)

vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO shdown/ppconsul
    REF 3f7b376bf0f1ace8251b2cd82757fbc44f32853f
    SHA512 be668e30de0d3ea4b7e8223daaacbb5e8e66b8e66b91a99518d3c993ed128aa6dbc4fc20dfc9803e411f2fe04a5bb7c3ba832115da6a67582c4837baba9441e8
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
