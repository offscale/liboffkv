include(vcpkg_common_functions)

vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO oliora/ppconsul
    REF e32cf6f80b598760278c018eed546f88292f1735
    SHA512 8ca3b1f37a22bf27ce27af27676ba88a67c0c7b3357488edb925649b5d59b546b5cbf7a14452d07b41bb2649de7d0a8126d247a797f1a0e7395c3704fb54a758
    HEAD_REF master
)

vcpkg_configure_cmake(
    SOURCE_PATH ${SOURCE_PATH}
    PREFER_NINJA
)
vcpkg_install_cmake()

vcpkg_fixup_cmake_targets(CONFIG_PATH cmake)


file(INSTALL ${SOURCE_PATH}/LICENSE_1_0.txt DESTINATION ${CURRENT_PACKAGES_DIR}/share/ppconsul RENAME copyright)
file(REMOVE_RECURSE ${CURRENT_PACKAGES_DIR}/debug)
file(REMOVE ${CURRENT_PACKAGES_DIR}/share/ppconsul/ppconsulConfig-debug.cmake)

vcpkg_copy_pdbs()
