include(vcpkg_common_functions)

vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO shdown/ppconsul
    REF 65c6aa6a9f06e9c8f0e3c39ac4ec8498f772263a
    SHA512 d5ccbb9e1631deedbb61e6ac1de258a93a604162792b0ebdb16720032017d7b566039159e10f551941daa4a92c90541547ba8801d7df9b7a9a6a1ff93cfcae82
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
