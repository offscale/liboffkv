include(vcpkg_common_functions)

vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO raid-7/libetcd-cpp
    REF fcb2afbd0e47e67aedbd1fee06ee132fc9a84c53
    SHA512 459507ef8543657156822f208e709f013d2b4fb22736946de4295b96c7da85823829467ca2308168f96ee96cb94d20d53740598a6624fa2cbf78fd2d9d5dda54
    HEAD_REF master
)

vcpkg_configure_cmake(
    SOURCE_PATH ${SOURCE_PATH}
    PREFER_NINJA
)
vcpkg_install_cmake()

file(WRITE ${CURRENT_PACKAGES_DIR}/share/etcdpp/copyright "")

vcpkg_copy_pdbs()
