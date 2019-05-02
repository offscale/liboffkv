include(vcpkg_common_functions)

set(VCPKG_BUILD_TYPE release)

vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO nokia/etcd-cpp-apiv3
    REF master
    SHA512 f16d38e527f91adf7a5859c73efe281deb307ffed38e1ddef64671e258fcc77d75566ebb05817a77e05a0f098bb1159488f89106a4840da568936e9158f23505
    HEAD_REF master
)

file(COPY "${CMAKE_CURRENT_LIST_DIR}/CMakeLists.txt" DESTINATION ${SOURCE_PATH})

vcpkg_configure_cmake(
    SOURCE_PATH ${SOURCE_PATH}
    PREFER_NINJA
)
vcpkg_install_cmake()

file(WRITE ${CURRENT_PACKAGES_DIR}/share/etcdpp/copyright "")

vcpkg_copy_pdbs()
