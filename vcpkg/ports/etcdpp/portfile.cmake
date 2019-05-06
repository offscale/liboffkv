include(vcpkg_common_functions)

# set(VCPKG_BUILD_TYPE release)

vcpkg_from_git(
    OUT_SOURCE_PATH SOURCE_PATH
    URL git@github.com:SamuelMarks/libetcd-cpp.git
    REF 65a2fe55d427fb6511c419e34b607b57d667ec67
    SHA512 db5aa4365eadbe3ff9f1549a35162c197fa821f4bf759a568694747aed1dda4902852a2472f22bb43e2ffea48698653e94be18503e9ac55691a8421ce8e23e7b
)

# file(COPY ${CMAKE_CURRENT_LIST_DIR}/CMakeLists.txt DESTINATION ${SOURCE_PATH})

vcpkg_configure_cmake(
    SOURCE_PATH ${SOURCE_PATH}
    PREFER_NINJA
)
vcpkg_install_cmake()

file(WRITE ${CURRENT_PACKAGES_DIR}/share/etcdpp/copyright "")

vcpkg_copy_pdbs()
