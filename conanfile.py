import os

from conan import ConanFile
from conan.errors import ConanInvalidConfiguration
from conan.tools.build import check_min_cppstd, cross_building
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
from conan.tools.files import apply_conandata_patches, copy, export_conandata_patches, get, rmdir
from conan.tools.microsoft import is_msvc, is_msvc_static_runtime
from conan.tools.scm import Version


class ConanUStore(ConanFile):

    exports = 'VERSION', 'LICENSE', 'README.md'
    exports_sources = 'CMakeLists.txt', 'src/*', 'include/*', 'cmake/*', 'VERSION'

    name = 'ustore'
    version = open('VERSION').read()
    # Take just the first line
    license = open('LICENSE').read().split('\n', 1)[0]
    description = open('README.md').read()
    url = 'https://github.com/unum-cloud/ustore.git'
    homepage = 'https://unum.cloud/ustore'

    # Complete list of possible settings:
    # https://docs.conan.io/en/latest/extending/custom_settings.html
    settings = {
        # https://docs.conan.io/en/latest/introduction.html#all-platforms-all-build-systems-and-compilers
        'os': ['Linux'],
        # https://docs.conan.io/en/latest/integrations/compilers.html
        'compiler': [
            'gcc', 'clang', 'intel',
            # Not fully supported yet:
            # 'intel-cc'
        ],
        # https://github.com/conan-io/conan/issues/2500
        'build_type': ['Release'],
        'arch': ['x86', 'x86_64', 'armv8', 'armv8_32', 'armv8.3'],
    }
    generators = 'CMakeToolchain', 'cmake_find_package', 'cmake'
    options = {'with_arrow': [True, False]}
    
    default_options = {
        'with_arrow': False,
        'openssl:shared': True,
        'arrow:shared': True,
        'arrow:parquet': True,
        'arrow:dataset_modules': True,
        'arrow:with_re2': True,
        'arrow:compute': True,
        'arrow:with_flight_rpc': True,
        'arrow:with_utf8proc': True,
        'arrow:encryption': False,
        'arrow:with_openssl': True,
        'arrow:with_csv': True,
        'arrow:simd_level': 'avx2',
        'arrow:with_jemalloc': False,
        'arrow:with_json': True,
        'arrow:with_flight_sql': True,
        'arrow:with_snappy': False,
        'arrow:cli': False,
        'arrow:gandiva': False,
        'arrow:with_s3': False,
    }
    

    def layout(self):
        cmake_layout(self)
        

    def generate(self):
        tc = CMakeToolchain(self)
        
        if cross_building(self):
            cmake_system_processor = {
                "armv8": "aarch64",
                "armv8.3": "aarch64",
            }.get(str(self.settings.arch), str(self.settings.arch))
            if cmake_system_processor == "aarch64":
                tc.variables["ARROW_CPU_FLAG"] = "armv8"
        if is_msvc(self):
            tc.variables["ARROW_USE_STATIC_CRT"] = is_msvc_static_runtime(self)
        # tc.variables["ARROW_BUILD_STATIC"] = not bool(
        #     self.options["arrow"].shared)
        tc.variables["ARROW_DEPENDENCY_SOURCE"] = "BUNDLED"
        # tc.variables["ARROW_BUILD_SHARED"] = bool(self.options['arrow'].shared)
        tc.variables["ARROW_BUILD_TESTS"] = False
        tc.variables["ARROW_ENABLE_TIMING_TESTS"] = False
        tc.variables["ARROW_BUILD_EXAMPLES"] = False
        tc.variables["ARROW_BUILD_BENCHMARKS"] = False
        tc.variables["ARROW_BUILD_INTEGRATION"] = False
        tc.variables["PARQUET_REQUIRE_ENCRYPTION"] = bool(
            self.options['arrow'].encryption)
        tc.variables["ARROW_BUILD_UTILITIES"] = bool(self.options['arrow'].cli)
        tc.variables["re2_SOURCE"] = "BUNDLED"
        tc.variables["Protobuf_SOURCE"] = "BUNDLED"
        tc.variables["Snappy_SOURCE"] = "BUNDLED"
        tc.variables["gRPC_SOURCE"] = "BUNDLED"
        tc.variables["ZLIB_SOURCE"] = "BUNDLED"
        tc.variables["Thrift_SOURCE"] = "BUNDLED"
        tc.variables["utf8proc_SOURCE"] = "BUNDLED"
        tc.variables["ARROW_INCLUDE_DIR"] = True
        
        tc.variables["ARROW_WITH_THRIFT"] = self._with_thrift()
        tc.variables["Thrift_SOURCE"] = "SYSTEM"
        if self._with_thrift():
            tc.variables["THRIFT_VERSION"] = bool(self.dependencies["thrift"].ref.version) # a recent thrift does not require boost
            tc.variables["ARROW_THRIFT_USE_SHARED"] = bool(self.dependencies["thrift"].options.shared)
        
        tc.variables["ARROW_BUILD_STATIC"] = True
        

    def build(self):
        # https://docs.conan.io/en/1.50/reference/build_helpers/cmake.html#constructor
        cmake = CMake(self)
        cmake.configure(variables={
            'USTORE_BUILD_SDK_PYTHON': 0,
            'USTORE_BUILD_TESTS': 0,
            'USTORE_BUILD_BENCHMARKS': 0,
            'USTORE_BUILD_API_FLIGHT_CLIENT': self.options['with_arrow'],
        })
        

    def configure(self):
        pass
    
    
    def package(self):
        cmake = CMake(self)
        cmake.install()

        self.copy('*.a', dst='lib',
                  src=os.path.join(self.source_folder, 'build/lib'))
        

    def package_info(self):
        # Larger projects like UStore or Boost would have a lot of parts,
        # but it is recommended to ship one library per component
        # https://docs.conan.io/en/1.26/creating_packages/package_information.html#using-components
        self.cpp_info.name = 'ustore'
        self.cpp_info.includedirs = ['include/']
        self.cpp_info.components['ucset'].libs = ['libustore_embedded_ucset']
        self.cpp_info.components['leveldb'].libs = [
            'libustore_embedded_leveldb']
        self.cpp_info.components['rocksdb'].libs = [
            'libustore_embedded_rocksdb']
        if self.default_options['with_arrow']:
            self.cpp_info.components['flight_client'].libs = [
                'ustore_flight_client']
        self.cpp_info.set_property("cmake_file_name", "OpenSSL")
        self.cpp_info.set_property("cmake_find_mode", "both")
        self.cpp_info.set_property("pkg_config_name", "openssl")
        self.cpp_info.set_property("cmake_build_modules", [self._module_file_rel_path])
        self.cpp_info.names["cmake_find_package"] = "OpenSSL"
        self.cpp_info.names["cmake_find_package_multi"] = "OpenSSL"
        self.cpp_info.components["ssl"].builddirs.append(self._module_subfolder)
        self.cpp_info.components["ssl"].build_modules["cmake_find_package"] = [self._module_file_rel_path]
        self.cpp_info.components["ssl"].set_property("cmake_build_modules", [self._module_file_rel_path])
        self.cpp_info.components["crypto"].builddirs.append(self._module_subfolder)
        self.cpp_info.components["crypto"].build_modules["cmake_find_package"] = [self._module_file_rel_path]
        self.cpp_info.components["crypto"].set_property("cmake_build_modules", [self._module_file_rel_path])
        if self._use_nmake:
            libsuffix = "d" if self.settings.build_type == "Debug" else ""
            self.cpp_info.components["ssl"].libs = ["libssl" + libsuffix]
            self.cpp_info.components["crypto"].libs = ["libcrypto" + libsuffix]
        else:
            self.cpp_info.components["ssl"].libs = ["ssl"]
            self.cpp_info.components["crypto"].libs = ["crypto"]
        self.cpp_info.components["ssl"].requires = ["crypto"]
        if not self.options.no_zlib:
            self.cpp_info.components["crypto"].requires.append("zlib::zlib")
        if self.settings.os == "Windows":
            self.cpp_info.components["crypto"].system_libs.extend(["crypt32", "ws2_32", "advapi32", "user32", "bcrypt"])
        elif self.settings.os == "Linux":
            self.cpp_info.components["crypto"].system_libs.extend(["dl", "rt"])
            self.cpp_info.components["ssl"].system_libs.append("dl")
            if not self.options.no_threads:
                self.cpp_info.components["crypto"].system_libs.append("pthread")
                self.cpp_info.components["ssl"].system_libs.append("pthread")
        elif self.settings.os == "Neutrino":
            self.cpp_info.components["crypto"].system_libs.append("atomic")
            self.cpp_info.components["ssl"].system_libs.append("atomic")
        self.cpp_info.components["crypto"].set_property("cmake_target_name", "OpenSSL::Crypto")
        self.cpp_info.components["crypto"].set_property("pkg_config_name", "libcrypto")
        self.cpp_info.components["ssl"].set_property("cmake_target_name", "OpenSSL::SSL")
        self.cpp_info.components["ssl"].set_property("pkg_config_name", "libssl")
        self.cpp_info.components["crypto"].names["cmake_find_package"] = "Crypto"
        self.cpp_info.components["crypto"].names["cmake_find_package_multi"] = "Crypto"
        self.cpp_info.components["ssl"].names["cmake_find_package"] = "SSL"
        self.cpp_info.components["ssl"].names["cmake_find_package_multi"] = "SSL"
        openssl_modules_dir = os.path.join(self.package_folder, "lib", "ossl-modules")
        self.runenv_info.define_path("OPENSSL_MODULES", openssl_modules_dir)
        # For legacy 1.x downstream consumers, remove once recipe is 2.0 only:
        self.env_info.OPENSSL_MODULES = openssl_modules_dir
            

    def requirements(self):
        # Most of our dependencies come from `CMake`
        # https://docs.conan.io/en/latest/reference/conanfile/methods.html#requirements
        self.requires('arrow/10.0.0')
        # https://conan.io/center/openssl
        self.requires('openssl/1.1.1s')
        self.requires('protobuf/3.21.4')
        

    def system_requirements(self):
        # If we need a fresher version of Arrow, we can get it from the default package manager.
        # https://docs.conan.io/en/latest/reference/conanfile/methods.html?#system-requirements

        # https://arrow.apache.org/install/
        # SystemPackageTool().install('libarrow-dev ')
        # SystemPackageTool().install('libarrow-flight-dev')
        pass
    
    def _with_thrift(self, required=False):
        # No self.options.with_thift exists
        return bool(required or self._parquet())
    
    def _parquet(self, required=False):
        if required or self.options['arrow'].parquet == "auto":
            return bool(self.options.get_safe("substrait", False))
        else:
            return bool(self.options['arrow'].parquet)
