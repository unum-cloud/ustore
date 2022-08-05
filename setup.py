import os

from pybind11.setup_helpers import Pybind11Extension, build_ext
from setuptools import setup
from qibuild import cmake

__version__ = open('VERSION', 'r').read()
__libname__ = 'ukv'

cmake_cache = cmake.read_cmake_cache('CMakeCache.txt')

include_dirs = ['include/', 'python/', 'python/pybind/']
if 'fmt_INCLUDE_DIR' in cmake_cache:
    include_dirs.append(cmake_cache['fmt_INCLUDE_DIR'])
if 'nlohmann_json_SOURCE_DIR' in cmake_cache:
    include_dirs.append(f'{cmake_cache["nlohmann_json_SOURCE_DIR"]}/include')

compile_args = cmake_cache.pop('CMAKE_CXX_FLAGS', ['-std=c++17', '-O3'])

ext_modules = [
    Pybind11Extension(
        'ukv/stl',
        [
            'python/pybind.cpp',
            'python/pybind/database.cpp',
            'python/pybind/networkx.cpp',
            'python/pybind/pandas.cpp',
        ],
        include_dirs=include_dirs,
        library_dirs=['build/lib/'],
        libraries=['ukv_stl'],
        extra_compile_args=compile_args,
        define_macros=[
            ('UKV_VERSION', __version__),
            ('UKV_PYTHON_MODULE_NAME', 'stl')
        ],
    ),
    Pybind11Extension(
        'ukv/rocks',
        [
            'python/pybind.cpp',
            'python/pybind/database.cpp',
            'python/pybind/networkx.cpp',
            'python/pybind/pandas.cpp',
        ],
        include_dirs=include_dirs,
        library_dirs=['build/lib/'],
        libraries=['ukv_rocksdb', 'rocksdb'],
        extra_compile_args=compile_args,
        define_macros=[
            ('UKV_VERSION', __version__),
            ('UKV_PYTHON_MODULE_NAME', 'rocks')
        ],
    ),
    Pybind11Extension(
        'ukv/level',
        [
            'python/pybind.cpp',
            'python/pybind/database.cpp',
            'python/pybind/networkx.cpp',
            'python/pybind/pandas.cpp',
        ],
        include_dirs=include_dirs,
        library_dirs=['build/lib/'],
        libraries=['ukv_leveldb', 'leveldb'],
        extra_compile_args=compile_args,
        define_macros=[
            ('UKV_VERSION', __version__),
            ('UKV_PYTHON_MODULE_NAME', 'level')
        ],
    ),
]

# Lets use README.md as `long_description`
this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md')) as f:
    long_description = f.read()


setup(
    name=__libname__,
    version=__version__,

    author='Ashot Vardanian',
    author_email='info@unum.cloud',
    url='https://github.com/unum-cloud/UKV',
    description='Python bindings for Unum\'s Universal Key-Value store.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    zip_safe=False,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'License :: Other/Proprietary License',
        'Natural Language :: English',
        'Operating System :: POSIX',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: C',
        'Programming Language :: C++',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: Implementation :: CPython',
        'Topic :: Database',
        # More classifiers to come later:
        # https://pypi.org/classifiers/
        # 'Environment :: GPU',
        # 'Framework :: Apache Airflow :: Provider',
        # 'Framework :: IPython',
    ],

    ext_modules=ext_modules,
    extras_require={'test': 'pytest'},
    cmdclass={'build_ext': build_ext},
    python_requires='>=3.6',
)
