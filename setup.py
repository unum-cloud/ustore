import os

from pybind11.setup_helpers import Pybind11Extension, build_ext
from setuptools import setup

__version__ = '0.0.1'
__libname__ = 'ukv'

ext_modules = [
    Pybind11Extension(
        'ukv',
        ['python/ukv_pybind.cpp'],
        # Example: passing in the version to the compiled code
        include_dirs=['include/'],
        library_dirs=['build/lib/'],
        libraries=['ukv_rocksdb'],
        define_macros=[('UKV_VERSION', __version__)],
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
    description='Python bindings for Unum\'s Univeral Key-Value store.',
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
