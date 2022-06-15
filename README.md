# UKV

Universal Key-Value store interface for both in-memory and persistent collections written in C/C++ and Assembly with bindings for:

* Python via [pybind11](https://github.com/pybind/pybind11),
* JavaScript,
* GoLang,
* Java,
* C#,

and many others in the future.

## Development

### Python

```sh
rm -rf build && cmake . && make && pip install --upgrade --force-reinstall . && pytest
```

### Java

```sh
apt-get install default-jdk
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
cd java
javac -h . DataBase.java -Xlint:deprecation
gcc -c -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux -I${PWD}/../include com_unum_ukv_DataBase.c -o com_unum_ukv_DataBase.o
gcc -shared -fPIC -o libukv.a com_unum_ukv_DataBase.o -lc
java -cp . -Djava.library.path=/NATIVE_SHARED_LIB_FOLDER com.unum.ukv.DataBase
java -cp . DataBase.java
```

### Go
