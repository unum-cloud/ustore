#!/usr/bin/env bash

# Depending on your platform, Java may be installed at different paths.
# Linux:
#       apt-get install default-jdk
#       export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
# MacOS:
#       brew install java
#       export JAVA_HOME="/usr/libexec/java_home -v 1.8"

cd java &&
    javac -h . DataBase.java -Xlint:deprecation &&
    gcc -c -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux -I${PWD}/../include com_unum_ukv_DataBase.c -o com_unum_ukv_DataBase.o &&
    gcc -shared -fPIC -o libukv.a com_unum_ukv_DataBase.o -lc &&
    java -cp . -Djava.library.path=/NATIVE_SHARED_LIB_FOLDER com.unum.ukv.DataBase &&
    java -cp . DataBase.java
cd ..
