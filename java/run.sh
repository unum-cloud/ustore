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
    gcc -shared -fPIC -o libukv_java.so com_unum_ukv_DataBase.o -L${PWD}/../build/lib -lukv_stl_embedded -lc &&
    java -Djava.library.path=. -cp . DataBase.java
cd ..
