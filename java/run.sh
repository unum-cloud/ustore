#!/usr/bin/env bash

# Get the absolute path of the executable
SELF_PATH=$(cd -P -- "$(dirname -- "$0")" && pwd -P) && SELF_PATH=$SELF_PATH/$(basename -- "$0")
DIR_PATH=$(dirname -- "$SELF_PATH")

# Depending on your platform, Java may be installed at different paths.
# Linux:
#       apt-get install default-jdk
#       export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
# MacOS:
#       brew install java
#       export JAVA_HOME="/usr/libexec/java_home -v 1.8"

cd java/com/unum/ukv &&
    javac -h . DataBase.java -Xlint:deprecation &&
    gcc -c -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux -I${DIR_PATH}/../include com_unum_ukv_Shared.c -o com_unum_ukv_Shared.o &&
    gcc -c -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux -I${DIR_PATH}/../include com_unum_ukv_DataBase_Context.c -o com_unum_ukv_DataBase_Context.o &&
    gcc -c -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux -I${DIR_PATH}/../include com_unum_ukv_DataBase_Transaction.c -o com_unum_ukv_DataBase_Transaction.o &&
    gcc -shared -fPIC -o libukv_java.so com_unum_ukv_Shared.o com_unum_ukv_DataBase_Context.o com_unum_ukv_DataBase_Transaction.o -L${DIR_PATH}/../build/lib -lukv_stl -lc &&
    java -ea -Xcheck:jni -Djava.library.path=. -cp . DataBase.java
cd ../../../..
