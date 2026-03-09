#!/bin/bash
set -eo pipefail

DRIVER_JAR="../../bindings/java/driver/build/libs/driver-1.5.0.jar"

echo "Building driver JAR..."
pushd ../../bindings/java
JAVA_HOME=/usr/lib/jvm/java-17-openjdk ./gradlew :driver:jar -x test
popd

JSON_JAR="json-20231013.jar"
if [ ! -f "$JSON_JAR" ]; then
    echo "Downloading org.json dependency..."
    wget -q https://repo1.maven.org/maven2/org/json/json/20231013/json-20231013.jar -O "$JSON_JAR"
fi

echo "Compiling DecentDBJdbcExample.java..."
javac -cp "$DRIVER_JAR:$JSON_JAR" DecentDBJdbcExample.java

echo "Running DecentDBJdbcExample..."
java --enable-native-access=ALL-UNNAMED -cp ".:$DRIVER_JAR:$JSON_JAR" DecentDBJdbcExample
