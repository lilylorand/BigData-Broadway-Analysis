#!/bin/bash

echo "Cleaning old class files..."
rm -f *.class

echo "Compiling Java files..."
javac -classpath ".:commons-csv-1.10.0.jar:`yarn classpath`" -d . *.java

echo "Building jar..."
jar -cvf broadway_analytics.jar *.class

echo "Bundling CSV library into jar..."

rm -rf tmp_jar
mkdir tmp_jar
cd tmp_jar

jar -xf ../broadway_analytics.jar
jar -xf ../commons-csv-1.10.0.jar

jar -cvf ../broadway_analytics.jar *

cd ..
rm -rf tmp_jar

echo "Build complete!"