#!/bin/bash
CP=""
for D in lib/*.jar; do CP=${CP}:${D}; done
CP=${CP}:`hadoop classpath`
CP=${CP}:metascope.jar

if [ -z "$1" ]; then
  echo "No argument supplied. Please set the path to schedoscope.conf, e.g. './start-metascope /apps/schedoscope/schedoscope.conf'"
else
  java -Xmx1024m -XX:MaxPermSize=512M -cp ${CP} -Dconfig.file=${1} org.schedoscope.metascope.Metascope
fi
