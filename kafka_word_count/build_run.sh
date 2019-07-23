#!/bin/bash

#
# Wojciech Golab, 2017
#

source ./settings.sh

echo --- Cleaning
rm -f WordCount.class

echo --- Compiling Java
$JAVA_CC WordCount.java
if [ $? -eq 0 ]
then
  echo "Success..."
else
  echo "Error..."
  exit 1
fi

echo --- WordCount demo has benn started

$JAVA WordCount $KBROKERS wordcount-input-$USER wordcount-output-$USER wordcount-$USER