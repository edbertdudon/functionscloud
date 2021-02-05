#!/bin/bash
# Pre-run Spark R script for required packages
sudo apt-get install -y libcurl4-openssl-dev
sudo apt-get install -y libssl-dev
sudo apt-get install -y libgit2-dev
sudo apt-get install -y libxml2-dev
sudo apt-get install -y libmyodbc
sudo apt-get install -y msodbcsql17
sudo apt-get install -y libsqora.so.19.1

gsutil cp gs://tart-90ca2.appspot.com/scripts/tart-12b0c03bcce1.json /tmp/tart-12b0c03bcce1.json
# gsutil cp gs://tart-90ca2.appspot.com/scripts/mysql-connector-java-8.0.20.jar /tmp/mysql-connector-java-8.0.20.jar
# gsutil cp gs://tart-90ca2.appspot.com/scripts/mssql-jdbc-8.4.0.jre8.jar /tmp/mssql-jdbc-8.4.0.jre8.jar
# gsutil cp gs://tart-90ca2.appspot.com/scripts/ojdbc8.jar /tmp/ojdbc8.jar
