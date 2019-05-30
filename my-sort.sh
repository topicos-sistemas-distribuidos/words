#!/bin/bash

# test the hadoop cluster by running wordcount
mvn "-Dmyproperty=WordOrderByLength" clean && mvn compile "-Dmyproperty=WordOrderByLength" && mvn package "-Dmyproperty=WordOrderByLength"

# create input files 
mkdir input$1
echo "My sort - Try $1"
cp /root/words/books/* input$1

# create input directory on HDFS
hadoop fs -mkdir -p input$1

# put input files to HDFS
hdfs dfs -put ./input$1/* input$1

# run sort
yarn jar /root/words/target/words-1.0.0.jar input$1 output$1

# print the output of sort
echo -e "\nsort output:"
hdfs dfs -cat output$1/part-r-00000
