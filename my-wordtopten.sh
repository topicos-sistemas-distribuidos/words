#!/bin/bash

# test the hadoop cluster by running wordlength
mvn "-Dmyproperty=TopTenHashTag" clean && mvn compile "-Dmyproperty=TopTenHashTag" && mvn package "-Dmyproperty=TopTenHashTag"

# create input files 
mkdir input$1
echo "My TopTenHashTag - Try $1"
cp /root/words/books/* input$1

# create input directory on HDFS
hadoop fs -mkdir -p input$1

# put input files to HDFS
hdfs dfs -put ./input$1/* input$1

# run TopTenHashTag 
hadoop jar /root/words/target/words-1.0.0.jar input$1 output$1

# print the output of wordlength
echo -e "\nTopTenHashTag output:"
hdfs dfs -cat output$1/part-r-00000