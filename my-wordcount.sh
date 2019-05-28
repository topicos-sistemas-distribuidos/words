#!/bin/bash

# test the hadoop cluster by running wordcount
mvn "-Dmyproperty=WordCount" clean && mvn compile && mvn package

# create input files 
mkdir input$1
echo "My wordcount - Try $1"
echo "Hello Docker" >input$1/file2.txt
echo "Hello Hadoop" >input$1/file1.txt

# create input directory on HDFS
hadoop fs -mkdir -p input$1

# put input files to HDFS
hdfs dfs -put ./input$1/* input$1

# run wordcount 
hadoop jar /root/words/target/words-1.0.0.jar input$1 output$1

# print the input files
echo -e "\ninput file1.txt:"
hdfs dfs -cat input$1/file1.txt

echo -e "\ninput file2.txt:"
hdfs dfs -cat input$1/file2.txt

# print the output of wordcount
echo -e "\nwordcount output:"
hdfs dfs -cat output$1/part-r-00000
