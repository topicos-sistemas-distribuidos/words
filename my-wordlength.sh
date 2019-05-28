#!/bin/bash

# test the hadoop cluster by running wordlength
mvn "-Dmyword=WordLength" clean && mvn compile && mvn package

# create input files 
mkdir input$1
echo "My wordlength - Try $1"
echo "Hello Docker" >input$1/file2.txt
echo "Hello Hadoop" >input$1/file1.txt

# create input directory on HDFS
hadoop fs -mkdir -p input$1

# put input files to HDFS
hdfs dfs -put ./input$1/* input$1

# run wordlength 
hadoop jar /root/words/target/words-1.0.0.jar input$1 output$1

# print the input files
echo -e "\ninput file1.txt:"
hdfs dfs -cat input$1/file1.txt

echo -e "\ninput file2.txt:"
hdfs dfs -cat input$1/file2.txt

# print the output of wordlength
echo -e "\nwordlength output:"
hdfs dfs -cat output$1/part-r-00000