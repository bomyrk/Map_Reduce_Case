# Mod-2-Assignment
 MapReduce Code

 these are code of MapReduce program for some simples tasks.
 1- count word for Shakespeare dataset
 2- time duration of international call with phone dataset
 3- Analysis of speed dataset
 4- Analysis of names occurrences.

Once the code has been written in Eclipse, you have to produce the jar file and then execute it in the terminal.

to execute any jar you should type into terminal

hadoop jar path/to/jar/file driver/class/  <in file> <out file>


###### MAPREDUCE HADOOP ASSIGNMENT 1 #######

# all codes for different question are submitted separetly
# code 1 for question 1: Find out the count of each word in the ‘Shakespeare.txt’ dataset in the ‘Shakespeare.rar’
# code 2 for question 2: Find out the count of each word using two reducers only
# code 3 for question 3: Find out the most commonly used words (Words with the count over 100 are considered common)

# upload shakepeare file in hdfs
hdfs dfs -put Shakespeare.txt input/

# to list all jar files and make sure it is present
ls -lrt hadoop

# to execute the mapreduce (.jar) program
hadoop jar wordcount1.jar WordCount1 input/Shakespeare.txt output/Shakespeare1
hadoop jar wordcount2.jar WordCount1 input/Shakespeare.txt output/Shakespeare2
hadoop jar wordcount3.jar WordCount1 input/Shakespeare.txt output/Shakespeare3

# to list output from directory output
hdfs dfs -ls output/Shakespeare1
hdfs dfs -ls output/Shakespeare2
hdfs dfs -ls output/Shakespeare3


###### MAPREDUCE HADOOP ASSIGNMENT 2 #######

# all codes for different question are submitted separetly


# data set already in hdfs
hdfs dfs -ls input/

# to list all jar files and make sure it is present
ls -lrt hadoop

# to execute the mapreduce (.jar) program
hadoop jar abccall.jar ABCcall input/Shakespeare.txt output/Abccall

# to list output from directory output
hdfs dfs -ls output/Abccall


###### MAPREDUCE HADOOP ASSIGNMENT 3 #######

# all codes for different question are submitted separetly

# upload Speed-data.txt file in hdfs
hdfs dfs -put Speed-data.txt input/

# check data in hdfs
hdfs dfs -ls input/

# to list all jar files and make sure it is present
ls -lrt hadoop

# to execute the mapreduce (.jar) program
hadoop jar speedcar.jar SpeedCar input/Speed-data.txt output/Speedcar1
hadoop jar speedcarb.jar SpeedCarb input/Speed-data.txt output/Speedcar2

# to list output from directory output
hdfs dfs -ls  output/Speedcar1
hdfs dfs -ls  output/Speedcar1

###### MAPREDUCE HADOOP ASSIGNMENT 4 #######

# all codes for different question are submitted separetly

# upload NationalNames.csv file in hdfs
hdfs dfs -put NationalNames.csv input/

# check data in hdfs
hdfs dfs -ls input/

# to list all jar files and make sure it is present
ls -lrt hadoop

# to execute the mapreduce (.jar) program
hadoop jar childname1.jar ChildName1 input/NationalNames.txt output/Childname1
hadoop jar childname2.jar ChildName2 input/NationalNames.txt output/Childname2
hadoop jar childname3.jar ChildName3 input/NationalNames.txt output/Childname3
hadoop jar childname4.jar ChildName4 input/NationalNames.txt output/Childname4
hadoop jar childname5.jar ChildName5 input/NationalNames.txt output/Childname5
hadoop jar childname66.jar ChildName66 input/NationalNames.txt output/Childname6
hadoop jar childname7.jar ChildName7 input/NationalNames.txt output/Childname7

# to list output from directory output
hdfs dfs -ls  output/Childname1
hdfs dfs -ls  output/Childname2
hdfs dfs -ls  output/Childname3
hdfs dfs -ls  output/Childname4
hdfs dfs -ls  output/Childname5
hdfs dfs -ls  output/Childname6
hdfs dfs -ls  output/Childname7
