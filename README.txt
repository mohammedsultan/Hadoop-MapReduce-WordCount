AMI NAME : CS643-Hadoop-Namenode-Masihuddin_Mohammed
AMI ID: ami-c9b499ac
Region: US East (Ohio)

#Data Migration to HDFS#

After downloading the input file (states file), I followed 2 steps to transfer the contents from local machine to HDFS.

a.	Copy this data from local to namenode.
b.	Copy this data from namenode to hdfs.

Use the following commands:

Create a directory called states in namenode and transfer the states content from local machine to namenode/states

namenode$ mkdir ~/states
local$ scp -i ~/.ssh/hadoopkey.pem ~/Downloads/states/* namenode:~/states/

Make a directory called /user in HDFS

namenode$ hdfs dfs -mkdir /user

Copy the contents from /home/ec2-user/states to HDFS’s /user directory

namenode$ hdfs dfs -put '/home/ec2-user/states' /user/

#WordCount1:#

Compile the program

namenode$ javac -classpath $HADOOP_HOME/share/hadoop/common/hadoop-common-2.7.4.jar:$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.4.jar:$HADOOP_HOME/share/hadoop/common/lib/commons-cli-1.2.jar -d ./ WordCount1.java

Create a jar file called wc.jar

namenode$ jar cf wc.jar WordCount1*.class

Run the application program

namenode$ hadoop jar wc.jar WordCount1 /user/states final

To check the output, use the command:

namenode$ hdfs dfs –cat /user/ec2-user/final/part-r-00000


#WordCount2:#

Compile the program

namenode$ javac -classpath $HADOOP_HOME/share/hadoop/common/hadoop-common-2.7.4.jar:$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.4.jar:$HADOOP_HOME/share/hadoop/common/lib/commons-cli-1.2.jar -d ./ WordCount2.java

Create a jar file called wc.jar

namenode$ jar cf wc.jar WordCount2*.class

Run the application program

namenode$ hadoop jar wc.jar WordCount2 /user/states final2


To check the output, use the command:

namenode$ hdfs dfs –cat /user/ec2-user/final2/part-r-00000
