# PageRank-calculation-in-Hadoop-MapReduce
Finding pageRank of each page in a huge corpus and considering the top 100 pages based on its page rank

First copy input files from local to dsba cluster
scp simplewiki.xml spothina@dsba-hadoop.uncc.edu:/hdfs/user/spothina

// Compile the program
javac pageRank.java -cp $(hadoop classpath) -d build/
// Build jar file
jar -cvf pageRank.jar -C build/ .

// copy jar from local to dsba cluster
scp pageRank.jar spothina@dsba-hadoop.uncc.edu:/hdfs/user/spothina

(Give comands in the following order)
// Running pageRank on cluster
hadoop jar pageRank.jar org.myorg.pageRank simplewiki.xml pgRnk pgRnkFinal

// output is created in pgRnkFinal/part-r-00000 file
// strip top 100 lines
head -100 pgRnkFinal/part-r-00000 >> output.txt

// Copy the output file from cluster to local
scp spothina@dsba-hadoop.uncc.edu:/hdfs/user/spothina/output.txt  /home/cloudera/pagerank/
