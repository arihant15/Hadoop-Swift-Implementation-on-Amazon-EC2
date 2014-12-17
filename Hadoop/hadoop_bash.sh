echo "******************************************************";
echo "WELCOME TO HADOOP"
echo "******************************************************";
echo "Formatting HDFS Directory"
echo "******************************************************";
echo "******************************************************";
echo "";
hdfs namenode -format;
echo "******************************************************";
echo "******************************************************";
echo "Starting Hadoop...";
echo "******************************************************";
echo "******************************************************";
echo ""
start-all.sh;
echo ""
echo "******************************************************";
echo ""
jps
echo ""
echo "******************************************************";
echo "Enter the FQN for the File to be put in Hadoop HDFS directory: ";																																																						
read filename;
echo ""
echo "Reading File: $filename";
echo ""
echo "Creating 'input' directory in Hadoop HDFS...";
hdfs dfs -mkdir /input;
echo ""
echo "Placing the file in HDFS directory...";
hdfs dfs -put $filename /input001;
echo ""
echo "******************************************************";
echo "Running Hadoop Word Count";
echo "******************************************************";
echo ""
hadoop jar HadoopWordCount.jar HadoopWordCount /input001 output001 output002;
echo "******************************************************";
echo "******************************************************";
echo "Extracting the Output from HDFS and Placing the Output in the Local Directory";
hdfs dfs -get output001 output_wordcount;
echo "The Output is placed in 'output_wordcount' directory";
echo "******************************************************";
echo "Extracting the Sort Output from HDFS and Placing the Output in the Local Directory";
hdfs dfs -get output002 output_sort;
cho "The Sort output is placed in 'output_sort' directory";
echo "******************************************************";
echo "Stopping Hadoop..."
echo ""
stop-all.sh
echo "******************************************************";
echo ""
echo "Thank you for using Hadoop Word Count";
echo ""