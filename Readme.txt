Shared Memory

1. Compile  
   1. javac WordProcessor.java
   2. javac SortHelper.java
1. Execute 
   1. java WordProcessor 
   2. Please enter the path of file to read
   3. Enter the file to read 
   4. Please enter the no of thread to read the file 
   5. Enter the integer value for no of threads
Sorted Word Count  will be printed on screen.
Note :All these steps are automated using a bash file SharedMemory_bash.sh

Hadoop

The Hadoop source is code is complied using Java Eclipse IDE.

1. Execute
   1. Format HDFS namenode drectroy using: $hdfs namenode -format
   2. Start Hadoop using: $start-all.sh
   3. Create a input directroy: $hdfs dfs -mkdir <dir_name>
   4. Place the input file into hdfs input dir: $hdfs dfs -put $filename <dir_name>
   5. $hadoop jar HadoopWordCount.jar HadoopWordCount <hdfs_input_dir> <hdfs_wordcount_output_dir> <hdfs_sort_output_dir>
   6. $hdfs dfs -get <hdfs_wordcount_output_dir> <local_wc_output_dir>
   7. $hdfs dfs -get <hdfs_sort_output_dir> <local_sort_output_dir>
   8. Stop using : $stop-all.sh
   9. Output files are placed in the output directroy.
Note :All these steps are automated using a bash file hadoop_bash.sh

Swift

1. Compilation   
        1.1 Invoke the makefile script “swift_bash.sh”
              1.2 It will call wordcount.swift
        1.3 Enter the fully qualified filename of the txt file to be read
        1.4 Hit Enter
1. Execution
        2.1 The file is split
        2.2 The bash script “swift_wordcount” is invoked by the swift program.
        2.3 The program is executing and work is divided among the workers by the             
                 head-node.
1. Output
        The output is displayed on the screen.