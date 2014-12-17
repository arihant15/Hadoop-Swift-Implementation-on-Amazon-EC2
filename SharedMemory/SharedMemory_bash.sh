echo "Please add the path of file to read";
read fileName;
echo "Enter the no of Threads to read the file"
read nt;
java -jar WordProcessor.jar $fileName $nt;
