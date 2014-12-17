echo "Enter FQN file name"
read file
echo "*******************************************"
echo "Creating Input Directory"
mkdir inputs
cd inputs
echo "*******************************************"
echo "Splitting the files"
size=$(wc -l $file | awk '{print $1}')
split -l $(($size/500)) $file
cd ..
echo "*******************************************"
echo "Starting Swift Word Count"
start_date=$(date +"%s")
swift wordcount.swift
end_date=$(date +"%s")
diff=$(($end_date-$start_date))
echo "*******************************************"
echo "Time taken to Hashmap and Sort: $(($diff / 60)) min $(($diff % 60)) sec $(($diff % 1000)) ms"
echo "*******************************************"
echo "Thank you for using Swift Word Count"
