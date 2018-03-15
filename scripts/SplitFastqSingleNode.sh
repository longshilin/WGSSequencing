#!/usr/bin/env bash

# sh SplitFastqSingleNode.sh input.txt fastq 10000 heart
fileName=$1 # input.txt
hdfsPath=$2 # fastq
numberReads=$3 # 10000
runName=$4 # heart

forwardReads=forwards_reads.txt
reverseReads=reverse_reads.txt

SplitPairedReads(){

SampleName=$1
forwardFile=$2
reverseFile=$3

numLines=$(($numberReads * 4))

# Split forward reads
number=0
fileNumber=$(printf "%04d" "${number}")
count=1
local_file="${SampleName}_${fileNumber}_R1.fastq"
output_file="${hdfsPath}/${SampleName}_${fileNumber}_R1.fastq.gz"

# 对$forwardFile文件中一行一行的读取，将读取的行内容存于$local_file文件中
# 如果numLines=10000，则在满足(10000*4)即每40000行打包成一个文件$local_file
# 通过gzip命令压缩$local_file,得到${local_file}.gz，并将其上传到HDFS上
# [本地文件格式]为 "${SampleName}_${fileNumber}_R1.fastq"
# [HDFS上的文件格式为] "${hdfsPath}/${SampleName}_${fileNumber}_R1.fastq.gz"
# 其中SampleName为input.txt文件中指定的第一个参数，fileNumber是从0开始以+1自增长的序列号，hdfsPath为脚本执行时指定
while IFS= read -r line; do
    if [ $(($count%$numLines)) = 0 ]; then
        echo $line >> $local_file
        gzip $local_file
        hadoop fs -put "${local_file}.gz" $output_file
        printf "${SampleName}_${fileNumber}\t${SampleName}_${fileNumber}_R1.fastq \n">> $forwardReads
        number=$((number + 1))
        fileNumber=$(printf "%04d" "${number}")
        local_file="${SampleName}_${fileNumber}_R1.fastq"
        output_file="${hdfsPath}/${SampleName}_${fileNumber}_R1.fastq.gz"
    else
        echo $line >> $local_file
    fi
    count=$((count + 1))
done < "${forwardFile}"

# 对最后一个不满足numLines要求的文件同样打包及上传
if [ -s $local_file ]
then
    printf "${SampleName}_${fileNumber}\t${SampleName}_${fileNumber}_R1.fastq \n">> $forwardReads
    gzip $local_file
    hadoop fs -put "${local_file}.gz" $output_file
fi
# Split reverse reads
number=0
fileNumber=$(printf "%04d" "${number}")
count=1
local_file="${SampleName}_${fileNumber}_R2.fastq"
output_file="${hdfsPath}/${SampleName}_${fileNumber}_R2.fastq.gz"

while IFS= read -r line; do
    if [ $(($count%$numLines)) = 0 ]; then
        echo $line >> $local_file
        gzip $local_file
        hadoop fs -put "${local_file}.gz" $output_file
        printf "${SampleName}_${fileNumber}_R2.fastq \n" >> $reverseReads
        number=$((number + 1))
        fileNumber=$(printf "%04d" "${number}")
        local_file="${SampleName}_${fileNumber}_R2.fastq"
        output_file="${hdfsPath}/${SampleName}_${fileNumber}_R2.fastq.gz"

    else
        echo $line >> $local_file
    fi
    count=$((count + 1))
done < "${reverseFile}"

if [ -s $local_file ]
then
    printf "${SampleName}_${fileNumber}_R2.fastq \n" >> $reverseReads
    gzip $local_file
    hadoop fs -put "${local_file}.gz" $output_file
fi

# 当所有打包文件上传到HDFS后，删除本地打包文件
deleteFiles="${SampleName}_*_R*.fastq.gz"
rm -rf $deleteFiles

}

while IFS= read -r line; do
    a=$(echo $line | tr ',' "\n")
    SplitPairedReads ${a[0]} ${a[1]} ${a[2]}
done < $fileName
# 在$fileName中指明SplitPairedReads的这三个参数，分别为SampleName、forwardFile、reverseFile
# 分别对应样本名称、R1序列和R2序列，因为DNA是两条互补的链

fileList="${runName}_fastq_files.txt"
output_fileList="${hdfsPath}/${runName}_fastq_files.txt"

# 将已打包上传的总的文件列表收录在fileList中，即"heart_fastq_files.txt"
# paste将按行将不同文件行信息放在一行
paste $forwardReads $reverseReads > $fileList

# 同样上传至HDFS上的 "fastq/heart_fastq_files.txt"
hadoop fs -put $fileList $output_fileList

#------ Clean up
rm -rf $forwardReads
rm -rf $reverseReads
rm -rf $fileList

