#!/usr/bin/env bash

###################################################
#   Description: 将DNA序列的FASTQ格式文件按等长序列切分 #
#   Author:elon                                   #
#   Version:1.0                                   #
#   CreateTime:2018-3-14                          #
###################################################

# 输入要切分的样本名称
sampleName=$1
# 由于FASTQ数据是成对的，所以输入就是一个文件对：
#   forwardFile.fastq和reverseFile.fastq
forwardFile=$2
reverseFile=$3
# 指定每个分区中包含的DNA序列数
# 每个分区文件共计{numberReads*4}行数据
numberReads=$4

# 记录分区文件名列表
export forwardReads="forwardFile.txt"
export reverseReads="reverseFile.txt"
# 记录hdfs上传目录
export hdfsPath="/dnaseq/fastq"

# 每个分区文件总行数
numLines=$(($numberReads * 4))

#--- 对forwardFile进行分区 ---
fileNo=0
count=1
# 按0000,0001,0002,...格式输出
partitionNo=$(printf "%04d" "${fileNo}")
local_file="${sampleName}_R1.fastq.${partitionNo}"
output_file="${hdfsPath}/${sampleName}_R1.fastq.${partitionNo}.gz"

while IFS= read -r line; do
    if [ $(($count%$numLines)) -eq 0 ]; then
        echo ${line} >> ${local_file}
        gzip ${local_file}
        hadoop fs -put "${local_file}.gz" ${output_file}
        printf "${sampleName}_${partitionNo}\t${sampleName}_R1.fastq.${partitionNo} \n">> ${forwardReads}
        fileNo=$((fileNo + 1))
        partitionNo=$(printf "%04d" "${fileNo}")
        local_file="${sampleName}_R1.fastq.${partitionNo}"
        output_file="${hdfsPath}/${sampleName}_R1.fastq.${partitionNo}.gz"
    else
        echo ${line} >> ${local_file}
    fi
    count=$((count + 1))
done < "${forwardFile}"

if [ -s ${local_file} ]; then
    printf "${sampleName}_${partitionNo}\tR1.fastq.${partitionNo} \n">> ${forwardReads}
    gzip ${local_file}
    hadoop fs -put "${local_file}.gz" ${output_file}
fi
#--- end forwardFile ---

#--- 对reverseFile进行分区 ---
fileNo=0
count=1
partitionNo=$(printf "%04d" "${fileNo}")
local_file="${sampleName}_R2.fastq.${partitionNo}"
output_file="${hdfsPath}/${sampleName}_R2.fastq.${partitionNo}"

while IFS= read -r line; do
    if [ $(($count%$numLines)) -eq 0 ]; then
        echo ${line} >> ${local_file}
        gzip ${local_file}
        hadoop fs -put "${local_file}.gz" ${output_file}
        printf "${sampleName}_R2.fastq.${partitionNo} \n" >> ${reverseReads}
        fileNo=$((fileNo + 1))
        partitionNo=$(printf "%04d" "${fileNo}")
        local_file="${sampleName}_R2.fastq.${partitionNo}"
        output_file="${hdfsPath}/${sampleName}_R2.fastq.${partitionNo}.gz"

    else
        echo ${line} >> ${local_file}
    fi
    count=$((count + 1))
done < "${reverseFile}"

if [ -s ${local_file} ]; then
    printf "${sampleName}_R2.fastq.${partitionNo} \n" >> ${reverseReads}
    gzip ${local_file}
    hadoop fs -put "${local_file}.gz" ${output_file}
fi
#--- end reverseFile ---

# 当所有打包文件上传到HDFS后，删除本地gz文件
deleteFiles="R*.fastq.gz"
rm -rf ${deleteFiles}

#--- 合并文件列表 ---
fileList="fastq_files.txt"
output_fileList="${hdfsPath}/fastq_fileList.txt"
paste ${forwardReads} ${reverseReads} > ${fileList}
hadoop fs -put ${fileList} ${output_fileList}
#--- end merge ---

# Clean up
rm -rf ${forwardReads}
rm -rf ${reverseReads}
rm -rf ${fileList}
