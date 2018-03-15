#!/usr/bin/env bash

###
### 1.bwa软件建立参考序列索引文件
###

cd ~/reference
mkdir -p index/bwa && cd index/bwa
nohup time ~/biosoft/bwa/bwa-0.7.15/bwa index   -a bwtsw   -p ~/reference/index/bwa/hg19  ~/reference/genome/hg19/hg19.fa 1>hg19.bwa_index.log 2>&1   &


###
### 2.FASTQ文件切分及上传
###

hadoop fs -rm -r fastq

hadoop fs -mkdir fastq


# 在单节点上执行fastq文件的切分
# 样本文件名 hdfs上传路径名 每个分区存放的序列条数 脚本运行代号名称
sh SplitFastqSingleNode.sh input.txt fastq 10000 heart


###
### 3.使用MapReduce并行计算框架 进行测序比对
###
# 对BwaMapping进行编译
hadoop com.sun.tools.javac.Main theComparison.java
# 对BwaMapping进行打包
jar cf theComparison.jar *.class

# 对应输入名（运行名称） 输出文件名 参考序列存储的hdfs路径 fastq文件所在的文件名 片段比对质量控制(大于指定值的才比对出来)
# Usage: theComparison <input file> <output path> <bwa_seq_db_hdfs_path> <fastq_loc> <mapq>
hadoop jar theComparison.jar BwaMapping heart /output/theComparisonOut "genomes/bwa.tar.gz" "fastq" 10
# 得出每个R1和R2比对的bam文件

###
### 5.重复标记 本地重新比对
###
# 对BwaMapping进行编译
hadoop com.sun.tools.javac.Main theRecalibration.java
# 对BwaMapping进行打包
jar cf theRecalibration.jar *.class

# Usage: theRecalibration <input file> <output path>
hadoop jar theRecalibration.jar theRecalibration /output/theComparisonOut /output/theRecalibrationOut


###
### 6.重复标记 本地重新比对
###
# 对theVariantDetection进行编译
hadoop com.sun.tools.javac.Main theVariantDetection.java
# 对theVariantDetection进行打包
jar cf theVariantDetection.jar *.class

# Usage: theVariantDetection <input file> <output path>
hadoop jar theVariantDetection.jar theVariantDetection /output/theRecalibrationOut /output/theVariantDetectionOut
# 得出VCF文件