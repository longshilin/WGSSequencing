import java.io.*;
import java.io.IOException;
import java.util.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.LogManager;


public class BwaMapping extends Configured implements Tool{

    private static Logger theLogger = Logger.getLogger(BwaMapping.class.getName());

    public static class BwaMappingMap extends Mapper<LongWritable, Text, Text, Text> {
        
        private Text jobID = new Text();
        private Text inputLineValue = new Text();

        /**
         * 一个map()程序操作一行数据，即对一行数据进行比对操作，比对命令嵌套在java对象中
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            int id = context.getTaskAttemptID().getTaskID().getId();
            jobID.set(String.valueOf(id));
            ProcessBuilder pb;
            Process process;
            File errorFile;
            File outputFile;
            
            File deleteFile;
            boolean deleteFile_exists;

            Configuration mapper_conf = context.getConfiguration();
            String fastq_loc = mapper_conf.get("input_fastq_loc"); // "fastq"
            String bwa_db = mapper_conf.get("input_bwa_db"); // "genomes/bwa.tar.gz"
            String mapq = mapper_conf.get("input_mapq"); // 10
            
            /**
             NOTE:
             当在相同节点有多个重复的mapper时，bwa.tar.gz在本地结点已存在时，无需重新在hdfs上download下来
             该压缩包中包括参考基因组hg19文件
             下载并解压hdfs上的genomes/bwa.tar.gz文件，得到本地文件bwa.tar.gz以及本地文件目录bwa/hg19.fa
            **/
            
            File bwa_db_compressed = new File("bwa.tar.gz");
            boolean bwa_db_exists = bwa_db_compressed.exists();
            if(bwa_db_exists){
                theLogger.info("NOTE: bwa.tar.gz already downloaded");
            }else{
                pb = new ProcessBuilder("hadoop","fs","-get",bwa_db,"bwa.tar.gz");
                pb.inheritIO();
                theLogger.info("Command executed: " + pb.command());
                process = pb.start();
                process.waitFor();
                
                pb = new ProcessBuilder("tar","zxvf","bwa.tar.gz");
                pb.inheritIO();
                theLogger.info("Command executed: " + pb.command());
                process = pb.start();
                process.waitFor();
            }

            /**
             * 用于检测是否真正解压得到了该文件"bwa/hg19.fa"
             */

            File bwa_db_fa = new File("bwa/hg19.fa");
            boolean bwa_db_fa_exists = bwa_db_fa.exists();
            
            if(bwa_db_fa_exists){
                theLogger.info("NOTE: bwa/hg19.fa exists");
            }else{
                // If we got into this clause, it means bwa.tar.gz exists but
                // it hasn't been uncompressed. This is an odd situation because
                // the clause that downloaded it should have uncompressed it. We
                // will go ahead an uncompress it again.
                pb = new ProcessBuilder("tar","zxvf","bwa.tar.gz");
                pb.inheritIO();
                theLogger.info("Command executed: " + pb.command());
                process = pb.start();
                process.waitFor();
            }


            /**
             * 构造R1和R2文件对象
             */
            // Get input with sample name and name of fastq files
            String inputLine = value.toString(); // value为 "fastq/heart_fastq_files.txt"文件中的每一行，MapReduce中的Map方法自动给我们提取出来了每行为一个value
            String delimiter = "\\s+"; // 匹配任何多个空白字符，包括空格、制表符、换行符等等。
            //inputLine：${SampleName}_${fileNumber}\t${SampleName}_${fileNumber}_R1.fastq ${SampleName}_${fileNumber}_R2.fastq
            String [] strArray = inputLine.split(delimiter);
            String sample_name = strArray[0];
            
            // Reconstruct name of fastq files
            StringBuffer fastq_temp=new StringBuffer(fastq_loc); // fastq
            fastq_temp.append("/");
            fastq_temp.append(sample_name);
            fastq_temp.append("*");
            String fastq = fastq_temp.toString(); // fastq="fastq/${SampleName}_${fileNumber}*"
            
            // R1 file
            StringBuffer fastq_R1_temp=new StringBuffer(strArray[1]);
            fastq_R1_temp.append(".gz");
            String fastq_R1 = fastq_R1_temp.toString(); // fastq_R1="${SampleName}_${fileNumber}_R1.fastq.gz"
            
            // R2 file
            StringBuffer fastq_R2_temp=new StringBuffer(strArray[2]);
            fastq_R2_temp.append(".gz");
            String fastq_R2 = fastq_R2_temp.toString(); // fastq_R2="${SampleName}_${fileNumber}_R2.fastq.gz"

            /**
             * 删除上一个mapper遗留下来的R1.fastq、R1.fastq.gz 和 R2.fastq、R2.fastq.gz
             */
            // Delete R1 fastq file first if it exists
            deleteFile = new File(fastq_R1);
            deleteFile_exists = deleteFile.exists();
            if(deleteFile_exists){
                pb = new ProcessBuilder("rm","-rf",fastq_R1);
                pb.inheritIO();
                theLogger.info("Command executed: " + pb.command());
                process = pb.start();
                process.waitFor();
            }
            
            deleteFile = new File(strArray[1]);
            deleteFile_exists = deleteFile.exists();
            if(deleteFile_exists){
                pb = new ProcessBuilder("rm","-rf",strArray[1]);
                pb.inheritIO();
                theLogger.info("Command executed: " + pb.command());
                process = pb.start();
                process.waitFor();
            }
            // Delete R2 fastq file first if it exists
            deleteFile = new File(fastq_R2);
            deleteFile_exists = deleteFile.exists();
            if(deleteFile_exists){
                pb = new ProcessBuilder("rm","-rf",fastq_R2);
                pb.inheritIO();
                theLogger.info("Command executed: " + pb.command());
                process = pb.start();
                process.waitFor();
            }
            
            deleteFile = new File(strArray[2]);
            deleteFile_exists = deleteFile.exists();
            if(deleteFile_exists){
                pb = new ProcessBuilder("rm","-rf",strArray[2]);
                pb.inheritIO();
                theLogger.info("Command executed: " + pb.command());
                process = pb.start();
                process.waitFor();
            }

            /**
             * 下载并解压sample_name文件
             */
            // Get fastq files from hdfs 从HDFS上获取sample_name文件中的两个文件fastq_R1和fastq_R2
            pb = new ProcessBuilder("hadoop","fs","-get",fastq,".");
            pb.inheritIO();
            theLogger.info("Command executed: " + pb.command());
            process = pb.start();
            process.waitFor();
            
            // Uncompress fastq file
            
            pb = new ProcessBuilder("gunzip",fastq_R1);
            pb.inheritIO();
            theLogger.info("Command executed: " + pb.command());
            process = pb.start();
            process.waitFor();
            
            
            pb = new ProcessBuilder("gunzip",fastq_R2);
            pb.inheritIO();
            theLogger.info("Command executed: " + pb.command());
            process = pb.start();
            process.waitFor();
            
            // Find out the number of processors
            int cores = Runtime.getRuntime().availableProcessors();
            
            /** Build the command
             NOTE: when you exec a system command from a Java application, you don't 
             actually get a Unix or Linux shell to run your command in. You're 
             really just running the command without a shell wrapper. So, to use a feature 
             like a Unix/Linux pipe (pipeline) -- which is a shell feature -- you 
             have to invoke a shell, and then run your commands inside that shell.
             
             -c string If  the  -c  option  is  present, then commands are read from
             string.  The -c options enables us to pass the command as a string
             
             The - symbol in samtools (after -bS) is to tell the samtools program to 
             take the input from pipe

             执行基因比对命令
            **/

            // Command: bwa mem -M -t ${cores} bwa/hg19_canonical.fa ${SampleName}_${fileNumber}_R1.fastq ${SampleName}_${fileNumber}_R2.fastq /
            //              | samtools view -q ${mapq}  -bS -
            // 对R1和R2进行bwa比对，并筛选出reads比对质量大于10的
            StringBuffer cmd_temp=new StringBuffer("bwa mem -M -t ");
            cmd_temp.append(String.valueOf(cores));
            cmd_temp.append(" bwa/hg19_canonical.fa ");
            cmd_temp.append(strArray[1]);
            cmd_temp.append(" ");
            cmd_temp.append(strArray[2]);
            cmd_temp.append(" | samtools view -q ");
            cmd_temp.append(mapq);
            cmd_temp.append(" -bS - ");
            String piped_cmd = cmd_temp.toString();
            pb = new ProcessBuilder("/bin/sh","-c",piped_cmd);
            //inherit IO
            pb.inheritIO();
            // Re-direct output to a file
            StringBuffer outF_temp=new StringBuffer(sample_name);
            outF_temp.append(".bam");
            outputFile = new File(outF_temp.toString()); // ${SampleName}_${fileNumber}.bam
            pb.redirectOutput(outputFile);
            // Re-direct error to a file
            StringBuffer outE_temp=new StringBuffer(sample_name);
            outE_temp.append(".log");
            errorFile = new File(outE_temp.toString()); // ${SampleName}_${fileNumber}.log
            pb.redirectError(errorFile);
            // Print the command to the screen
            // bwa mem -M -t ${cores} bwa/hg19_canonical.fa ${SampleName}_${fileNumber}_R1.fastq ${SampleName}_${fileNumber}_R2.fastq /
            //        | samtools view -q ${mapq}  -bS -
            System.out.println("Command executed: " + pb.command());
            // Run command and wait for it to finish
            process = pb.start();
            process.waitFor();

            /**
             * 删除解压的R1.fastq和R2.fastq
             */
            // Clean-up (delete fastq files)
            deleteFile = new File(strArray[1]);
            deleteFile_exists = deleteFile.exists();
            if(deleteFile_exists){
                pb = new ProcessBuilder("rm","-rf",strArray[1]);
                pb.inheritIO();
                theLogger.info("Command executed: " + pb.command());
                process = pb.start();
                process.waitFor();
            }
        
            deleteFile = new File(strArray[2]);
            deleteFile_exists = deleteFile.exists();
            if(deleteFile_exists){
                pb = new ProcessBuilder("rm","-rf",strArray[2]);
                pb.inheritIO();
                theLogger.info("Command executed: " + pb.command());
                process = pb.start();
                process.waitFor();
            }

            /**
             * mappers 端的输出：job任务ID 处理的数据对（sampleName+fileNumber）
             */
            inputLineValue.set(sample_name);
            context.write(jobID,inputLineValue);
            
        } // end of map method
	
    }// end of mapper class

    @Override
    public int run(String[] args) throws Exception {
        
        if (args.length != 5) {
            System.err.println("Usage: BwaMapping <input file> <output path> <bwa_Refseq_db_hdfs_path> <fastq_location_hdfs_path> <mapq>");
            System.exit(-1);
        }
        
        Configuration conf = new Configuration();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        // One line per mapper
        conf.setInt(NLineInputFormat.LINES_PER_MAP, 1);

        String runName = args[0]; // heart
        String output = args[1]; // BwaMappingOut
        conf.set("input_bwa_db",args[2]); // "genomes/bwa.tar.gz"  给出参考基因组在hdfs上的位置
        conf.set("input_fastq_loc",args[3]); //"fastq"  给出fastq文件的位置
        conf.set("input_mapq",args[4]); //10  samtools view:  -q INT   only include reads with mapping quality >= INT [0]
        
        StringBuffer runName_temp=new StringBuffer(args[3]);
        runName_temp.append("/");
        runName_temp.append(runName);
        runName_temp.append("_fastq_files.txt");

        // "fastq/heart_fastq_files.txt" 其中包含所有的分块的文件名称，
        // 其中一行数据格式如：${SampleName}_${fileNumber}\t${SampleName}_${fileNumber}_R1.fastq ${SampleName}_${fileNumber}_R2.fastq
        String input = runName_temp.toString();
        
        Job job = new Job(conf, "BwaMapping");
        job.setJarByClass(BwaMappingMap.class);
        
        
        job.setInputFormatClass(NLineInputFormat.class);
        job.setMapperClass(BwaMappingMap.class);

        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        Path outPath = new Path(output);
        FileOutputFormat.setOutputPath(job, outPath);
        outPath.getFileSystem(conf).delete(outPath, true); // 提前删除已存在的输出文件夹
        
        job.waitForCompletion(true); // 提交作业
        
        return (job.waitForCompletion(true) ? 0 : 1);
        
    }
    
    public static void main(String[] args) throws Exception {
        
        int exitCode = ToolRunner.run(new BwaMapping(), args);
        System.exit(exitCode);
    }
}
