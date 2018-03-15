import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class theComparison extends Configured implements Tool {

    public static class theComparisonMapper extends Mapper<Object, Text, Text, IntWritable> {


        private Text word = new Text();

        /**
         * 比对阶段map()函数
         * 输入文件fastq_fileList.txt中每一行表示一个分区去的FASTQ文件对
         * 每一行生成一个map任务，有多少个分区就生成多少个map任务
         *
         * @param key     是MapReduce框架生成的一个键,相当于输入文件中每行最开始的偏移量
         * @param value   ${SampleName}_${fileNumber}\t${SampleName}_${fileNumber}_R1.fastq ${SampleName}_${fileNumber}_R2.fastq
         * @param context 输出23条染色体的key,value对,表示分23各Reducer来处理各染色体的后续步骤
         */
        public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

            Configuration mapper_conf = context.getConfiguration();
            String run_name = mapper_conf.get("run_name"); // "heart"
            DNASeq.comparisonMapper(value.toString(), run_name);
            for (int i = 1; i < 24; i++) {
                word.set("chr" + i + ";" + run_name);
                // mapper输出 <chrID>;run_name形式，限定Reducer个数为23个,对应23个染色体分区
                context.write(word, null);
            }
        }
    }


    public static class theComparisonReducer extends Reducer<Text, IntWritable, Text, Text> {

        private Text jobID = new Text();
        private Text inputLineValue = new Text();

        /**
         * 比对阶段的reduce()函数
         * 对map阶段已比对的bam文件按照染色体号进行分区 并合并为一个大的chri.bam文件
         *
         * @param key     chrID;run_name
         * @param values  null
         * @param context
         */
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int id = context.getTaskAttemptID().getTaskID().getId();
            jobID.set(String.valueOf(id));
            try {
                DNASeq.comparisonReducer(key.toString());
            } catch (Exception e) {
                e.printStackTrace();
            }
            inputLineValue.set(key);
            context.write(key, inputLineValue);
        }
    }


    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 5) {
            System.err.println("Usage: BwaMapping <input file(runName)> <output path> <fastq_loc>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        // 输入文件的每一行用一个mapper来处理
        conf.setInt(NLineInputFormat.LINES_PER_MAP, 1);

        String runName = args[0]; // heart
        conf.set("run_name", args[0]);
        String output = args[1]; // BwaMappingOut
        conf.set("input_fastq_loc", args[3]); //"fastq"

        StringBuffer runName_temp = new StringBuffer(args[3]);
        runName_temp.append("/");
        runName_temp.append(runName);
        runName_temp.append("_fastq_files.txt");

        // "fastq/heart_fastq_files.txt" 其中包含所有的分块的文件名称，
        // 其中一行数据格式如：${SampleName}_${fileNumber}\t${SampleName}_${fileNumber}_R1.fastq ${SampleName}_${fileNumber}_R2.fastq
        String input = runName_temp.toString();

        Job job = new Job(conf, "BwaMapping");
        job.setJarByClass(theComparisonMapper.class);


        job.setInputFormatClass(NLineInputFormat.class);
        job.setMapperClass(theComparisonMapper.class);

        // 设置reducer为23个
        job.setNumReduceTasks(23);
        job.setReducerClass(theComparisonReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        Path outPath = new Path(output);
        FileOutputFormat.setOutputPath(job, outPath);
        outPath.getFileSystem(conf).delete(outPath, true); // 提前删除已存在的输出文件夹

        job.waitForCompletion(true); // 提交作业

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    // 函数主入口
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new theComparison(), args);
        System.exit(exitCode);
    }
}