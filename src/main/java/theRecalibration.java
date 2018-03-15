import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class theRecalibration extends Configured implements Tool {

    public static class theRecalibrationMapper extends Mapper<Object, Text, Text, Text> {

        private Text jobID = new Text();
        private Text inputLineValue = new Text();
        /**
         * 在再校正步骤中，每个map()函数会处理一个特定的已比对染色体。
         * 映射器会完成重复标记、本地重新比对和再校正
         *
         * @param key     由MR程序生成 忽略
         * @param value   chrID;run_name\tjobID
         * @param context 为reducer传递一个analysisID，相当于每个analysisID只有一个规约器
         */
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            int id = context.getTaskAttemptID().getTaskID().getId();
            jobID.set(String.valueOf(id));
            try {
                DNASeq.recalibrationMapper(value.toString());
            } catch (Exception e) {
                e.printStackTrace();
            }
            inputLineValue.set(key);
            context.write(key, inputLineValue);
        }

    }

    @Override
    public int run(String[] args) throws Exception {
        // 这里的input是上一个步骤最后reducer输出的文件
        if (args.length != 2) {
            System.err.println("Usage: theRecalibration <input file> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        // 输入文件的每一行用一个mapper来处理
        conf.setInt(NLineInputFormat.LINES_PER_MAP, 1);

        Job job = new Job(conf, "Recalibaration");
        job.setJarByClass(theRecalibrationMapper.class);


        job.setInputFormatClass(NLineInputFormat.class);
        job.setMapperClass(theRecalibrationMapper.class);

        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        Path outPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outPath);
        outPath.getFileSystem(conf).delete(outPath, true); // 提前删除已存在的输出文件夹

        job.waitForCompletion(true); // 提交作业

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    // 函数主入口
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new theRecalibration(), args);
        System.exit(exitCode);
    }
}