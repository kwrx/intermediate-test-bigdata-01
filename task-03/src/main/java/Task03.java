import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

public class Task03 {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


        @Override
        protected void map(LongWritable key, Text value, Context context) {

            String[] line = value.toString().split(",");

            String name = line[0];
            String surname = line[1];
            String age = line[2];
            String city = line[3];
            String province = line[4];
            String skillName = line[5];
            String skillDescription = line[6];
            int skillLevel = Integer.parseInt(line[7]);

            if(skillLevel > 5) {
                try {
                    context.write(new Text(name + " " + surname), new IntWritable(skillLevel));
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

        }

    }

    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {


        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable value : values) {
                sum += value.get();
            }

            if(sum >= 2) {
                context.write(key, new IntWritable(sum));
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

        }

    }

    public static void main(String[] args) throws Exception {

        Job job = Job.getInstance(new Configuration(), "Average");
        job.setJarByClass(Task03.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath((JobConf) job.getConfiguration(), new Path(args[0]));
        FileOutputFormat.setOutputPath((JobConf) job.getConfiguration(), new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }


}
