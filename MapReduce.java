package mavend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class MapReduce {
    public static class MayMmAPPER extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //contains里面写你要查找的关键字
//            if (value.toString().contains("guanjianzi")){
//                context.write(new Text("resonse"), new IntWritable(1));//有一次写一次
//        }else if (value.toString().contains("guanjianzi   row")){
//            context.write(new Text("taotal "),  new IntWritable(1));
//            }
            String[] values = value.toString().split(" ");
            for (String s : values) {
                context.write(new Text(s), new IntWritable(1));

            }
        }
    }

        //readuce
        public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

            @Override
            protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                int sum = 0;
                for (IntWritable i : values) {
                    sum = sum + i.get();
                }

                context.write(key, new IntWritable(sum));
            }
        }

        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
            //声明使用job工具
            Configuration configuration = new Configuration();
            Job job = Job.getInstance(configuration);
            job.setJarByClass(MapReduce.class);
//按照之前定义的class进行mapreduce的操作
            job.setMapperClass(MayMmAPPER.class);
            job.setReducerClass(MyReducer.class);
//设置intput的参数
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
//设置output的输入参数
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
//我们自己设置输出的类型（map）
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
//我们自己设置输出的类型（reduce）
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.waitForCompletion(true);//等待进行下一步操作
        }
}
