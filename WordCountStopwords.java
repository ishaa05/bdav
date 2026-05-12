import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountStopwords {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        private Text word = new Text();

        private Set<String> stopwords = new HashSet<>();

        @Override
        protected void setup(Context context)
                throws IOException {

            Configuration conf = context.getConfiguration();

            FileSystem fs = FileSystem.get(conf);

            BufferedReader br = new BufferedReader(
                    new InputStreamReader(
                            fs.open(new Path("/stopwords.txt"))
                    )
            );

            String line;

            while ((line = br.readLine()) != null) {
                stopwords.add(line.trim());
            }

            br.close();
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().toLowerCase();

            line = line.replaceAll("[^a-zA-Z ]", "");

            StringTokenizer itr = new StringTokenizer(line);

            while (itr.hasMoreTokens()) {

                String token = itr.nextToken();

                if (!stopwords.contains(token)) {

                    word.set(token);

                    context.write(word, one);
                }
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);

            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "wordcount stopwords");

        job.setJarByClass(WordCountStopwords.class);

        job.setMapperClass(TokenizerMapper.class);

        job.setCombinerClass(IntSumReducer.class);

        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


//to compile
//javac -classpath $(hadoop classpath) -d . WordCountStopwords.java
// jar -cvf wcstop.jar *

//run
//hadoop jar wcstop.jar WordCountStopwords /books /output_stop