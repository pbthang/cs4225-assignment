/*
 * Name: Pham Ba Thang
 * Matric no: A0219715B
 */

import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TopkCommonWords {
    private static Text fileName1 = new Text();
    private static Text fileName2 = new Text();
    private static HashSet<String> stopwords = new HashSet<>();

    public static class MyMapper extends Mapper<Object, Text, Text, Text>{
        private Text fileName = new Text();
        private Text word = new Text();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            this.fileName.set(((FileSplit) context.getInputSplit()).getPath().getName());
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("[\\t\\n\\r\\f\\s]");
            for(String w: words){
                word.set(w);
                if (!stopwords.contains(w) && !w.isBlank()) {
                    context.write(word, this.fileName);
                }
            }
        }
    }

    public static class MyReducer extends Reducer<Text,Text,IntWritable,Text> {
        SortedMap<Integer, ArrayList<String>> pq = Collections.synchronizedSortedMap(new TreeMap<Integer, ArrayList<String>>());

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count_file1 = 0;
            int count_file2 = 0;

            for (Text fname : values) {
                if (fname.equals(TopkCommonWords.fileName1)) {
                    count_file1++;
                } else if (fname.equals(TopkCommonWords.fileName2)) {
                    count_file2++;
                }
            }

            int minCount = Math.min(count_file1, count_file2);
            if (minCount == 0) return;
            if (pq.containsKey(minCount)) {
                pq.get(minCount).add(key.toString());
            } else {
                ArrayList<String> words = new ArrayList<>();
                words.add(key.toString());
                pq.put(minCount, words);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            int i = 0;
            while (i < 20) {
                Integer count = pq.lastKey();
                if (count == null) break;
                ArrayList<String> words = pq.get(count);
                for (String w : words) {
                    if (i >= 20) break;
                    context.write(new IntWritable(count), new Text(w));
                    i++;
                }
                pq.remove(count);
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        // Initialize file names
        TopkCommonWords.fileName1.set(new Path(args[0]).getName());
        TopkCommonWords.fileName2.set(new Path(args[1]).getName());

        // Initialize stopwords
        Scanner scanner = new Scanner(new File(args[2]));
        while (scanner.hasNextLine()) {
            String word = scanner.nextLine();
            stopwords.add(word);
        }
        scanner.close();

        // Run job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(TopkCommonWords.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    } 
}
