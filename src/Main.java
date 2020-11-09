import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.log4j.Logger;

public class Main extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Main(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "accidentscount");
        job.setJarByClass(this.getClass());
        // Use TextInputFormat, the default unless job.setInputFormatClass is used
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        private MapKey mapKey;
        private IntWritable size = new IntWritable();

        private ArrayList<String> streets = new ArrayList<>();
        private ArrayList<String> typesParticipants = new ArrayList<>(Arrays.asList("PEDESTRIAN", "CYCLIST", "MOTORIST"));
        private ArrayList<String> characters = new ArrayList<>(Arrays.asList("INJURED", "KILLED"));
        private int zipcode;
        private int injuredPedestrians = 0;
        private int killedPedestrians = 0;
        private int injuredCyclist = 0;
        private int killedCyclist = 0;
        private int injuredMotorist = 0;
        private int killedMotorist = 0;


        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


            if (key.get() == 0) {
                return;
            }

            String line = value.toString();
            int i = 0;
            for (String word : line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")) {
                if (i == 0) {
                    String year = word.substring(word.lastIndexOf('/') + 1, word.lastIndexOf('/') + 5);
                    if (Integer.parseInt(year) <= 2012) {
                        return;
                    }
                }
                if (i == 2) {
                    if (word.length() > 0) {
                        zipcode = Integer.parseInt(word);
                    }
                }
                //--------------------------------------------------- STREET
                if (i == 6) {
                    if (word.length() > 0) {
                        streets.add(word);
                    }
                }
                if (i == 7) {
                    if (word.length() > 0) {
                        streets.add(word);
                    }
                }
                if (i == 8) {
                    if (word.length() > 0) {
                        streets.add(word);
                    }
                }
                //--------------------------------------------------- STREET
                if (i == 11) { // injured pedestrians
                    injuredPedestrians++;
                }
                if (i == 12) { // killed pedestrians
                    killedPedestrians++;
                }
                if (i == 13) { // injured cyclist
                    injuredCyclist++;
                }
                if (i == 14) { // killed cyclist
                    killedCyclist++;
                }
                if (i == 15) { // injured motorist
                    injuredMotorist++;
                }
                if (i == 16) { // killed motorist
                    killedMotorist++;
                }
                i++;
            }

            for (String street : streets) {
                mapKey = new MapKey();
                mapKey.setStreet(new Text(street));
                mapKey.setZipcode(new IntWritable(zipcode));

                mapKey.setCharackerOfAccident(new Text("INJURED"));
                mapKey.setTypeParticipant(new Text("PEDESTRIAN"));
                context.write(new Text(mapKey.toString()), new IntWritable(injuredPedestrians));

                mapKey.setCharackerOfAccident(new Text("KILLED"));
                mapKey.setTypeParticipant(new Text("PEDESTRIAN"));
                context.write(new Text(mapKey.toString()), new IntWritable(killedPedestrians));

                mapKey.setCharackerOfAccident(new Text("INJURED"));
                mapKey.setTypeParticipant(new Text("CYCLIST"));
                context.write(new Text(mapKey.toString()), new IntWritable(injuredCyclist));

                mapKey.setCharackerOfAccident(new Text("KILLED"));
                mapKey.setTypeParticipant(new Text("CYCLIST"));
                context.write(new Text(mapKey.toString()), new IntWritable(killedCyclist));

                mapKey.setCharackerOfAccident(new Text("INJURED"));
                mapKey.setTypeParticipant(new Text("MOTORIST"));
                context.write(new Text(mapKey.toString()), new IntWritable(injuredMotorist));

                mapKey.setCharackerOfAccident(new Text("KILLED"));
                mapKey.setTypeParticipant(new Text("MOTORIST"));
                context.write(new Text(mapKey.toString()), new IntWritable(killedMotorist));
            }

        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
        Float average;
        Float count;
        int sum;

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            average = 0f;
            count = 0f;
            sum = 0;
            Text sumText = new Text("average size of station for " + key + " year is: ");
            for (IntWritable val : values) {
                sum += val.get();
                count += 1;
            }
            average = sum / count;
            result.set(average);
            context.write(sumText, result);
        }
    }
}