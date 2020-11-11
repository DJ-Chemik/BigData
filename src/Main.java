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

        private SimpleMapKey mapKey = new SimpleMapKey();
        private IntWritable size = new IntWritable();

        private ArrayList<String> streets = new ArrayList<>();
        private ArrayList<String> typesParticipants = new ArrayList<>(Arrays.asList("PEDESTRIAN", "CYCLIST", "MOTORIST"));
        private ArrayList<String> characters = new ArrayList<>(Arrays.asList("INJURED", "KILLED"));
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

//            String[] line = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            String[] line = value.toString().split(",");

            if (line[0].length() > 0) {
                String year = line[0].substring(line[0].lastIndexOf('/') + 1, line[0].lastIndexOf('/') + 5);
                if (Integer.parseInt(year) <= 2012) {
                    return;
                }
            }
            if (line.length < 17) {
                return;
            }
            if (line[2].length() > 0) {
                System.out.println("--------------------------------------------------------" + line[2]);
                mapKey.setZipcode(Integer.parseInt(line[2]));
            }
            injuredPedestrians = Integer.parseInt(line[11]);
            killedPedestrians = Integer.parseInt(line[12]);
            injuredCyclist = Integer.parseInt(line[13]);
            killedCyclist = Integer.parseInt(line[14]);
            injuredMotorist = Integer.parseInt(line[15]);
            killedMotorist = Integer.parseInt(line[16]);
            //--------------------------------------------------- STREET
            if (line[6].length() > 0) {
                mapKey.setStreet(line[6]);

                mapKey.setCharackerOfAccident("INJURED");
                mapKey.setTypeParticipant("PEDESTRIAN");
                context.write(new Text(mapKey.toString()), new IntWritable(injuredPedestrians));
                mapKey.setCharackerOfAccident("KILLED");
                mapKey.setTypeParticipant("PEDESTRIAN");
                context.write(new Text(mapKey.toString()), new IntWritable(killedPedestrians));
                mapKey.setCharackerOfAccident("INJURED");
                mapKey.setTypeParticipant("CYCLIST");
                context.write(new Text(mapKey.toString()), new IntWritable(injuredCyclist));
                mapKey.setCharackerOfAccident("KILLED");
                mapKey.setTypeParticipant("CYCLIST");
                context.write(new Text(mapKey.toString()), new IntWritable(killedCyclist));
                mapKey.setCharackerOfAccident("INJURED");
                mapKey.setTypeParticipant("MOTORIST");
                context.write(new Text(mapKey.toString()), new IntWritable(injuredMotorist));
                mapKey.setCharackerOfAccident("KILLED");
                mapKey.setTypeParticipant("CYCLIST");
                context.write(new Text(mapKey.toString()), new IntWritable(killedMotorist));
            }
            if (line[7].length() > 0) {
                mapKey.setStreet(line[7]);

                mapKey.setCharackerOfAccident("INJURED");
                mapKey.setTypeParticipant("PEDESTRIAN");
                context.write(new Text(mapKey.toString()), new IntWritable(injuredPedestrians));
                mapKey.setCharackerOfAccident("KILLED");
                mapKey.setTypeParticipant("PEDESTRIAN");
                context.write(new Text(mapKey.toString()), new IntWritable(killedPedestrians));
                mapKey.setCharackerOfAccident("INJURED");
                mapKey.setTypeParticipant("CYCLIST");
                context.write(new Text(mapKey.toString()), new IntWritable(injuredCyclist));
                mapKey.setCharackerOfAccident("KILLED");
                mapKey.setTypeParticipant("CYCLIST");
                context.write(new Text(mapKey.toString()), new IntWritable(killedCyclist));
                mapKey.setCharackerOfAccident("INJURED");
                mapKey.setTypeParticipant("MOTORIST");
                context.write(new Text(mapKey.toString()), new IntWritable(injuredMotorist));
                mapKey.setCharackerOfAccident("KILLED");
                mapKey.setTypeParticipant("CYCLIST");
                context.write(new Text(mapKey.toString()), new IntWritable(killedMotorist));
            }
            if (line[8].length() > 0) {
                mapKey.setStreet(line[8]);

                mapKey.setCharackerOfAccident("INJURED");
                mapKey.setTypeParticipant("PEDESTRIAN");
                context.write(new Text(mapKey.toString()), new IntWritable(injuredPedestrians));
                mapKey.setCharackerOfAccident("KILLED");
                mapKey.setTypeParticipant("PEDESTRIAN");
                context.write(new Text(mapKey.toString()), new IntWritable(killedPedestrians));
                mapKey.setCharackerOfAccident("INJURED");
                mapKey.setTypeParticipant("CYCLIST");
                context.write(new Text(mapKey.toString()), new IntWritable(injuredCyclist));
                mapKey.setCharackerOfAccident("KILLED");
                mapKey.setTypeParticipant("CYCLIST");
                context.write(new Text(mapKey.toString()), new IntWritable(killedCyclist));
                mapKey.setCharackerOfAccident("INJURED");
                mapKey.setTypeParticipant("MOTORIST");
                context.write(new Text(mapKey.toString()), new IntWritable(injuredMotorist));
                mapKey.setCharackerOfAccident("KILLED");
                mapKey.setTypeParticipant("CYCLIST");
                context.write(new Text(mapKey.toString()), new IntWritable(killedMotorist));
            }
            //--------------------------------------------------- STREET
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        int sum;

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            sum = 0;
            Text sumText = new Text("For case: " + key + ", was number of accidents: ");
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(sumText, result);
        }
    }
}