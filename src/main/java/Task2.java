import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.ArrayList;
import java.util.Random;

public class Task2 {

    public static Text convertPointToText(int[] point){
        return new Text(point[0]+" "+point[1]+" "+point[2]);
    }

    public static class FirstIterationMapper
            extends Mapper<Object, Text, Text, Text>{

        private static int[][] centers = new int[k][];


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String dataset = value.toString();
            String[] datapoint = dataset.split("\n");
            String[][] columns = new String[datapoint.length][];
            int[][] points = new int[datapoint.length][];
            for(int i = 0; i < columns.length; i++){
                columns[i] = datapoint[i].split(",");
                points[i] = new int[columns[i].length];
                for(int j = 0; j < columns[i].length; j++){
                    points[i][j] = Integer.parseInt(columns[i][j]);
                }
            }

            for(int i = 0; i < points.length; i++){
                int minDistance = -1;
                int[] closestCenter = null;
                for(int j = 0; j < centers.length; j++){
                    int distance = euclideanDistance.distance(points[i], centers[j]);
                    if(minDistance == -1 || distance < minDistance){
                        closestCenter = centers[j];
                        minDistance = distance;
                    }
                }
                context.write(convertPointToText(closestCenter),convertPointToText(points[i]));
            }
        }

        protected void setup(Context context) throws IOException, InterruptedException {
            // Load centroids from a file or the job configuration
            centers = loadInitialCenters();
        }

        public int[][] loadInitialCenters() throws IOException {
            ArrayList<String> lines = new ArrayList<>();
            Random rand = new Random();
            int[][] center = new int[k][];

            // Reading the file
            try (BufferedReader br = new BufferedReader(new FileReader("clustering.csv"))) {
                String line;
                while ((line = br.readLine()) != null) {
                    lines.add(line);
                }
            }

            for(int i=0; i<k; i++){
                int lineNum = rand.nextInt(lines.size());
                String[] currLine = lines.get(lineNum).split(",");
                int[] currLineInt = new int[currLine.length];
                for(int j=0; j<currLine.length; j++){
                    currLineInt[j] = Integer.parseInt(currLine[j]);
                }
                center[i] = currLineInt;
                System.out.println(center[i][0] + " " + center[i][1] + " " + center[i][2]);
            }
            return center;
        }

    }

    public static class SubsequentIterationMapper
            extends Mapper<Object, Text, Text, Text>{

        private static int[][] centers = new int[k][];

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String dataset = value.toString();
            String[] datapoint = dataset.split("\n");
            String[][] columns = new String[datapoint.length][];
            int[][] points = new int[datapoint.length][];
            for(int i = 0; i < columns.length; i++){
                columns[i] = datapoint[i].split(",");
                points[i] = new int[columns[i].length];
                for(int j = 0; j < columns[i].length; j++){
                    points[i][j] = Integer.parseInt(columns[i][j]);
                }
            }

            for(int i = 0; i < points.length; i++){
                int minDistance = -1;
                int[] closestCenter = null;
                for(int j = 0; j < centers.length; j++){
                    int distance = euclideanDistance.distance(points[i], centers[j]);
                    if(minDistance == -1 || distance < minDistance){
                        closestCenter = centers[j];
                        minDistance = distance;
                    }
                }
                context.write(convertPointToText(closestCenter),convertPointToText(points[i]));
            }
        }

        protected void setup(Context context) throws IOException, InterruptedException {
            // Load centroids from a file or the job configuration
            centers = loadInitialCenters();
        }

        public int[][] loadInitialCenters() throws IOException {
            ArrayList<String> lines = new ArrayList<>();
            Random rand = new Random();
            int[][] center = new int[k][];

            // Reading the file
            try (BufferedReader br = new BufferedReader(new FileReader("Task2/Task2Output" + (r-1) + "/part-r-00000"))) {
                String line;
                while ((line = br.readLine()) != null) {
                    lines.add(line);
                }
            }

            for(int i=0; i<k; i++){
                String currCenter = lines.get(i).split("\t")[0];
                String[] currLine = currCenter.split(" ");
                int[] currLineInt = new int[currLine.length];
                for(int j=0; j<currLine.length; j++){
                    currLineInt[j] = Integer.parseInt(currLine[j]);
                }
                center[i] = currLineInt;
                System.out.println(center[i][0] + " " + center[i][1] + " " + center[i][2]);
            }
            return center;
        }

    }

    public static class KMeansReducer
            extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int numPoints = 0;
            String points = "";
            int[] sum = {0,0,0};
            for (Text val : values) {
                String[] stringPoint = val.toString().split(" ");
                int[] point = new int[stringPoint.length];
                for(int i = 0; i < point.length; i++){
                    point[i] = Integer.parseInt(stringPoint[i]);
                }
                for(int i=0; i<3; i++){
                    sum[i] += point[i];
                }
                points += "(" + point[0] + "," + point[1] + "," + point[2] + "), ";
                numPoints++;
            }

            for(int i=0; i<sum.length; i++){
                sum[i] /= numPoints;
            }
            //result.set(sum);
            context.write(convertPointToText(sum), new Text(points));
        }
    }
    public static int k = 5;
    public static int r;
    public static int R;
    public static void main(String[] args) throws Exception {
        R = 10;
        Configuration conf = new Configuration();
        long startTime = System.currentTimeMillis();
        for(r=0; r<R; r++){
            Job job = Job.getInstance(conf, "Task 2 - Iteration " + r);
            job.setJarByClass(Task2.class);
            //job.setNumReduceTasks(0);
            job.setReducerClass(KMeansReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            if(r==0){
                job.setMapperClass(FirstIterationMapper.class);
            }
            else{
                job.setMapperClass(SubsequentIterationMapper.class);
            }
            FileInputFormat.addInputPath(job, new Path("clustering.csv"));
            FileOutputFormat.setOutputPath(job, new Path("Task2/Task2Output" + r));
            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Time taken: " + (endTime - startTime));
        System.exit(0);

    }
}