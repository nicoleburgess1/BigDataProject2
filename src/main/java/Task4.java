import java.io.BufferedReader;
import java.io.FileNotFoundException;
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

public class Task4 {
    static int k = 5;
    public static Text convertPointToText(int[] point){
        return new Text(point[0]+" "+point[1]+" "+point[2]);
    }

    public static int[][] firstCenters;
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
            centers = loadInitialCenters();
            firstCenters=centers;
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
            centers = loadInitialCenters();
        }

        public int[][] loadInitialCenters() throws IOException {
            ArrayList<String> lines = new ArrayList<>();
            Random rand = new Random();
            int[][] center = new int[k][];

            // Reading the file
            try (BufferedReader br = new BufferedReader(new FileReader("Task4/Task4Output" + (r-1) + "/part-r-00000"))) {
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

    public static class KMeansCombiner
            extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int numPoints = 0;
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
                numPoints++;
            }

            context.write(key, new Text(convertPointToText(sum)+ " " + numPoints));
        }
    }

    public static class KMeansReducer
            extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int numPoints = 0;
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
                numPoints+=Integer.parseInt(stringPoint[3]);;
            }

            for(int i=0; i<sum.length; i++){
                sum[i] /= numPoints;
            }
            context.write(convertPointToText(sum), new Text("1"));
        }
    }

    public static int r;
    public static void main(String[] args) throws Exception {
        int R = 20;
        Configuration conf = new Configuration();
        boolean finished = false;
        long startTime = System.currentTimeMillis();
        for(r=0; r<R && !finished; r++){
            Job job = Job.getInstance(conf, "Task 4 - Iteration " + r);
            job.setJarByClass(Task4.class);
            job.setCombinerClass(KMeansCombiner.class);
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
            FileOutputFormat.setOutputPath(job, new Path("Task4/Task4Output" + r));
            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }

            finished = isFinished(r);
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Time taken: " + (endTime - startTime) + "ms");

        System.exit(0);

    }

    public static boolean isFinished(int r) throws IOException {
        int[][] oldCenters, newCenters;
        if(r==0)
            return false;
        if(r==1){
            oldCenters=firstCenters;
        }
        else
            oldCenters=getListOfCenters("Task4/Task4Output" + (r-1) + "/part-r-00000");
        newCenters= getListOfCenters("Task4/Task4Output" + (r) + "/part-r-00000");
        for(int i=0; i<oldCenters.length; i++){
            if(euclideanDistance.distance(oldCenters[i], newCenters[i]) > 1)
                return false;
        }
        return true;
    }

    public static int[][] getListOfCenters(String path) throws IOException {
        ArrayList<String> lines = new ArrayList<>();
        int[][] center = new int[k][];

        // Reading the file
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
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
        }
        return center;
    }
}