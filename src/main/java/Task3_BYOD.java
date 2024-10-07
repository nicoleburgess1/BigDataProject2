import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

public class Task3_BYOD {

    public static Text convertPointToText(int[] point){
        return new Text(point[0]+" "+point[1]+" "+point[2]+ " "+ point[3]);
    }
    public static int[][] firstCenters;
    public static class FirstIterationMapper
            extends Mapper<Object, Text, Text, Text>{

        private final static IntWritable one = new IntWritable(1);
        private static int[][] centers = new int[2][];

        //private Text Education = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Random rand = new Random();
            //StringTokenizer itr = new StringTokenizer(value.toString());
            String dataset = value.toString();
            String[] datapoint = dataset.split("\n");
            String[][] columns = new String[datapoint.length][];
            int[][] points = new int[datapoint.length][];
            for(int i = 0; i < columns.length; i++){
                columns[i] = datapoint[i].split(",");
                points[i] = new int[columns[i].length];
                for(int j = 0; j < columns[i].length; j++){
                    points[i][j] = (int) Double.parseDouble(columns[i][j]);
                }
            }

            for(int i = 0; i < points.length; i++){
                int minDistance = -1;
                int[] closestCenter = null;
                for(int j = 0; j < centers.length; j++){
                    int distance = euclideanDistance.distance4(points[i], centers[j]);
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
            firstCenters = centers;
        }

        public int[][] loadInitialCenters() throws IOException {
            ArrayList<String> lines = new ArrayList<>();
            Random rand = new Random();
            int[][] center = new int[2][];

            // Reading the file
            try (BufferedReader br = new BufferedReader(new FileReader("penguins.csv"))) {
                String line;
                while ((line = br.readLine()) != null) {
                    lines.add(line);
                }
            }

            for(int i=0; i<2; i++){
                int lineNum = rand.nextInt(lines.size());
                String[] currLine = lines.get(lineNum).split(",");
                int[] currLineInt = new int[currLine.length];
                for(int j=0; j<currLine.length; j++){
                    currLineInt[j] = (int) Double.parseDouble(currLine[j]);
                }
                center[i] = currLineInt;
                System.out.println(center[i][0] + " " + center[i][1] + " " + center[i][2]+ " " + center[i][3]);
            }
            return center;
        }

    }

    public static class SubsequentIterationMapper
            extends Mapper<Object, Text, Text, Text>{

        private final static IntWritable one = new IntWritable(1);
        private static int[][] centers = new int[2][];

        //private Text Education = new Text();

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
                    points[i][j] = (int) Double.parseDouble(columns[i][j]);
                }
            }

            for(int i = 0; i < points.length; i++){
                int minDistance = -1;
                int[] closestCenter = null;
                for(int j = 0; j < centers.length; j++){
                    int distance = euclideanDistance.distance4(points[i], centers[j]);
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
            int[][] center = new int[2][];

            // Reading the file
            try (BufferedReader br = new BufferedReader(new FileReader("Task3_BYOD/Task3_BYOD_Output" + (r-1) + "/part-r-00000"))) {
                String line;
                while ((line = br.readLine()) != null) {
                    lines.add(line);
                }
            }

            for(int i=0; i<2; i++){
                String currCenter = lines.get(i).split("\t")[0];
                String[] currLine = currCenter.split(" ");
                int[] currLineInt = new int[currLine.length];
                for(int j=0; j<currLine.length; j++){
                    currLineInt[j] = (int) Double.parseDouble(currLine[j]);
                }
                center[i] = currLineInt;
                System.out.println(center[i][0] + " " + center[i][1] + " " + center[i][2] + " " + center[i][3]);
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
            int[] sum = {0,0,0,0};
            for (Text val : values) {
                String[] stringPoint = val.toString().split(" ");
                int[] point = new int[stringPoint.length];
                for(int i = 0; i < point.length; i++){
                    point[i] = (int) Double.parseDouble(stringPoint[i]);
                }
                for(int i=0; i<4; i++){
                    sum[i] += point[i];
                }
                numPoints++;
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

        for(r=0; r<R && !finished; r++){
            Job job = Job.getInstance(conf, "Task 2 - Iteration " + r);
            job.setJarByClass(Task3_BYOD.class);
            job.setReducerClass(KMeansReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            if(r==0){
                job.setMapperClass(FirstIterationMapper.class);
            }
            else{
                job.setMapperClass(SubsequentIterationMapper.class);
            }
            FileInputFormat.addInputPath(job, new Path("penguins.csv"));
            FileOutputFormat.setOutputPath(job, new Path("Task3_BYOD/Task3_BYOD_Output" + r));
            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }
            finished = isFinished(r);
        }

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
            oldCenters=getListOfCenters("Task3_BYOD/Task3_BYOD_Output" + (r-1) + "/part-r-00000");
        newCenters= getListOfCenters("Task3_BYOD/Task3_BYOD_Output" + (r) + "/part-r-00000");
        for(int i=0; i<oldCenters.length; i++){
            if(euclideanDistance.distance4(oldCenters[i], newCenters[i]) > 1)
                return false;
        }
        return true;
    }
    public static int[][] getListOfCenters(String path) throws IOException {
        ArrayList<String> lines = new ArrayList<>();
        int[][] center = new int[2][];

        // Reading the file
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = br.readLine()) != null) {
                lines.add(line);
            }
        }

        for(int i=0; i<2; i++){
            String currCenter = lines.get(i).split("\t")[0];
            String[] currLine = currCenter.split(" ");
            int[] currLineInt = new int[currLine.length];
            for(int j=0; j<currLine.length; j++){
                currLineInt[j] = (int) Double.parseDouble(currLine[j]);
            }
            center[i] = currLineInt;
        }
        return center;
    }
}