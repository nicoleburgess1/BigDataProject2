import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class silhouette {
        static int k = 5;
        public static Text convertPointToText(int[] point){
            return new Text(point[0]+" "+point[1]+" "+point[2]);
        }

        public static int[][] firstCenters;
        public static class FirstIterationMapper
                extends Mapper<Object, Text, Text, Text> {

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

                if(returnPoints)
                    context.write(key, new Text(convertPointToText(sum)+ "\t" + numPoints + "\t" + points));
                else
                    context.write(key, new Text(convertPointToText(sum)+ "\t" + numPoints));
            }
        }

        public static class KMeansReducer
                extends Reducer<Text,Text,Text,Text> {

            public static ArrayList<int[]> newCenters = new ArrayList<>();

            public void reduce(Text key, Iterable<Text> values,
                               Context context
            ) throws IOException, InterruptedException {
                String points = "";
                int numPoints = 0;
                int[] sums = new int[3];
                int[] sum = {0,0,0};
                for (Text val : values) {
                    String[] parts = val.toString().split("\t");

                    String[] stringSums = parts[0].split(" ");
                    for(int i = 0; i < stringSums.length; i++){
                        sums[i] = Integer.parseInt(stringSums[i]);
                    }
                    for(int i=0; i<3; i++){
                        sum[i] += sums[i];
                    }
                    if(parts.length>2)
                        points += parts[2];
                    numPoints+=Integer.parseInt(parts[1]);;
                }

                for(int i=0; i<sum.length; i++){
                    sum[i] /= numPoints;
                }
                newCenters.add(sum);
                if(returnPoints)
                    context.write(convertPointToText(sum), new Text(points));
                else
                    context.write(convertPointToText(sum), new Text(""));
            }

            public void cleanup(Context context) throws IOException, InterruptedException {
                if(isFinished(r, newCenters))
                    context.write(new Text("CONVERGED"), new Text(""));
                else
                    context.write(new Text("NOT CONVERGED"), new Text(""));

                newCenters.clear();
            }

        }


        public static class SilhouetteMapper
            extends Mapper<Text, Text, Text, Text>{
            public void map(Text key, Text value, Context context
            ) throws IOException, InterruptedException {
                ArrayList<String> lines = new ArrayList<>();
                try (BufferedReader br = new BufferedReader(new FileReader("Task4/Task4Output" + (r-1) + "/part-r-00000"))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        lines.add(line);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                double total = 0;
                for (int i = 0; i < lines.size() - 1; i++) {
                    String[] points = lines.get(i).split("\t")[1].split("\\),");
                    for (int j = 0; j < points.length - 1; j++) {
                        double sum = 0;
                        double a = 0;
                        String[] point = points[j].split(",");
                        int[] int_point = new int[point.length];
                        String string_point = point[0].replace("(", "");
                        string_point = string_point.replace(" ", "");
                        System.out.println(string_point);
                        int_point[0] = Integer.parseInt(string_point);
                        int_point[1] = Integer.parseInt(point[1]);
                        int_point[2] = Integer.parseInt(point[2]);
                        for (int k = 0; k<points.length - 1; k++){
                            String[] point_1 = points[k].split(",");
                            int[] int_point_1 = new int[point_1.length];
                            String string_point_1 = point_1[0].replace("(", "");
                            string_point_1 = string_point_1.replace(" ", "");
                            System.out.println(string_point);
                            int_point_1[0] = Integer.parseInt(string_point_1);
                            int_point_1[1] = Integer.parseInt(point_1[1]);
                            int_point_1[2] = Integer.parseInt(point_1[2]);
                            double distance = euclideanDistance.distance(int_point, int_point_1);
                            sum += (double) distance;
                        }
                        a = sum/points.length;

                        for (int l = 0; l < lines.size() - 1; l++) {
                            double min = 10000;
                            double b = 0;
                            double sillhouette = 0;
                            double sum_1 = 0;
                            if (l!=i){
                                String[] points_diff_cluster = lines.get(l).split("\t")[1].split("\\),");
                                for (int k = 0; k<points_diff_cluster.length - 1; k++){
                                    String[] point_1 = points_diff_cluster[k].split(",");
                                    int[] int_point_1 = new int[point_1.length];
                                    String string_point_1 = point_1[0].replace("(", "");
                                    string_point_1 = string_point_1.replace(" ", "");
                                    System.out.println(string_point);
                                    int_point_1[0] = Integer.parseInt(string_point_1);
                                    int_point_1[1] = Integer.parseInt(point_1[1]);
                                    int_point_1[2] = Integer.parseInt(point_1[2]);
                                    double distance = euclideanDistance.distance(int_point, int_point_1);
                                    sum_1 += (double) distance;
                                }
                                double temp = sum_1/points_diff_cluster.length;
                                if (temp<min){
                                    min = temp;
                                }
                            }
                            b = min;
                            sillhouette = (b - a) / Math.max(a, b);
                            System.out.println(sillhouette);
                            context.write(convertPointToText(int_point), new Text(String.valueOf(sillhouette)));
                        }
                    }

                }
            }
        }
        public static int r;
        public static int threshold = 1;
        public static boolean finished;
        public static boolean returnPoints; //false if only returning centers (Task e.i) true if returning points and centers (Task e.ii)

        public static void main(String[] args) throws Exception {
            int R = 10;
            returnPoints = true;
            Configuration conf = new Configuration();
            finished = false;
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
                if(euclideanDistance.distance(oldCenters[i], newCenters[i]) > threshold)
                    return false;
            }
            return true;
        }

        public static boolean isFinished(int r, ArrayList<int[]> current) throws IOException {
            int[][] oldCenters, newCenters;
            newCenters = new int[k][];
            if(r==0)
                return false;
            if(r==1){
                oldCenters=firstCenters;
            }
            else
                oldCenters=getListOfCenters("Task4/Task4Output" + (r-1) + "/part-r-00000");


            for(int i=0; i<current.size(); i++){
                newCenters[i]= current.get(i);
                System.out.println("New Center: " + newCenters[i][0] + " " + newCenters[i][1] + " " + newCenters[i][2]);
            }
            for(int i=0; i<oldCenters.length; i++){
                if(euclideanDistance.distance(oldCenters[i], newCenters[i]) > threshold) //threshold
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

