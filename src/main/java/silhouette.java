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
import java.util.Arrays;
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
                    //System.out.println(center[i][0] + " " + center[i][1] + " " + center[i][2]);
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
                try (BufferedReader br = new BufferedReader(new FileReader("Task4Silhouette/output" + (r-1) + "/part-r-00000"))) {
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
                    //System.out.println(center[i][0] + " " + center[i][1] + " " + center[i][2]);
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
            public void map(Text key, Text values, Context context
            ) throws IOException, InterruptedException {
                //String[] dataset = values.toString().split("\n");
                context.write(new Text("AHHH"), new Text("ahh"));
                /*

                ArrayList<String[]> points = new ArrayList<>();
                for(int i=0; i<dataset.length-1; i++){ //the last line is CONVERGED or NOT CONVERGED
                    points.add(dataset[i].split(" "));
                }
                for(int i=0; i<points.size(); i++){
                    for(int j=0; j<points.get(i).length; j++){
                        points.get(i)[j] = points.get(i)[j].substring(points.get(i)[j].indexOf("(")+1,points.get(i)[j].indexOf(")"));
                    }
                }
                ArrayList<String[][]> xyzString = new ArrayList<>(); //array of points which each have an array of values xyz
                for(int i=0; i<points.size(); i++){
                    String[][] temp = new String[points.get(i).length][];
                    for(int j=0; j<points.get(i).length; j++){
                        temp[j] = points.get(i)[j].split(",");
                    }
                    xyzString.add(temp);
                }


                ArrayList<int[][]> xyzPoints = new ArrayList<>();
                for (String[][] cluster : xyzString) {
                    int[][] clusterPoints = new int[cluster.length][cluster[0].length];
                    for (int i = 0; i < cluster.length; i++) {
                        for (int j = 0; j < cluster[i].length; j++) {
                            clusterPoints[i][j] = Integer.parseInt(cluster[i][j]);
                        }
                    }
                    xyzPoints.add(clusterPoints);
                }



                for (int i = 0; i < xyzPoints.size(); i++) {
                    int[][] points_1 = xyzPoints.get(i);
                    for (int j = 0; j < points_1.length; j++) {
                        double sum = 0;
                        double a = 0;
                        int[] point = points_1[j];
                        for (int k = 0; k<points_1.length; k++){
                            int[] point_1 = points_1[k];
                            double distance = euclideanDistance.distance(point, point_1);
                            sum += (double) distance;
                        }
                        a = sum/points_1.length;

                        for (int l = 0; l < xyzPoints.size(); l++) {
                            double min = 10000;
                            double b = 0;
                            double silhouette = 0;
                            double sum_1 = 0;
                            if (l!=i){
                                int[][] points_diff_cluster = xyzPoints.get(l);
                                for (int k = 0; k<points_diff_cluster.length; k++){
                                    int[] point_1 = points_diff_cluster[k];
                                    double distance = euclideanDistance.distance(point, point_1);
                                    sum_1 += (double) distance;
                                }
                                double temp = sum_1/points_diff_cluster.length;
                                if (temp<min){
                                    min = temp;
                                }
                            }
                            b = min;
                            silhouette = (b - a) / Math.max(a, b);
                            context.write(new Text("DEBUG"), new Text("Point: " + Arrays.toString(point) + ", a: " + a + ", b: " + b + ", silhouette: " + silhouette));
                            //context.write(convertPointToText(point), new Text(String.valueOf(silhouette)));

                        }
                    }

                }*/

            }
        }

    public static class SillhouetteReducer
            extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sum = 0;
            double i = 0;
            for (Text val : values) {
                double value = new Double(val.toString());
                sum += value;
                i += 1;
            }
            double average = sum/i;
            context.write(new Text("Sillhouette average"), new Text(String.valueOf(average)));
        }
    }

        public static int r;
        public static int threshold = 1;
        public static boolean finished;
        public static boolean returnPoints; //false if only returning centers (Task e.i) true if returning points and centers (Task e.ii)

        public static void main(String[] args) throws Exception {
            int R = 0;
            returnPoints = true;
            Configuration conf = new Configuration();
            finished = false;
            long startTime = System.currentTimeMillis();
            for(r=0; r<=R; r++) {
                Job job = Job.getInstance(conf, "Task 4 - Iteration " + r);
                Job silhouetteJob = Job.getInstance(conf, "Task 4 - Silhouette Calculation");
                job.setJarByClass(silhouette.class);
                job.setCombinerClass(KMeansCombiner.class);
                job.setReducerClass(KMeansReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job, new Path("clustering.csv"));
                FileOutputFormat.setOutputPath(job, new Path("Task4Silhouette/output" + r));
                //if (r == 0) {
                //    job.setMapperClass(FirstIterationMapper.class);
                //}
                 if(finished || r>=R){
                    System.out.println(r);
                    silhouetteJob.setJarByClass(silhouette.class);
                    silhouetteJob.setMapperClass(SilhouetteMapper.class);
                    FileInputFormat.addInputPath(silhouetteJob, new Path("Task4_BYOD/Task4_BYOD_Output6/part-r-00000"));
                    //FileInputFormat.addInputPath(silhouetteJob, new Path("Task4Silhouette/output" + (r - 1) + "/part-r-00000"));
                    FileOutputFormat.setOutputPath(silhouetteJob, new Path("Task4Silhouette/silhouette"));
                    silhouetteJob.setOutputKeyClass(Text.class);
                    silhouetteJob.setOutputValueClass(Text.class);
                    silhouetteJob.setNumReduceTasks(0);
                    if (!silhouetteJob.waitForCompletion(true)) {
                        System.exit(1);
                    }
                    else System.exit(0);
                }
                else {
                    job.setMapperClass(SubsequentIterationMapper.class);
                }
                if (!job.waitForCompletion(true)) {
                    System.exit(1);
                }
                finished = isFinished(r);
            }
            long endTime = System.currentTimeMillis();
            System.out.println("Time taken: " + (endTime - startTime) + "ms");

            //System.exit(0);

        }

        public static boolean isFinished(int r) throws IOException {
            int[][] oldCenters, newCenters;
            if(r==0){
                System.out.println(r);
                return false;}
            if(r==1){
                System.out.println(r);
                oldCenters=firstCenters;
            }
            else{
                System.out.println(r);
                oldCenters=getListOfCenters("Task4Silhouette/output" + (r-1) + "/part-r-00000");}
            newCenters= getListOfCenters("Task4Silhouette/output" + (r) + "/part-r-00000");
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
                oldCenters=getListOfCenters("Task4Silhouette/output" + (r-1) + "/part-r-00000");


            for(int i=0; i<current.size(); i++){
                newCenters[i]= current.get(i);
                //System.out.println("New Center: " + newCenters[i][0] + " " + newCenters[i][1] + " " + newCenters[i][2]);
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

