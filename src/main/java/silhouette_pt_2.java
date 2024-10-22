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

public class silhouette_pt_2 {
    public static Text convertPointToText(int[] point){
        return new Text(point[0]+" "+point[1]+" "+point[2]);
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

        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            Job silhouetteJob = Job.getInstance(conf, "Task 4 - Silhouette Calculation");
            silhouetteJob.setJarByClass(silhouette_pt_2.class); // Correct class name
            FileInputFormat.addInputPath(silhouetteJob, new Path("Task4_BYOD/Task4_BYOD_Output6/part-r-00000"));
            FileOutputFormat.setOutputPath(silhouetteJob, new Path("Silhouette"));
            silhouetteJob.setMapperClass(SilhouetteMapper.class);
            silhouetteJob.setOutputKeyClass(Text.class);
            silhouetteJob.setOutputValueClass(Text.class);
            silhouetteJob.setNumReduceTasks(0);
            System.exit(silhouetteJob.waitForCompletion(true) ? 0 : 1);

    }


}
