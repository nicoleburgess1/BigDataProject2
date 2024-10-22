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

public class sihouette_try_3 {
    public static Text convertPointToText(int[] point){
        return new Text(point[0]+" "+point[1]+" "+point[2]);
    }

    public static class SilhouetteMapper
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            ArrayList<String> lines = new ArrayList<>();
            try (BufferedReader br = new BufferedReader(new FileReader("Task4/Task4Output24/part-r-00000"))) {
                String line;
                while ((line = br.readLine()) != null) {
                    lines.add(line);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            for (int i = 0; i < lines.size() - 1; i++) {
                String[] points = lines.get(i).split("\t")[1].split("\\),");
                for (int j = 0; j < points.length - 1; j++) {
                    double sum = 0;
                    double a = 0;
                    double min = 10000;
                    double b = 0;
                    double sillhouette = 0;
                    double sum_1 = 0;
                    String[] point = points[j].split(",");
                    int[] int_point = new int[point.length];
                    String string_point = point[0].replace("(", "");
                    string_point = string_point.replace(" ", "");
                    //System.out.println(string_point);
                    int_point[0] = Integer.parseInt(string_point);
                    int_point[1] = Integer.parseInt(point[1]);
                    int_point[2] = Integer.parseInt(point[2]);
                    for (int k = 0; k < points.length - 1; k++) {
                        String[] point_1 = points[k].split(",");
                        int[] int_point_1 = new int[point_1.length];
                        String string_point_1 = point_1[0].replace("(", "");
                        string_point_1 = string_point_1.replace(" ", "");
                        //System.out.println(string_point);
                        int_point_1[0] = Integer.parseInt(string_point_1);
                        int_point_1[1] = Integer.parseInt(point_1[1]);
                        int_point_1[2] = Integer.parseInt(point_1[2]);
                        double distance = euclideanDistance.distance(int_point, int_point_1);
                        sum += (double) distance;
                    }
                    a = sum / points.length;

                    for (int l = 0; l < lines.size() - 1; l++) {
                        min = 10000;
                        b = 0;
                        sillhouette = 0;
                        sum_1 = 0;
                        if (l != i) {
                            String[] points_diff_cluster = lines.get(l).split("\t")[1].split("\\),");
                            for (int k = 0; k < points_diff_cluster.length - 1; k++) {
                                String[] point_1 = points_diff_cluster[k].split(",");
                                int[] int_point_1 = new int[point_1.length];
                                String string_point_1 = point_1[0].replace("(", "");
                                string_point_1 = string_point_1.replace(" ", "");
                                //System.out.println(string_point);
                                int_point_1[0] = Integer.parseInt(string_point_1);
                                int_point_1[1] = Integer.parseInt(point_1[1]);
                                int_point_1[2] = Integer.parseInt(point_1[2]);
                                double distance = euclideanDistance.distance(int_point, int_point_1);
                                sum_1 += (double) distance;
                            }
                            double temp = sum_1 / points_diff_cluster.length;
                            if (temp < min) {
                                min = temp;
                            }
                        }

                    }
                    b = min;
                    sillhouette = (b - a) / Math.max(a, b);
                    context.write(convertPointToText(int_point), new Text(String.valueOf(sillhouette)));

                }

            }
        }

    }

    public static class SillhouetteReducer
            extends Reducer<Text,Text,Text,Text> {
        static double sum;
        static double count;
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            sum = 0;
            count = 0;
            for (Text val : values) {
                double value = new Double(val.toString());
                sum += value;
                count += 1;
            }
        }

        @Override
        protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            double average = sum/count;
            context.write(new Text("Sillhouette average"), new Text(String.valueOf(average)));
        }
    }

    static int k = 5;
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Silhouette");
        job.setJarByClass(sihouette_try_3.class);
        job.setMapperClass(sihouette_try_3.SilhouetteMapper.class);
        job.setReducerClass(sihouette_try_3.SillhouetteReducer.class);
        //job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("Task4/Task4Output24/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("Silhouette"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
