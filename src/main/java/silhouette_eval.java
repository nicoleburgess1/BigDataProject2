import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class silhouette_eval {

    public static double silhouette(int[][][] clusters) {
        double total = 0;
        for (int[][] points : clusters) {
            double sum = 0;
            for (int[] point : points) {
                for (int[] ints : points) {
                    sum += (double) euclideanDistance.distance(point, ints);
                }
            }
            total += sum / points.length;
        }
        return total / (clusters.length - 1);
    }

    public static double run_silhouette(String output_path) {
        ArrayList<String> lines = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(output_path))) {
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
                    double min = 10000;
                    double b = 0;
                    double sillhouette = 0;
                    double sum_1 = 0;
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
                    b = min;
                    sillhouette = (b - a) / Math.max(a, b);
                    System.out.println(sillhouette);
                }

            }

        }
        return 0;
    }

    public static void main(String[] args){
        run_silhouette("Task4510/Task4Output9/part-r-00000");
    }

}


