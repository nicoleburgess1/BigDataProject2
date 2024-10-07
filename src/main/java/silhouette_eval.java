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
        return total/(clusters.length-1);
    }
}
