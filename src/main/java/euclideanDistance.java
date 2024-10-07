import java.lang.Math;

public class euclideanDistance {

    public static int distance (int[] point1,int[] point2) {
        int x1 = point1[0];
        int y1 = point1[1];
        int z1 = point1[2];
        int x2 = point2[0];
        int y2 = point2[1];
        int z2 = point2[2];
        return (int) Math.round(java.lang.Math.sqrt((java.lang.Math.pow(x2-x1, 2)) + (java.lang.Math.pow(y2-y1, 2)) +(java.lang.Math.pow(z2-z1, 2))));
    }
    public static double distance4 (int[] point1,int[] point2) {
        double x1 = point1[0];
        double y1 = point1[1];
        double z1 = point1[2];
        double j1 = point1[3];
        double x2 = point2[0];
        double y2 = point2[1];
        double z2 = point2[2];
        double j2 = point2[3];
        return Math.round(java.lang.Math.sqrt((java.lang.Math.pow(x2-x1, 2)) + (java.lang.Math.pow(y2-y1, 2)) +(java.lang.Math.pow(z2-z1, 2))+ java.lang.Math.pow(j2-j1, 2)));
    }
    public static double distance4_normalized (int[] point1,int[] point2) {
        int x_max = 60;
        int y_max = 22;
        int z_max = 231;
        int j_max = 6300;
        double x1 = (double) point1[0] / x_max;
        double y1 = (double) point1[1] / y_max;
        double z1 = (double) point1[2] / z_max;
        double j1 = (double) point1[3] / j_max;
        double x2 = (double) point2[0] / x_max;
        double y2 = (double) point2[1] / y_max;
        double z2 = (double) point2[2] / z_max;
        double j2 = (double) point2[3] / j_max;
        return (java.lang.Math.sqrt((java.lang.Math.pow(x2-x1, 2)) + (java.lang.Math.pow(y2-y1, 2)) +(java.lang.Math.pow(z2-z1, 2))+ java.lang.Math.pow(j2-j1, 2)));
    }
}
