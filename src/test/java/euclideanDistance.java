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
}
