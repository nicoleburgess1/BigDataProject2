import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;


public class createData {

    public static void createClustering() {
        Random rand = new Random();
        File data = new File("clustering.csv");
        try (PrintWriter writer = new PrintWriter(new FileWriter(data))) {
            for(int i=1; i<=5000; i++){
                int x = rand.nextInt(10000);
                int y = rand.nextInt(10000);
                int z = rand.nextInt(10000);
                writer.println( x + "," + y + "," + z);
            }
            System.out.println("CSV file written successfully.");
        } catch (IOException e) {
            System.out.println("An error occurred while writing the CSV file.");
        }
    }

    public static void main(String[] args) {
        createClustering();
    }
}
