
/**
 * G89GEN
 */
import java.io.IOException;

public class G89GEN {

    public static void main(String[] args) throws IOException {
        /*
         * The idea is to create each cluster as formed by 1 point of class A with
         * coordinates (-10, 10 * i) and many points of class B with coordinates
         * (10, 10 * i).
         * In this way the Lloyd's clustering will focus where there are
         * more points, discriminating the minority class, and i'ts cluster center will
         * be close to the points of class B.
         * On the other hand the Fair clustering will give importance also to the points
         * of the minority class and it will set some cluster's centers near the points
         * of class A
         */
        int N = Integer.parseInt(args[0]);
        int K = Integer.parseInt(args[1]);

        if (N < 2 * K) {
            throw new IllegalArgumentException("USAGE: file_path, num_partition, num_centers, num_iteration");
        }

        for (int i = 0; i < K; i++) {
            System.out.println(-10.0 + "," + (0.0 + 10.0 * i) + ",A");
        }

        int nB = (N - K) / K; // number of points of class B belonging to each cluster
        int nBLast = nB + (N - K) % K; // number of points of class B belonging to the last cluster

        for (int i = 0; i < K - 1; i++) {
            for (int j = 0; j < nB; j++) {
                System.out.println(10.0 + "," + (0.0 + 10.0 * i) + ",B");
            }
        }
        for (int j = 0; j < nBLast; j++) {
            System.out.println(10.0 + "," + (0.0 + 10.0 * (K - 1)) + ",B");
        }

    }
}
