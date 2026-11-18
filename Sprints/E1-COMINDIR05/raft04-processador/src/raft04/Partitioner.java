package raft04;

public class Partitioner {
    // Distribui os sensores pelas partições de forma determinística
    public static int getPartition(String sensor, int n) {
        if (sensor.startsWith("S")) {
            try {
                int id = Integer.parseInt(sensor.substring(1));
                return id % n;
            } catch (NumberFormatException e) {
                return 0;
            }
        }
        return Math.abs(sensor.hashCode()) % n;
    }
}