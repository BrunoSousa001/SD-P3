package raft04;

public class Partitioner {
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