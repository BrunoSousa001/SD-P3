package raft04;

public class Partitioner {
    // Partição determinística por hash (igual à ficha anterior: S02 e S05 -> p1 quando N=3)
    public static int getPartition(String sensor, int n) {
        return Math.floorMod(sensor.hashCode(), n);
    }
}