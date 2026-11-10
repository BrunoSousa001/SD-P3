package raft04;

public class SensorRecord {
    private final long seq;
    private final long term;             // NOVO: Termo em que o registo foi criado
    private final String sensorId;
    private final double temperature;
    private final long timestamp;
    private final long lamport;
    private final VectorClock vector;

    public SensorRecord(long seq, long term, String sensorId, double temperature, long timestamp,
                        long lamport, VectorClock vector) {
        this.seq = seq;
        this.term = term;
        this.sensorId = sensorId;
        this.temperature = temperature;
        this.timestamp = timestamp;
        this.lamport = lamport;
        this.vector = vector;
    }

    public static SensorRecord now(long seq, long term, String sensorId, double temperature,
                                   long lamport, VectorClock vector) {
        return new SensorRecord(seq, term, sensorId, temperature, System.currentTimeMillis(), lamport, vector);
    }

    // NOVO formato: seq;term;sensor;temperatura;instante;L;V
    public String toLine() {
        return seq + ";" + term + ";" + sensorId + ";" + temperature + ";" + timestamp + ";" + lamport + ";" + vector;
    }

    public static SensorRecord fromLine(String line, int n) {
        String[] f = line.trim().split(";");
        if (f.length != 7) { // NOVO: Agora são 7 campos
            throw new IllegalArgumentException("Registo mal formado: " + line);
        }
        return new SensorRecord(
                Long.parseLong(f[0]),
                Long.parseLong(f[1]), // term
                f[2],
                Double.parseDouble(f[3]),
                Long.parseLong(f[4]),
                Long.parseLong(f[5]),
                VectorClock.parse(f[6], n)
        );
    }

    public long getSeq() { return seq; }
    public long getTerm() { return term; }
    public String getSensorId() { return sensorId; }
    public double getTemperature() { return temperature; }
    public long getTimestamp() { return timestamp; }
    public long getLamport() { return lamport; }
    public VectorClock getVector() { return vector; }
}