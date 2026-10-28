package rep04;

public class SensorRecord {
    private final long seq;
    private final String sensorId;
    private final double temperature;
    private final long timestamp;        // instante da receção no líder, em ms desde 1970 (permite medir latências)
    private final long lamport;          // relógio de Lamport L
    private final VectorClock vector;    // relógio vetorial V

    public SensorRecord(long seq, String sensorId, double temperature, long timestamp,
                        long lamport, VectorClock vector) {
        this.seq = seq;
        this.sensorId = sensorId;
        this.temperature = temperature;
        this.timestamp = timestamp;
        this.lamport = lamport;
        this.vector = vector;
    }

    // cria um registo com o instante atual (usado só pelo líder)
    public static SensorRecord now(long seq, String sensorId, double temperature,
                                   long lamport, VectorClock vector) {
        return new SensorRecord(seq, sensorId, temperature, System.currentTimeMillis(), lamport, vector);
    }

    // representação textual: seq;sensor;temperatura;instante;L;V
    public String toLine() {
        return seq + ";" + sensorId + ";" + temperature + ";" + timestamp + ";" + lamport + ";" + vector;
    }

    // n é a dimensão do vetor (número de partições)
    public static SensorRecord fromLine(String line, int n) {
        String[] f = line.trim().split(";");
        if (f.length != 6) {
            throw new IllegalArgumentException("Registo mal formado: " + line);
        }
        return new SensorRecord(Long.parseLong(f[0]), f[1], Double.parseDouble(f[2]), Long.parseLong(f[3]),
                Long.parseLong(f[4]), VectorClock.parse(f[5], n));
    }

    public long getSeq() { return seq; }
    public String getSensorId() { return sensorId; }
    public double getTemperature() { return temperature; }
    public long getTimestamp() { return timestamp; }
    public long getLamport() { return lamport; }
    public VectorClock getVector() { return vector; }
}
