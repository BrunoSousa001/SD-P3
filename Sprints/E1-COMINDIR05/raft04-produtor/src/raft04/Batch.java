package raft04;

import java.util.ArrayList;
import java.util.List;

public final class Batch {
    public final long term;
    public final int leader;
    public final String address;
    public final int port;
    public final long hb;
    public final long lastSeq;
    public final long lastTerm; // NOVO
    public final List<SensorRecord> records;
    public final int count;

    public Batch(long term, int leader, String address, int port, long hb, long lastSeq, long lastTerm, List<SensorRecord> records) {
        this(term, leader, address, port, hb, lastSeq, lastTerm, records, records.size());
    }

    private Batch(long term, int leader, String address, int port, long hb, long lastSeq, long lastTerm, List<SensorRecord> records, int count) {
        this.term = term;
        this.leader = leader;
        this.address = address;
        this.port = port;
        this.hb = hb;
        this.lastSeq = lastSeq;
        this.lastTerm = lastTerm;
        this.records = records;
        this.count = count;
    }

    public String encode() {
        StringBuilder sb = new StringBuilder();
        // NOVO: LOTE;term;leader;address;port;hb;lastSeq;lastTerm;n
        sb.append("LOTE;").append(term).append(';').append(leader).append(';')
                .append(address).append(';').append(port).append(';')
                .append(hb).append(';').append(lastSeq).append(';')
                .append(lastTerm).append(';').append(records.size());
        for (SensorRecord r : records) {
            sb.append('\n').append(r.toLine());
        }
        return sb.toString();
    }

    public static Batch decode(String msg, int n) {
        String[] linhas = msg.strip().split("\\r?\\n");
        Batch h = cabecalho(linhas[0]);
        if (linhas.length - 1 != h.count) {
            throw new IllegalArgumentException("LOTE com " + (linhas.length - 1) + " linhas de registo, cabeçalho indica " + h.count);
        }
        List<SensorRecord> records = new ArrayList<>();
        for (int i = 1; i < linhas.length; i++) {
            records.add(SensorRecord.fromLine(linhas[i], n));
        }
        return new Batch(h.term, h.leader, h.address, h.port, h.hb, h.lastSeq, h.lastTerm, records, h.count);
    }

    public static Batch decodeHeader(String msg) {
        String primeira = msg.strip().split("\\r?\\n", 2)[0];
        return cabecalho(primeira);
    }

    private static Batch cabecalho(String linha) {
        String[] f = linha.trim().split(";");
        if (f.length != 9 || !f[0].equals("LOTE")) { // NOVO: Agora são 9 campos no cabeçalho
            throw new IllegalArgumentException("LOTE mal formado: " + linha);
        }
        int count = Integer.parseInt(f[8]);
        if (count < 0) {
            throw new IllegalArgumentException("LOTE com n negativo: " + linha);
        }
        return new Batch(
                Long.parseLong(f[1]), // term
                Integer.parseInt(f[2]), // leader
                f[3], // address
                Integer.parseInt(f[4]), // port
                Long.parseLong(f[5]), // hb
                Long.parseLong(f[6]), // lastSeq
                Long.parseLong(f[7]), // lastTerm
                new ArrayList<>(), count);
    }

    public String seqRange() {
        if (records.isEmpty()) return "-";
        return records.get(0).getSeq() + ".." + records.get(records.size() - 1).getSeq();
    }
}