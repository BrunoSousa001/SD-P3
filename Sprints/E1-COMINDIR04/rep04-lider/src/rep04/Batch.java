package rep04;

import java.util.ArrayList;
import java.util.List;

/**
 * Lote de registos publicado pelo líder, que serve também de heartbeat.
 *
 * LOTE;<endereço>;<porto>;<inc>;<hb>;<ultimoSeq>;<n>
 * <seq>;<sensor>;<temperatura>;<instante>;<L>;<V>      (n linhas)
 */
public final class Batch {
    public final String address;
    public final int port;
    public final long inc;
    public final long hb;
    public final long lastSeq;
    public final List<SensorRecord> records;
    public final int count;      // n declarado no cabeçalho (igual a records.size() quando o lote é decodificado por inteiro)

    public Batch(String address, int port, long inc, long hb, long lastSeq, List<SensorRecord> records) {
        this(address, port, inc, hb, lastSeq, records, records.size());
    }

    private Batch(String address, int port, long inc, long hb, long lastSeq, List<SensorRecord> records, int count) {
        this.address = address;
        this.port = port;
        this.inc = inc;
        this.hb = hb;
        this.lastSeq = lastSeq;
        this.records = records;
        this.count = count;
    }

    // produz a mensagem LOTE
    public String encode() {
        StringBuilder sb = new StringBuilder();
        sb.append("LOTE;").append(address).append(';').append(port).append(';').append(inc).append(';')
          .append(hb).append(';').append(lastSeq).append(';').append(records.size());
        for (SensorRecord r : records) {
            sb.append('\n').append(r.toLine());
        }
        return sb.toString();
    }

    // interpreta o cabeçalho e os registos; n é a dimensão do vetor (número de partições)
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
        return new Batch(h.address, h.port, h.inc, h.hb, h.lastSeq, records, h.count);
    }

    // interpreta apenas o cabeçalho (usado pelo produtor, que não precisa dos registos)
    public static Batch decodeHeader(String msg) {
        String primeira = msg.strip().split("\\r?\\n", 2)[0];
        return cabecalho(primeira);
    }

    private static Batch cabecalho(String linha) {
        String[] f = linha.trim().split(";");
        if (f.length != 7 || !f[0].equals("LOTE")) {
            throw new IllegalArgumentException("LOTE mal formado: " + linha);
        }
        int count = Integer.parseInt(f[6]);
        if (count < 0) {
            throw new IllegalArgumentException("LOTE com n negativo: " + linha);
        }
        return new Batch(f[1], Integer.parseInt(f[2]), Long.parseLong(f[3]), Long.parseLong(f[4]),
                Long.parseLong(f[5]), new ArrayList<>(), count);
    }

    // intervalo de seq do lote, ex.: "3..7"; "-" se vazio
    public String seqRange() {
        if (records.isEmpty()) return "-";
        return records.get(0).getSeq() + ".." + records.get(records.size() - 1).getSeq();
    }
}
