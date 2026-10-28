package rep04;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PartitionLeader {
    public static void main(String[] args) {
        int k = args.length > 0 ? Integer.parseInt(args[0]) : 0;
        int n = args.length > 1 ? Integer.parseInt(args[1]) : 1;
        String host = args.length > 2 ? args[2] : "127.0.0.1";
        String api = args.length > 3 ? args[3] : "http://127.0.0.1:5001/api/v0";

        int porto = Wire.PORTO_BASE + k;
        RecordFile file = new RecordFile("lider-p" + k + ".txt", n);
        Path hbFile = Paths.get("hb-p" + k + ".txt");
        Partitioner partitioner = new Partitioner(n);
        IpfsPubSub ipfs = new IpfsPubSub(api);

        // Cada arranque do líder define uma incarnação; os heartbeats são numerados por hb a partir de 1
        final long inc = System.currentTimeMillis();

        // Estado partilhado, protegido pelo MESMO monitor (lock):
        //   seq, L, V, hb e a lista pendentes dos registos ainda por publicar
        final Object lock = new Object();
        final long[] seqHolder = new long[1];
        final long[] lamportHolder = new long[1];
        final long[] hbHolder = new long[1];
        final VectorClock vector = new VectorClock(n);
        final List<List<SensorRecord>> pendentesHolder = new ArrayList<>();
        pendentesHolder.add(new ArrayList<>());

        // No arranque, L e V retomam do último registo do ficheiro (um líder reiniciado não recomeça a zero)
        try {
            SensorRecord ultimo = file.last();
            if (ultimo != null) {
                seqHolder[0] = ultimo.getSeq();
                lamportHolder[0] = ultimo.getLamport();
                vector.merge(ultimo.getVector());
            }
        } catch (IOException e) {
            System.out.println("Erro ao ler o último registo: " + e.getMessage());
            return;
        }

        System.out.println("Líder p" + k + " pronto (inc=" + inc + "). Recuperação TCP em " + host + ":" + porto
                + ", seq inicial=" + seqHolder[0] + " L=" + lamportHolder[0] + " V=" + vector
                + ", periodo=" + Wire.PERIODO_HB_MS + " ms");

        // ATIVIDADE 1: Thread de Entrada (ouve o Produtor/Processador no IPFS)
        Thread entrada = new Thread(() -> {
            try {
                System.out.println("Líder p" + k + " à escuta no tópico " + Wire.topicIn(k));
                ipfs.subscribe(Wire.topicIn(k), line -> {
                    try {
                        String tipo = Wire.type(line);
                        if (!tipo.equals("IN")) {
                            System.out.println("REJEITADO: " + line);
                            return;
                        }

                        String[] f = Wire.body(line).split(";");
                        if (f.length != 4) {
                            System.out.println("REJEITADO: " + line);
                            return;
                        }

                        String sensor = f[0];
                        double temp = Double.parseDouble(f[1]);
                        long lMsg = Long.parseLong(f[2]);
                        VectorClock vMsg = VectorClock.parse(f[3], n);

                        int partEsperada = partitioner.of(sensor);
                        if (partEsperada != k) {
                            System.out.println("PARTIÇÃO ERRADA: " + sensor + " pertence a p" + partEsperada);
                            return;
                        }

                        // Secção sincronizada da COMINDIR03 (L, V, seq, V[k]==seq, append) + registo em pendentes.
                        // O registo é numerado e gravado no instante da receção; só a PUBLICAÇÃO espera pelo lote.
                        SensorRecord r;
                        synchronized (lock) {
                            lamportHolder[0] = Math.max(lamportHolder[0], lMsg) + 1;
                            vector.merge(vMsg);
                            vector.tick(k);
                            seqHolder[0]++;
                            if (vector.get(k) != seqHolder[0]) {
                                System.out.println("ERRO: V[k]=" + vector.get(k) + " != seq=" + seqHolder[0]);
                            }
                            r = SensorRecord.now(seqHolder[0], sensor, temp, lamportHolder[0], vector.copy());
                            file.append(r);
                            pendentesHolder.get(0).add(r);
                        }
                        System.out.println("EVENTO: seq=" + r.getSeq() + " L=" + r.getLamport() + " V=" + r.getVector());

                    } catch (Exception e) {
                        System.out.println("REJEITADO: " + line);
                    }
                });
            } catch (Exception e) {
                System.out.println("Erro na thread de entrada: " + e.getMessage());
            }
        });
        entrada.setDaemon(true);
        entrada.start();

        // ATIVIDADE 2: envio periódico do lote (= heartbeat): no arranque e depois a cada PERIODO_HB_MS
        ScheduledExecutorService agendador = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "Lote-p" + k);
            t.setDaemon(true);
            return t;
        });
        agendador.scheduleAtFixedRate(() -> {
            try {
                enviarLote(k, host, porto, inc, ipfs, hbFile, lock, seqHolder, hbHolder, pendentesHolder);
            } catch (Throwable t) {
                // uma exceção não tratada cancelaria as execuções seguintes de scheduleAtFixedRate
                System.out.println("ERRO no envio do lote: " + t);
            }
        }, 0, Wire.PERIODO_HB_MS, TimeUnit.MILLISECONDS);

        // ATIVIDADE 3: Thread Principal - Servidor TCP de Recuperação (inalterado)
        try (ServerSocket server = new ServerSocket(porto)) {
            while (true) {
                try (Socket s = server.accept();
                     BufferedReader in = new BufferedReader(
                             new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                     PrintWriter out = new PrintWriter(
                             new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true)) {

                    s.setSoTimeout(Wire.T_TCP_MS);
                    String pedido = in.readLine();
                    if (pedido == null) continue;

                    String[] parts = pedido.trim().split("\\s+");
                    if (parts.length != 3 || !parts[0].equalsIgnoreCase("PEDIDO")) {
                        out.println("ERRO Pedido inválido");
                        continue;
                    }

                    long de = Long.parseLong(parts[1]);
                    long ate = Long.parseLong(parts[2]);

                    if (de > ate || de < 1) {
                        out.println("ERRO Intervalo inválido");
                        continue;
                    }

                    long limiteMax;
                    synchronized (lock) {
                        limiteMax = seqHolder[0];
                    }
                    if (ate > limiteMax) {
                        ate = limiteMax;
                    }

                    List<SensorRecord> lista = file.range(de, ate);
                    for (SensorRecord r : lista) {
                        out.println(r.toLine());
                    }
                    out.println("FIM");

                    System.out.println("RECUPERAÇÃO: " + de + ".." + ate + " para " + s.getRemoteSocketAddress() + " (" + lista.size() + " registos)");

                } catch (IOException e) {
                    System.out.println("Recuperação interrompida: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.out.println("Erro no ServerSocket TCP: " + e.getMessage());
        }
    }

    // Fecha o lote atual e publica-o: é simultaneamente o heartbeat da partição.
    private static void enviarLote(int k, String host, int porto, long inc, IpfsPubSub ipfs, Path hbFile,
                                   Object lock, long[] seqHolder, long[] hbHolder,
                                   List<List<SensorRecord>> pendentesHolder) {
        List<SensorRecord> lote;
        long ultimoSeq;
        long hb;
        // Mesma secção sincronizada que a atribuição de seq: troca pendentes, lê ultimoSeq e incrementa hb.
        // Assim ultimoSeq é exatamente o seq do último registo que está em 'lote' (ou já tinha sido publicado).
        synchronized (lock) {
            lote = pendentesHolder.get(0);
            pendentesHolder.set(0, new ArrayList<>());
            ultimoSeq = seqHolder[0];
            hb = ++hbHolder[0];
        }

        // Fora da secção sincronizada: a publicação no IPFS é lenta e não deve bloquear a atribuição de seq.
        Batch batch = new Batch(host, porto, inc, hb, ultimoSeq, lote);
        String msg = batch.encode();
        try {
            ipfs.publish(Wire.topicData(k), msg);
            int bytes = msg.getBytes(StandardCharsets.UTF_8).length;
            Files.writeString(hbFile, inc + ";" + hb + ";" + System.currentTimeMillis() + ";" + ultimoSeq + ";"
                            + lote.size() + ";" + bytes + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            System.out.println("HEARTBEAT: hb=" + hb + " n=" + lote.size() + " ultimoSeq=" + ultimoSeq);
        } catch (Exception e) {
            // Não volta a publicar os registos: a recuperação fica a cargo dos participantes (ultimoSeq do lote seguinte)
            System.out.println("ERRO a publicar o lote hb=" + hb + ": " + e.getMessage());
        }
    }
}
