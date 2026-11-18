package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Processador de alarmes: subscreve os LOTE de todas as partições, filtra termos obsoletos,
 * deduplica registos por (seq, termo), mantém (Lp, Vp) e publica ALARM-<sensor> quando temp >= LIMIAR_ALARME.
 */
public class AlarmProcessor {
    private static final Object lock = new Object();
    private static VectorClock vp;
    private static long lp = 0;
    private static int n;

    public static void main(String[] args) throws Exception {
        n = args.length > 0 ? Integer.parseInt(args[0]) : 3;
        vp = new VectorClock(n);
        IpfsPubSub ipfs = new IpfsPubSub(Wire.IPFS_API);

        Map<Integer, HeartbeatMonitor> monitors = new HashMap<>();
        Map<Integer, Long> maxTermoPorParticao = new ConcurrentHashMap<>();
        Set<String> processados = ConcurrentHashMap.newKeySet();

        for (int k = 0; k < n; k++) {
            final int partition = k;
            monitors.put(partition, new HeartbeatMonitor("processador", partition));

            // Subscreve os lotes de cada partição numa thread dedicada
            new Thread(() -> {
                try {
                    ipfs.subscribe(Wire.topicData(partition), msg -> {
                        try {
                            // 1. Atualiza o detetor de falhas da partição (ignora termos antigos internamente)
                            Batch header = Batch.decodeHeader(msg);
                            monitors.get(partition).onHeartbeat(header.term, header.leader, header.hb);

                            // 2. Descarta lotes de termos inferiores ao maior termo já observado nesta partição
                            long maxTerm = maxTermoPorParticao.getOrDefault(partition, 0L);
                            if (header.term < maxTerm) return;
                            maxTermoPorParticao.put(partition, header.term);

                            Batch full = Batch.decode(msg, n);
                            for (SensorRecord r : full.records) {
                                // Evita gerar alarmes em cadeia sobre registos de alarme
                                if (r.getSensorId().startsWith("ALARM-")) continue;

                                // 3. Deduplicação por (seq, termo): um mesmo seq de outro termo (após substituição de líder)
                                // representa uma nova escrita e deve ser avaliado de novo
                                String id = "p" + partition + ":" + r.getSeq() + ":" + r.getTerm();
                                long lSnap;
                                VectorClock vSnap;
                                synchronized (lock) {
                                    // Se este par (seq, termo) já foi tratado, ignora (evita duplicados)
                                    if (!processados.add(id)) continue;
                                    // Atualiza conhecimento causal (Lp, Vp) com todos os registos válidos observados
                                    lp = Math.max(lp, r.getLamport());
                                    vp.merge(r.getVector());
                                    // Só publica alarme se a temperatura atingir ou ultrapassar o limiar configurado
                                    if (r.getTemperature() < Wire.LIMIAR_ALARME) continue;
                                    lSnap = lp;
                                    vSnap = vp.copy();
                                }

                                // 4. Calcula a partição de destino pelo identificador "ALARM-<sensor>" (coincide com PartitionNode)
                                String alarmMsg = "ALARM-" + r.getSensorId();
                                int targetPartition = Partitioner.getPartition(alarmMsg, n);
                                if (monitors.get(targetPartition).isSuspicious()) {
                                    // Sem líder conhecido na partição de destino, avisa mas publica na mesma
                                    String aviso = "partição p" + targetPartition + " sem líder conhecido; alarme " + alarmMsg + " pode perder-se";
                                    System.out.println("AVISO: " + aviso);
                                    monitors.get(targetPartition).logAviso(aviso);
                                }

                                try {
                                    ipfs.publish(Wire.topicIn(targetPartition), Wire.in(alarmMsg, r.getTemperature(), lSnap, vSnap));
                                } catch (Exception ignored) {}

                                String linhaAlarme = System.currentTimeMillis() + ";ALARME;" + alarmMsg + ";temp=" + r.getTemperature() + "\n";
                                synchronized (lock) {
                                    try {
                                        Files.writeString(Paths.get("alarmes-processador.txt"), linhaAlarme,
                                                StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                                    } catch (IOException ignored) {}
                                }
                            }
                        } catch (Exception e) {
                            System.err.println("[p" + partition + "] Erro ao processar lote: " + e.getMessage());
                        }
                    });
                } catch (Exception e) {
                    System.err.println("[p" + partition + "] Erro na subscrição: " + e.getMessage());
                }
            }).start();
        }

        // Thread periódica para detetar timeouts de heartbeat (SUSPEITA) a cada VERIFICACAO_MS
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        exec.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            for (int k = 0; k < n; k++) monitors.get(k).checkTimeout(now);
        }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);

        Thread.sleep(Long.MAX_VALUE);
    }
}