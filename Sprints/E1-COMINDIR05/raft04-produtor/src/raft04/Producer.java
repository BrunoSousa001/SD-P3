package raft04;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Produtor de leituras: lê comandos da consola ('rajada' ou '<sensor> <temp>'),
 * monitoriza heartbeats por partição para avisar se o líder estiver SUSPEITO e publica IN em raft04-in-p<k>.
 */
public class Producer {
    public static void main(String[] args) throws Exception {
        int numParticoes = args.length > 0 ? Integer.parseInt(args[0]) : 3;
        IpfsPubSub ipfs = new IpfsPubSub(Wire.IPFS_API);

        Map<Integer, HeartbeatMonitor> monitors = new HashMap<>();
        for (int i = 0; i < numParticoes; i++) monitors.put(i, new HeartbeatMonitor("produtor", i));

        // Subscreve os cabeçalhos dos lotes de cada partição apenas para alimentar o detetor de falhas
        for (int i = 0; i < numParticoes; i++) {
            final int k = i;
            new Thread(() -> {
                try {
                    ipfs.subscribe(Wire.topicData(k), msg -> {
                        try {
                            Batch b = Batch.decodeHeader(msg);
                            monitors.get(k).onHeartbeat(b.term, b.leader, b.hb);
                        } catch (Exception e) {
                            System.err.println("[p" + k + "] LOTE inválido: " + e.getMessage());
                        }
                    });
                } catch (Exception e) {
                    System.err.println("Erro ao subscrever tópico de dados: " + e.getMessage());
                }
            }).start();
        }

        // Thread periódica que verifica se alguma partição entrou em SUSPEITA por falta de heartbeats
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
        exec.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            for (int k = 0; k < numParticoes; k++) monitors.get(k).checkTimeout(now);
        }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);

        Scanner scanner = new Scanner(System.in);
        System.out.println("Comandos: 'rajada <n> <delay_ms>' ou '<sensor> <temp>'");
        String[] todosSensores = {"S00", "S01", "S02", "S03", "S04", "S05"};

        // Loop interativo de leitura da consola: aceita 'rajada <n> <delay_ms>' ou '<sensor> <temp>'
        while (true) {
            if (scanner.hasNextLine()) {
                String line = scanner.nextLine().trim();
                if (line.isBlank()) continue;
                // Modo rajada: envia numLeituras pelos 6 sensores com pausa delay entre envios
                if (line.startsWith("rajada ")) {
                    String[] p = line.split("\\s+");
                    int numLeituras = Integer.parseInt(p[1]);
                    long delay = Long.parseLong(p[2]);
                    for (int i = 0; i < numLeituras; i++) {
                        String sensor = todosSensores[i % todosSensores.length];
                        double temp = Math.round((20.0 + ThreadLocalRandom.current().nextDouble() * 10) * 10.0) / 10.0;
                        enviarLeitura(sensor, temp, numParticoes, monitors, ipfs);
                        if (delay > 0) Thread.sleep(delay);
                    }
                // Modo manual: envia uma leitura única <sensor> <temperatura>
                } else {
                    String[] p = line.split("\\s+");
                    if (p.length >= 2) {
                        enviarLeitura(p[0], Double.parseDouble(p[1]), numParticoes, monitors, ipfs);
                    }
                }
            } else {
                Thread.sleep(100);
            }
        }
    }

    // Publica um IN na partição correspondente e regista-o em produtor.txt (sem retenção nem reenvio).
    private static void enviarLeitura(String sensor, double temp, int n, Map<Integer, HeartbeatMonitor> monitors, IpfsPubSub ipfs) throws Exception {
        int k = Partitioner.getPartition(sensor, n);
        monitors.get(k).checkTimeout(System.currentTimeMillis());
        if (monitors.get(k).isSuspicious()) {
            // Se o líder for suspeito, avisa mas envia na mesma (leituras durante eleição perdem-se)
            String aviso = "partição p" + k + " sem líder conhecido; leitura " + sensor + " pode perder-se";
            System.out.println("AVISO: " + aviso);
            monitors.get(k).logAviso(aviso);
        }

        VectorClock v = new VectorClock(n);
        ipfs.publish(Wire.topicIn(k), Wire.in(sensor, temp, 0L, v));

        String linha = System.currentTimeMillis() + ";p" + k + ";" + sensor + ";" + temp + "\n";
        Files.writeString(Paths.get("produtor.txt"), linha, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
}