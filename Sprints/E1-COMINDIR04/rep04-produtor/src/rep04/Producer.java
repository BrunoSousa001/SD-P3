package rep04;

import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Producer {
    public static void main(String[] args) {
        int n = args.length > 0 ? Integer.parseInt(args[0]) : 1;
        String api = args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0";

        Partitioner partitioner = new Partitioner(n);
        IpfsPubSub ipfs = new IpfsPubSub(api);
        Random rnd = new Random();
        MonitorLog log = new MonitorLog("produtor");

        // Um HeartbeatMonitor por partição, alimentado só com o cabeçalho dos LOTE
        HeartbeatMonitor[] monitores = new HeartbeatMonitor[n];
        for (int k = 0; k < n; k++) {
            final int kk = k;
            monitores[k] = new HeartbeatMonitor(k, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, log::log);
            Thread t = new Thread(() -> escutar(kk, monitores[kk], ipfs, log), "Produtor-p" + k);
            t.setDaemon(true);
            t.start();
        }
        ScheduledExecutorService verificacao = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "Verificacao");
            t.setDaemon(true);
            return t;
        });
        verificacao.scheduleAtFixedRate(() -> {
            try {
                for (HeartbeatMonitor m : monitores) m.check();
            } catch (Throwable t) {
                System.out.println("Erro na verificação: " + t);
            }
        }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);

        System.out.println("Produtor pronto (N=" + n + "). Comandos: <sensor> <temp> | rajada <n> <s> | rajada-quente <n> <s> | 'sair'");

        try (Scanner sc = new Scanner(System.in)) {
            while (true) {
                System.out.print("> ");
                if (!sc.hasNextLine()) break;
                String line = sc.nextLine().trim();
                if (line.equalsIgnoreCase("sair")) break;
                if (line.isBlank()) continue;

                String[] p = line.split("\\s+");

                // Comando: rajada <n> <s>
                if (p[0].equalsIgnoreCase("rajada")) {
                    if (p.length != 3) {
                        System.out.println("Uso: rajada <n> <s>");
                        continue;
                    }
                    int total = Integer.parseInt(p[1]);
                    int sMax = Integer.parseInt(p[2]);
                    int[] porParticao = new int[n];

                    long t0 = System.currentTimeMillis();
                    for (int i = 0; i < total; i++) {
                        int sNum = 1 + rnd.nextInt(sMax);
                        String sensor = String.format("S%02d", sNum);
                        double temp = 15.0 + rnd.nextDouble() * 15.0;

                        int k = partitioner.of(sensor);
                        porParticao[k]++;
                        enviar(ipfs, monitores, log, n, k, sensor, Math.round(temp * 10.0) / 10.0, false);
                    }
                    long t1 = System.currentTimeMillis();

                    System.out.println("Rajada concluída em " + (t1 - t0) + " ms (t0=" + t0 + ", t1=" + t1 + ")");
                    for (int k = 0; k < n; k++) {
                        System.out.println("  p" + k + ": " + porParticao[k] + " leituras");
                    }
                    continue;
                }

                // Comando: rajada-quente <n> <s> (80% em S01)
                if (p[0].equalsIgnoreCase("rajada-quente")) {
                    if (p.length != 3) {
                        System.out.println("Uso: rajada-quente <n> <s>");
                        continue;
                    }
                    int total = Integer.parseInt(p[1]);
                    int sMax = Integer.parseInt(p[2]);
                    int[] porParticao = new int[n];

                    long t0 = System.currentTimeMillis();
                    for (int i = 0; i < total; i++) {
                        String sensor;
                        if (rnd.nextDouble() < 0.8) {
                            sensor = "S01"; // 80% das leituras vêm de S01 (chave quente)
                        } else {
                            int sNum = sMax > 1 ? 2 + rnd.nextInt(sMax - 1) : 1;
                            sensor = String.format("S%02d", sNum);
                        }
                        double temp = 15.0 + rnd.nextDouble() * 15.0;

                        int k = partitioner.of(sensor);
                        porParticao[k]++;
                        enviar(ipfs, monitores, log, n, k, sensor, Math.round(temp * 10.0) / 10.0, false);
                    }
                    long t1 = System.currentTimeMillis();

                    System.out.println("Rajada-quente concluída em " + (t1 - t0) + " ms (t0=" + t0 + ", t1=" + t1 + ")");
                    for (int k = 0; k < n; k++) {
                        System.out.println("  p" + k + ": " + porParticao[k] + " leituras");
                    }
                    continue;
                }

                // Comando manual: <sensor> <temperatura>
                if (p.length != 2) {
                    System.out.println("Formato inválido. Use: <sensor> <temp>");
                    continue;
                }
                String sensor = p[0];
                double temp;
                try {
                    temp = Double.parseDouble(p[1]);
                } catch (NumberFormatException e) {
                    System.out.println("Temperatura inválida: " + p[1]);
                    continue;
                }

                enviar(ipfs, monitores, log, n, partitioner.of(sensor), sensor, temp, true);
            }
        } catch (Exception e) {
            System.out.println("Erro no produtor: " + e.getMessage());
        }
    }

    // Envia um IN à partição k. Se o líder é suspeito, avisa mas envia na mesma (não há retenção nem reenvio).
    private static void enviar(IpfsPubSub ipfs, HeartbeatMonitor[] monitores, MonitorLog log, int n, int k,
                               String sensor, double temp, boolean verbose) throws Exception {
        if (monitores[k].state() == HeartbeatMonitor.State.SUSPEITO) {
            System.out.println("AVISO: líder p" + k + " suspeito; leitura " + sensor + " pode perder-se");
            log.log("AVISO;p" + k + ";lider suspeito, leitura " + sensor + " pode perder-se");
        }
        // O produtor não observa registo algum: envia sempre L=0 e V=(0,...,0)
        String msg = Wire.in(sensor, temp, 0, new VectorClock(n));
        ipfs.publish(Wire.topicIn(k), msg);
        if (verbose) {
            System.out.println("Enviado para p" + k + " (" + Wire.topicIn(k) + "): " + msg);
        }
    }

    // Subscreve os LOTE da partição k e interpreta apenas o cabeçalho
    private static void escutar(int k, HeartbeatMonitor monitor, IpfsPubSub ipfs, MonitorLog log) {
        try {
            ipfs.subscribe(Wire.topicData(k), msg -> {
                try {
                    Batch h = Batch.decodeHeader(msg);
                    monitor.onHeartbeat(h.inc, h.hb);
                    log.log("RECEBIDO;p" + k + ";inc=" + h.inc + ";hb=" + h.hb + ";ultimoSeq=" + h.lastSeq + ";n=" + h.count);
                } catch (Exception e) {
                    System.out.println("[p" + k + "] REJEITADO: " + e.getMessage());
                }
            });
        } catch (Exception e) {
            System.out.println("[p" + k + "] Erro na subscrição: " + e.getMessage());
        }
    }
}
