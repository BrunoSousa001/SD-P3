package rep04;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PartitionReplica {
    public static void main(String[] args) {
        String id = args.length > 0 ? args[0] : "1";
        String particoesStr = args.length > 1 ? args[1] : "0";
        int apiPort = args.length > 2 ? Integer.parseInt(args[2]) : 5001;
        int n = args.length > 3 ? Integer.parseInt(args[3]) : 3;   // número de partições (dimensão do vetor)
        String api = "http://127.0.0.1:" + apiPort + "/api/v0";

        // Simulação de perda de lotes: -Dperda=<probabilidade> descarta essa fração dos LOTE recebidos
        final double probPerda = System.getProperty("perda") != null ? Double.parseDouble(System.getProperty("perda")) : 0.0;

        MonitorLog log = new MonitorLog("replica" + id);
        List<HeartbeatMonitor> monitores = new ArrayList<>();

        String[] parts = particoesStr.split(",");
        System.out.println("Réplica " + id + " iniciada. Partições atribuídas: " + Arrays.toString(parts)
                + " (periodo=" + Wire.PERIODO_HB_MS + " ms, fator=" + Wire.FATOR_TIMEOUT + ")");

        // Uma thread de subscrição por partição, cada uma com o seu HeartbeatMonitor
        for (String pStr : parts) {
            int k = Integer.parseInt(pStr.trim());
            HeartbeatMonitor monitor = new HeartbeatMonitor(k, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, log::log);
            monitores.add(monitor);
            Particao p = new Particao(id, k, n, api, monitor, log, probPerda);
            new Thread(p::executar, "Replica-" + id + "-p" + k).start();
        }

        // Thread de verificação: chama check() em todos os monitores a cada VERIFICACAO_MS
        ScheduledExecutorService verificacao = Executors.newSingleThreadScheduledExecutor();
        verificacao.scheduleAtFixedRate(() -> {
            try {
                for (HeartbeatMonitor m : monitores) m.check();
            } catch (Throwable t) {
                System.out.println("Erro na verificação: " + t);
            }
        }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);
    }

    // Estado e comportamento da réplica para UMA partição (acedido apenas pela thread de subscrição respetiva)
    private static final class Particao {
        private final String id;
        private final int k;
        private final int n;
        private final String api;
        private final HeartbeatMonitor monitor;
        private final MonitorLog log;
        private final double probPerda;
        private final Random rnd = new Random();

        private RecordFile file;
        private long ultimo;                                   // último seq aplicado
        private final TreeMap<Long, SensorRecord> retidos = new TreeMap<>();
        private String leaderHost;                             // contacto do líder, do cabeçalho do último LOTE
        private int leaderPort;
        private boolean recuperando = false;                   // evita recuperações encadeadas

        Particao(String id, int k, int n, String api, HeartbeatMonitor monitor, MonitorLog log, double probPerda) {
            this.id = id;
            this.k = k;
            this.n = n;
            this.api = api;
            this.monitor = monitor;
            this.log = log;
            this.probPerda = probPerda;
        }

        void executar() {
            file = new RecordFile("replica-" + id + "-p" + k + ".txt", n);
            try {
                ultimo = file.lastSeq();
            } catch (IOException e) {
                System.out.println("[p" + k + "] Erro ao ler lastSeq: " + e.getMessage());
                return;
            }
            try {
                IpfsPubSub ipfs = new IpfsPubSub(api);
                System.out.println("[p" + k + "] À escuta no tópico " + Wire.topicData(k) + ", seq inicial=" + ultimo);
                ipfs.subscribe(Wire.topicData(k), msg -> {
                    try {
                        lote(msg);
                    } catch (Exception e) {
                        System.out.println("[p" + k + "] REJEITADO: " + e.getMessage());
                    }
                });
            } catch (Exception e) {
                System.out.println("[p" + k + "] Erro na partição: " + e.getMessage());
            }
        }

        // Por cada LOTE: atualiza o detetor, aplica os registos e, se faltar algum, recupera por TCP
        private void lote(String msg) throws IOException {
            Batch b = Batch.decode(msg, n);

            if (probPerda > 0 && rnd.nextDouble() < probPerda) {
                System.out.println("[p" + k + "] LOTE DESCARTADO (simulação): hb=" + b.hb);
                return;
            }

            // 1. O detetor vê o heartbeat (mesmo que seja de uma incarnação antiga, os registos são tratados a seguir)
            leaderHost = b.address;
            leaderPort = b.port;
            monitor.onHeartbeat(b.inc, b.hb);
            log.log("RECEBIDO;p" + k + ";inc=" + b.inc + ";hb=" + b.hb + ";ultimoSeq=" + b.lastSeq
                    + ";n=" + b.records.size() + ";seq=" + b.seqRange());

            // 2. Aplica os registos do lote pela ordem (regra da COMINDIR02)
            for (SensorRecord r : b.records) {
                aplicar(r);
            }

            // 3. ultimoSeq: se ficou algum registo por aplicar, recupera-o por TCP no endereço do cabeçalho
            if (ultimo < b.lastSeq) {
                long de = ultimo + 1;
                long ate = retidos.isEmpty() ? b.lastSeq : retidos.firstKey() - 1;
                log.log("RECUPERACAO;p" + k + ";de=" + de + ";ate=" + ate);
                recuperarPorTcp(b.address, b.port, de, ate);
            }
        }

        // Regra da COMINDIR02: aplica se for o seguinte; ignora duplicados; retém se houver lacuna
        private void aplicar(SensorRecord r) throws IOException {
            long seq = r.getSeq();
            if (seq == ultimo + 1) {
                file.append(r);
                ultimo++;
                System.out.println("[p" + k + "] Aplicado: " + r.toLine());
                while (retidos.containsKey(ultimo + 1)) {
                    SensorRecord r2 = retidos.remove(ultimo + 1);
                    file.append(r2);
                    ultimo++;
                    System.out.println("[p" + k + "] Aplicado (cascata): " + r2.toLine());
                }
            } else if (seq <= ultimo || retidos.containsKey(seq)) {
                System.out.println("[p" + k + "] DUPLICADO: " + seq);
            } else {
                retidos.put(seq, r);
                System.out.println("[p" + k + "] EM ESPERA: " + seq + " (falta o registo " + (ultimo + 1) + ", retidos=" + retidos.size() + ")");
                // Regra da COMINDIR02: demasiados retidos => a lacuna é resolvida por TCP
                if (retidos.size() > Wire.LIMITE_RETIDOS && !recuperando && leaderHost != null) {
                    long de = ultimo + 1;
                    long ate = retidos.firstKey() - 1;
                    log.log("RECUPERACAO;p" + k + ";de=" + de + ";ate=" + ate);
                    recuperarPorTcp(leaderHost, leaderPort, de, ate);
                }
            }
        }

        // Liga ao líder por TCP e pede os registos em falta; os recebidos passam pela mesma regra de aplicação
        private void recuperarPorTcp(String host, int port, long de, long ate) {
            System.out.println("[p" + k + "] RECUPERAÇÃO: " + de + ".." + ate + " pedida a " + host + ":" + port);
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress(host, port), Wire.T_TCP_MS);
                s.setSoTimeout(Wire.T_TCP_MS);

                try (BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                     PrintWriter out = new PrintWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true)) {

                    out.println("PEDIDO " + de + " " + ate);

                    String line;
                    while ((line = in.readLine()) != null) {
                        if (line.equals("FIM")) {
                            break;
                        }
                        if (line.startsWith("ERRO")) {
                            System.out.println("[p" + k + "] RECUPERAÇÃO FALHOU: " + line);
                            return;
                        }
                        SensorRecord r = SensorRecord.fromLine(line, n);
                        System.out.println("[p" + k + "] RECUPERADO: " + r.getSeq());
                        aplicar(r);
                    }
                }
            } catch (Exception e) {
                System.out.println("[p" + k + "] RECUPERAÇÃO FALHOU: " + e.getMessage());
            }
        }
    }
}
