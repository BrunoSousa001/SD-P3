package rep02;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class PartitionReplica {
    public static void main(String[] args) {
        String id = args.length > 0 ? args[0] : "1";
        String particoesStr = args.length > 1 ? args[1] : "0";
        int apiPort = args.length > 2 ? Integer.parseInt(args[2]) : 5001;
        String api = "http://127.0.0.1:" + apiPort + "/api/v0";

        // Parâmetros de simulação de falhas para os testes do professor:
        // -Dperda=<probabilidade> (ex: -Dperda=0.2 descarta 20% das mensagens recebidas)
        // -Ddescartar=<seq> (descarta apenas o registo com esse seq)
        final double probPerda = System.getProperty("perda") != null ? Double.parseDouble(System.getProperty("perda")) : 0.0;
        final long seqDescartar = System.getProperty("descartar") != null ? Long.parseLong(System.getProperty("descartar")) : -1;
        final Set<Long> descartados = Collections.synchronizedSet(new HashSet<>());
        final Random rnd = new Random();

        String[] parts = particoesStr.split(",");
        System.out.println("Réplica " + id + " iniciada. Partições atribuídas: " + Arrays.toString(parts));

        // Cria uma thread de subscrição independente para CADA partição atribuída à réplica
        for (String pStr : parts) {
            int k = Integer.parseInt(pStr.trim());
            new Thread(() -> {
                executarParticao(id, k, api, probPerda, seqDescartar, descartados, rnd);
            }, "Replica-" + id + "-p" + k).start();
        }
    }

    private static void executarParticao(String id, int k, String api, double probPerda, long seqDescartar, Set<Long> descartados, Random rnd) {
        RecordFile file = new RecordFile("replica-" + id + "-p" + k + ".txt");
        final long[] ultimoHolder = new long[1];
        try {
            ultimoHolder[0] = file.lastSeq();
        } catch (IOException e) {
            System.out.println("[p" + k + "] Erro ao ler lastSeq: " + e.getMessage());
            return;
        }

        TreeMap<Long, SensorRecord> retidos = new TreeMap<>();
        String[] leaderContact = new String[2]; // leaderContact[0]=host, leaderContact[1]=porto

        try {
            IpfsPubSub ipfs = new IpfsPubSub(api);
            System.out.println("[p" + k + "] À escuta no tópico " + Wire.topicData(k) + ", seq inicial=" + ultimoHolder[0]);

            ipfs.subscribe(Wire.topicData(k), line -> {
                try {
                    // 1. Desmonta a mensagem REG usando o Wire: { host, porto, linhaDoRegisto }
                    String[] reg = Wire.regParts(line);
                    leaderContact[0] = reg[0];
                    leaderContact[1] = reg[1];

                    SensorRecord r = SensorRecord.fromLine(reg[2]);
                    long seq = r.getSeq();

                    // Simulação controlada de falhas (Testes do Enunciado)
                    if (seq == seqDescartar && descartados.add(seq)) {
                        System.out.println("[p" + k + "] DESCARTADO (simulação): " + seq);
                        return;
                    }
                    if (probPerda > 0 && rnd.nextDouble() < probPerda) {
                        System.out.println("[p" + k + "] DESCARTADO (simulação perda): " + seq);
                        return;
                    }

                    // 2. Aplicação da Regra de Sequência e Consistência:
                    if (seq == ultimoHolder[0] + 1) {
                        file.append(r);
                        ultimoHolder[0]++;
                        System.out.println("[p" + k + "] Aplicado: " + r.toLine());

                        // Efeito cascata para registos retidos que ficaram em sequência
                        while (retidos.containsKey(ultimoHolder[0] + 1)) {
                            SensorRecord r2 = retidos.remove(ultimoHolder[0] + 1);
                            file.append(r2);
                            ultimoHolder[0]++;
                            System.out.println("[p" + k + "] Aplicado (cascata): " + r2.toLine());
                        }
                    } else if (seq <= ultimoHolder[0] || retidos.containsKey(seq)) {
                        System.out.println("[p" + k + "] DUPLICADO: " + seq);
                    } else if (seq > ultimoHolder[0] + 1) {
                        retidos.put(seq, r);
                        System.out.println("[p" + k + "] EM ESPERA: " + seq + " (falta o registo " + (ultimoHolder[0] + 1) + ", retidos=" + retidos.size() + ")");

                        // 3. A GRANDE NOVIDADE: GATILHO DE RECUPERAÇÃO POR TCP!
                        if (retidos.size() > Wire.LIMITE_RETIDOS) {
                            long de = ultimoHolder[0] + 1;
                            long ate = retidos.firstKey() - 1;

                            if (leaderContact[0] != null && leaderContact[1] != null) {
                                String host = leaderContact[0];
                                int port = Integer.parseInt(leaderContact[1]);
                                recuperarPorTcp(id, k, file, host, port, de, ate, retidos);
                                ultimoHolder[0] = file.lastSeq(); // atualiza o último seq após recuperação
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("[p" + k + "] REJEITADO: " + line);
                }
            });
        } catch (Exception e) {
            System.out.println("[p" + k + "] Erro na partição: " + e.getMessage());
        }
    }

    // Método que abre a ligação TCP direta ao Líder para pedir os registos em falta
    private static void recuperarPorTcp(String id, int k, RecordFile file, String host, int port, long de, long ate, TreeMap<Long, SensorRecord> retidos) {
        System.out.println("[p" + k + "] RECUPERAÇÃO: " + de + ".." + ate + " pedida a " + host + ":" + port);
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(host, port), Wire.T_TCP_MS);
            s.setSoTimeout(Wire.T_TCP_MS);

            try (BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                 PrintWriter out = new PrintWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true)) {

                // Envia o pedido no formato do protocolo: "PEDIDO <de> <até>"
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

                    SensorRecord r = SensorRecord.fromLine(line);
                    file.append(r);
                    System.out.println("[p" + k + "] RECUPERADO: " + r.getSeq());
                }

                // Após receber tudo do líder por TCP, descarrega em cascata os retidos da memória
                long ult = file.lastSeq();
                while (retidos.containsKey(ult + 1)) {
                    SensorRecord r2 = retidos.remove(ult + 1);
                    file.append(r2);
                    ult++;
                    System.out.println("[p" + k + "] Aplicado (pós-recuperação): " + r2.toLine());
                }
            }
        } catch (Exception e) {
            System.out.println("[p" + k + "] RECUPERAÇÃO FALHOU: " + e.getMessage());
        }
    }
}
