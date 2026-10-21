package rep02;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class PartitionLeader {
    public static void main(String[] args) {
        int k = args.length > 0 ? Integer.parseInt(args[0]) : 0;
        int n = args.length > 1 ? Integer.parseInt(args[1]) : 1;
        String host = args.length > 2 ? args[2] : "127.0.0.1";
        String api = args.length > 3 ? args[3] : "http://127.0.0.1:5001/api/v0";

        int porto = Wire.PORTO_BASE + k;
        RecordFile file = new RecordFile("lider-p" + k + ".txt");
        Partitioner partitioner = new Partitioner(n);
        IpfsPubSub ipfs = new IpfsPubSub(api);

        final Object lock = new Object();
        final long[] seqHolder = new long[1];

        try {
            seqHolder[0] = file.lastSeq();
        } catch (IOException e) {
            System.out.println("Erro ao ler lastSeq: " + e.getMessage());
            return;
        }

        System.out.println("Líder p" + k + " pronto. Recuperação TCP em " + host + ":" + porto + ", seq inicial=" + seqHolder[0]);

        // ATIVIDADE 1: Thread de Entrada (ouve o Produtor no IPFS e propaga)
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
                        if (f.length != 2) {
                            System.out.println("REJEITADO: " + line);
                            return;
                        }

                        String sensor = f[0]; //PEGA EM S01
                        double temp = Double.parseDouble(f[1]);

                        // Validação de Partição (Ponto 9 da Ficha)
                        int partEsperada = partitioner.of(sensor);
                        if (partEsperada != k) {
                            System.out.println("PARTIÇÃO ERRADA: " + sensor + " pertence a p" + partEsperada);
                            return;
                        }

                        SensorRecord r;
                        synchronized (lock) {
                            seqHolder[0]++;
                            r = SensorRecord.now(seqHolder[0], sensor, temp);
                            file.append(r); // 1. Grava no disco local
                        }

                        // 2. Propaga no IPFS com endereço e porto anunciados
                        ipfs.publish(Wire.topicData(k), Wire.reg(host, porto, r));
                        System.out.println("Registado e publicado p" + k + ": " + r.toLine());

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

        // ATIVIDADE 2: Thread Principal - Servidor TCP de Recuperação
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
}
