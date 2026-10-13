package rep01;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.TreeMap;

public class MulticastReplica {
    static final String GROUP = "230.0.0.1";
    static final int PORT = 6789;

    public static void main(String[] args) {
        String id = args.length > 0 ? args[0] : "1";
        RecordFile file = new RecordFile("replica-" + id + ".txt");
        try (MulticastSocket socket = new MulticastSocket(PORT)) {

            InetAddress group = InetAddress.getByName(GROUP);
            socket.joinGroup(group); // passa a receber o que é enviado ao grupo
            System.out.println("Réplica " + id + " à escuta em " + GROUP + ":" + PORT);
            byte[] buffer = new byte[1000];

            long ultimo = file.lastSeq(); // último nº da sequência de registos
            TreeMap<Long, SensorRecord> retidos = new TreeMap<>(); // TreeMap para registos com sequência desordenada

            // Variáveis de suporte para comando rajada
            long tPrimeiro = 0;
            long tUltimo = 0;
            int recebidos = 0;
            int escritos = 0;

            while (true) {
                DatagramPacket p = new DatagramPacket(buffer, buffer.length);
                socket.receive(p); // bloqueia até chegar datagrama (ou 2s de timeout)
                String line = new String(p.getData(), 0, p.getLength(), StandardCharsets.UTF_8);

                try {
                    SensorRecord r = SensorRecord.fromLine(line);
                    if (r.getSensorId().equals("SIM")) {
                        if (tUltimo > 0 && System.currentTimeMillis() - tUltimo > 5000) {
                            tPrimeiro = 0;
                            recebidos = 0;
                            escritos = 0;
                        }
                        if (tPrimeiro == 0)
                            tPrimeiro = System.currentTimeMillis();
                        tUltimo = System.currentTimeMillis();
                        recebidos++;
                    }
                    if (r.getSeq() == ultimo + 1) { // Registo Correto
                        file.append(r);
                        if (!r.getSensorId().equals("SIM")) {
                            System.out.println("Aplicado: " + r.toLine());
                        }
                        ultimo++;
                        escritos++;
                        while (retidos.containsKey(ultimo + 1)) {
                            SensorRecord r2 = retidos.remove(ultimo + 1);
                            file.append(r2);
                            if (!r2.getSensorId().equals("SIM")) {
                                System.out.println("Aplicado: " + r2.toLine());
                            }
                            ultimo++;
                            escritos++;
                        }
                    } else if (r.getSeq() <= ultimo || retidos.containsKey(r.getSeq())) {
                        if (!r.getSensorId().equals("SIM")) {
                            System.out.println("DUPLICADO: " + r.getSeq());
                        }
                    } else if (r.getSeq() > ultimo) {
                        if (!r.getSensorId().equals("SIM")) {
                            System.out.println("EM ESPERA: " + r.getSeq() + "(falta o registo " + (ultimo + 1) + ")");
                        }
                        retidos.put(r.getSeq(), r);
                    }
                    if (r.getSensorId().equals("SIM")) {
                        System.out.print("\rInicio: " + tPrimeiro + "\tFim: " + tUltimo + "\tRecebidos: " + recebidos
                                + "\tEscritos: " + escritos + "\tRetidos: " + retidos.size() + "\n");
                    }
                } catch (Exception e) {
                    System.out.println("REJEITADO: " + line);
                }
            }
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        }
    }
}
