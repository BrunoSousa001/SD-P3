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
            socket.joinGroup(group);                                // passa a receber o que é enviado ao grupo
            System.out.println("Réplica " + id + " à escuta em " + GROUP + ":" + PORT);
            byte[] buffer = new byte[1000];

            long ultimo = file.lastSeq();           //  último nº da sequência de registos
            TreeMap<Long, SensorRecord> retidos = new TreeMap<>();  //  TreeMap para registos com sequência desordenada

            while (true) {
                DatagramPacket p = new DatagramPacket(buffer, buffer.length);
                socket.receive(p);                                  // bloqueia até chegar um datagrama
                String line = new String(p.getData(), 0, p.getLength(), StandardCharsets.UTF_8);
                
                try {
                    SensorRecord r = SensorRecord.fromLine(line);
                    if(r.getSeq() == ultimo + 1){  //  Registo Correto
                        file.append(r);
                        System.out.println("Aplicado: " + r.toLine());
                        ultimo++;

                        while(retidos.containsKey(ultimo+1)){  //  Caso o registo seguinte esteja na TreeMap escreve-o no ficheiro local e remove de retidos
                            SensorRecord r2 = retidos.remove(ultimo+1);
                            file.append(r2);
                            System.out.println("Aplicado: " + r2.toLine());
                            ultimo++;
                        }
                    }
                    else if(r.getSeq() <= ultimo){  //  Registo Duplicado
                        System.out.println("DUPLICADO: " + r.getSeq());
                    }
                    else if(r.getSeq() > ultimo){  //  Registo Desordenado, regista num TreeMap enquanto não encontra a sequência em falta
                        System.out.println("EM ESPERA: " + r.getSeq() + "(falta o registo " + (ultimo + 1) + ")");
                        retidos.put(r.getSeq(), r);
                    }
                }
                catch(Exception e) {
                    System.out.println("REJEITADO: " + line);
                }
            }
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        }
    }
}



