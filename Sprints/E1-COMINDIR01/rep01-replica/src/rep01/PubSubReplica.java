package rep01;

import java.io.IOException;
import java.util.TreeMap;

public class PubSubReplica {
    static final String TOPIC = "rep01-temperaturas";

    static RecordFile file;
    static long ultimo;
    static final TreeMap<Long, SensorRecord> retidos = new TreeMap<>();

    // Variáveis de suporte para comando rajada
    static long tPrimeiro = 0;
    static long tUltimo = 0;
    static int recebidos = 0;
    static int escritos = 0;

    public static void main(String[] args) {
        String id = args.length > 0 ? args[0] : "1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 5001;
        String api = "http://127.0.0.1:" + port + "/api/v0";

        file = new RecordFile("replica-" + id + ".txt");
        try {
            ultimo = file.lastSeq();
            IpfsPubSub ipfs = new IpfsPubSub(api);
            System.out.println("Réplica PubSub " + id + " ligada a " + api + " no tópico " + TOPIC);

            ipfs.subscribe(TOPIC, line -> {
                try {
                    SensorRecord r = SensorRecord.fromLine(line);
                    if (r.getSensorId().equals("SIM")) {
                        if (tUltimo > 0 && System.currentTimeMillis() - tUltimo > 5000) {
                            tPrimeiro = 0;
                            recebidos = 0;
                            escritos = 0;
                        }
                        if (tPrimeiro == 0) tPrimeiro = System.currentTimeMillis();
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
                                + "\tEscritos: " + escritos + "\tRetidos: " + retidos.size());
                    }
                } catch (Exception e) {
                    System.out.println("REJEITADO: " + line);
                }
            });
        } catch (Exception e) {
            System.out.println("Erro na réplica PubSub: " + e.getMessage());
        }
    }
}
