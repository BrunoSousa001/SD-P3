package rep01;

import java.io.IOException;
import java.util.Scanner;

public class PubSubLeader {
    static final String TOPIC = "rep01-temperaturas";

    public static void main(String[] args) {
        String api = args.length > 0 ? args[0] : "http://127.0.0.1:5001/api/v0";
        IpfsPubSub ipfs = new IpfsPubSub(api);

        try (Scanner sc = new Scanner(System.in)) {
            RecordFile file = new RecordFile("lider.txt");
            long seq = file.lastSeq();                              // continua a numeração após reinício
            System.out.println("Líder PubSub pronto (tópico: " + TOPIC + "). Formato: <sensor> <temperatura> | 'sair' para terminar");

            while (true) {
                System.out.print("> ");
                if (!sc.hasNextLine()) break;                       // bloqueia à espera do utilizador
                String line = sc.nextLine().trim();
                if (line.equalsIgnoreCase("sair")) break;

                String[] p = line.split("\\s+");

                if (p[0].equalsIgnoreCase("rajada")) {
                    if (p.length != 2) {
                        System.out.println("Uso: rajada <n>");
                        continue;
                    }
                    long tempoinicio = System.currentTimeMillis();
                    int nrajadas = Integer.parseInt(p[1]);
                    while (nrajadas != 0) {
                        double randomTemp = 15.0 + Math.random() * 15.0;
                        SensorRecord r = SensorRecord.now(++seq, "SIM", randomTemp);
                        file.append(r);
                        ipfs.publish(TOPIC, r.toLine());
                        nrajadas--;
                    }
                    long tempofim = System.currentTimeMillis();
                    System.out.println("Inicio: " + tempoinicio + " | Fim: " + tempofim + " | Tempo: " + (tempofim - tempoinicio) + " ms");
                    continue;
                }

                if (p.length != 2) {
                    System.out.println("Formato inválido.");
                    continue;
                }
                double temp;
                try {
                    temp = Double.parseDouble(p[1]);
                } catch (NumberFormatException e) {
                    System.out.println("Temperatura inválida: " + p[1]);
                    continue;
                }
                SensorRecord r = SensorRecord.now(++seq, p[0], temp);
                file.append(r);                                     // 1) escreve localmente
                ipfs.publish(TOPIC, r.toLine());                    // 2) propaga para o tópico
                System.out.println("Registado e publicado: " + r.toLine());
            }
        } catch (Exception e) {
            System.out.println("Erro: " + e.getMessage());
        }
    }
}
