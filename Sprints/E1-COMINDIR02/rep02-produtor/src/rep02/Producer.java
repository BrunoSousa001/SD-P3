package rep02;

import java.io.IOException;
import java.util.Random;
import java.util.Scanner;

public class Producer {
    public static void main(String[] args) {
        int n = args.length > 0 ? Integer.parseInt(args[0]) : 1;
        String api = args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0";

        Partitioner partitioner = new Partitioner(n);
        IpfsPubSub ipfs = new IpfsPubSub(api);
        Random rnd = new Random();

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
                        ipfs.publish(Wire.topicIn(k), Wire.in(sensor, Math.round(temp * 10.0) / 10.0));
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
                        ipfs.publish(Wire.topicIn(k), Wire.in(sensor, Math.round(temp * 10.0) / 10.0));
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

                int k = partitioner.of(sensor);
                ipfs.publish(Wire.topicIn(k), Wire.in(sensor, temp));
                System.out.println("Enviado para p" + k + " (" + Wire.topicIn(k) + "): " + Wire.in(sensor, temp));
            }
        } catch (Exception e) {
            System.out.println("Erro no produtor: " + e.getMessage());
        }
    }
}
