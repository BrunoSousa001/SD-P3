package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class Producer {
    public static void main(String[] args) throws Exception {
        int numParticoes = 3;
        Map<Integer, HeartbeatMonitor> monitors = new HashMap<>();

        for (int i = 0; i < numParticoes; i++) {
            monitors.put(i, new HeartbeatMonitor("produtor"));
        }

        // Subscrever os tópicos para atualizar os heartbeats
        for (int i = 0; i < numParticoes; i++) {
            final int k = i;
            PubSub.subscribe(Wire.topicData(k), msg -> {
                Batch b = Batch.decodeHeader(msg);
                monitors.get(k).onHeartbeat(b.term, b.leader, b.hb);
            });
        }

        String[] sensores = {"S00", "S01", "S02", "S03", "S04", "S05"};
        List<String> logs = new ArrayList<>();

        System.out.println("Produtor arrancado. A enviar leituras...");

        while (true) {
            for (String s : sensores) {
                int k = Integer.parseInt(s.substring(1)) % numParticoes; // Distribui sensores
                double temp = 20.0 + ThreadLocalRandom.current().nextDouble() * 10.0;

                HeartbeatMonitor mon = monitors.get(k);
                mon.checkTimeout(System.currentTimeMillis());

                if (mon.isSuspicious()) {
                    System.out.println("AVISO: partição p" + k + " sem líder conhecido; leitura " + s + " pode perder-se");
                }

                // Para o produtor, L e V são geridos de forma simples (cada produtor é independente)
                VectorClock v = new VectorClock(numParticoes);
                String msg = Wire.in(s, temp, System.currentTimeMillis(), v);

                PubSub.publish(Wire.topicIn(k), msg);

                // Registo no ficheiro produtor.txt
                logs.add(System.currentTimeMillis() + ";p" + k + ";" + s + ";" + temp);
            }

            try {
                Files.write(Paths.get("produtor.txt"), logs, StandardCharsets.UTF_8);
            } catch (IOException ignored) {}

            Thread.sleep(1000); // Envia a cada 1 segundo
        }
    }
}