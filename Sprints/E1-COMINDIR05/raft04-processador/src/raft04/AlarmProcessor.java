package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class AlarmProcessor {
    public static void main(String[] args) throws Exception {
        int numParticoes = 3;
        Map<Integer, HeartbeatMonitor> monitors = new HashMap<>();

        // Para garantir que tratamos apenas termos recentes
        Map<Integer, Long> maxTermoPorParticao = new ConcurrentHashMap<>();

        // Para não processar o mesmo registo duas vezes (guarda "p0:seq:term")
        Set<String> processados = ConcurrentHashMap.newKeySet();

        List<String> alarmesLog = new ArrayList<>();

        System.out.println("Processador arrancado. À espera de LOTEs...");

        for (int k = 0; k < numParticoes; k++) {
            final int partition = k;
            monitors.put(k, new HeartbeatMonitor("processador"));

            PubSub.subscribe(Wire.topicData(k), msg -> {
                Batch header = Batch.decodeHeader(msg);

                // 1. Atualiza o monitor
                monitors.get(partition).onHeartbeat(header.term, header.leader, header.hb);
                monitors.get(partition).checkTimeout(System.currentTimeMillis());

                // 2. Ignora lotes com termos mais antigos do que já vimos
                long maxTerm = maxTermoPorParticao.getOrDefault(partition, 0L);
                if (header.term < maxTerm) return;
                maxTermoPorParticao.put(partition, header.term);

                // 3. Processa os registos
                Batch fullBatch = Batch.decode(msg, numParticoes);
                for (SensorRecord r : fullBatch.records) {
                    String idRegisto = "p" + partition + ":" + r.getSeq() + ":" + r.getTerm();

                    if (processados.contains(idRegisto)) continue; // Duplicado
                    processados.add(idRegisto);

                    // Se a temperatura for alta, gera alarme
                    if (r.getTemperature() > Wire.LIMIAR_ALARME) {
                        if (monitors.get(partition).isSuspicious()) {
                            String aviso = "AVISO: partição p" + partition + " sem líder conhecido; alarme " + r.getSensorId() + " pode perder-se";
                            System.out.println(aviso);
                            alarmesLog.add(System.currentTimeMillis() + ";AVISO;" + aviso);
                        }

                        String alarme = "ALARME: Partição p" + partition + " | Sensor " + r.getSensorId() + " | Temp " + r.getTemperature() + " | seq=" + r.getSeq();
                        System.out.println(alarme);
                        alarmesLog.add(System.currentTimeMillis() + ";ALARME;" + alarme);
                    }
                }
            });
        }

        // Thread de verificação de timeouts
        while (true) {
            long now = System.currentTimeMillis();
            for (int k = 0; k < numParticoes; k++) {
                monitors.get(k).checkTimeout(now);
            }
            try {
                Files.write(Paths.get("monitor-processador.txt"), alarmesLog, StandardCharsets.UTF_8);
            } catch (IOException ignored) {}
            Thread.sleep(500);
        }
    }
}