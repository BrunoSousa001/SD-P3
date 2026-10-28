package rep04;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Uso: AlarmProcessor <N> [api]
 * Subscreve os LOTE de todas as partições, acumula o conhecimento causal (Lp, Vp), publica alarmes
 * e mantém um HeartbeatMonitor por partição (eventos em monitor-processador.txt).
 */
public class AlarmProcessor {
    private static final Object LOCK = new Object();
    private static long lp = 0;                                  // Lp
    private static VectorClock vp;                               // Vp
    private static final Set<String> VISTOS = new HashSet<>();   // "k#seq" já tratados

    private static Partitioner partitioner;
    private static IpfsPubSub ipfs;
    private static MonitorLog log;
    private static HeartbeatMonitor[] monitores;
    private static int n;

    public static void main(String[] args) {
        n = args.length > 0 ? Integer.parseInt(args[0]) : 3;
        String api = args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0";

        vp = new VectorClock(n);
        partitioner = new Partitioner(n);
        ipfs = new IpfsPubSub(api);
        log = new MonitorLog("processador");
        monitores = new HeartbeatMonitor[n];

        System.out.println("Processador de alarmes pronto (N=" + n + ", limiar=" + Wire.LIMIAR_ALARME
                + ", periodo=" + Wire.PERIODO_HB_MS + " ms, fator=" + Wire.FATOR_TIMEOUT + ")");

        for (int k = 0; k < n; k++) {
            monitores[k] = new HeartbeatMonitor(k, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, log::log);
            final int kk = k;
            new Thread(() -> escutar(kk), "Processador-p" + k).start();
        }

        // Thread de verificação do detetor
        ScheduledExecutorService verificacao = Executors.newSingleThreadScheduledExecutor();
        verificacao.scheduleAtFixedRate(() -> {
            try {
                for (HeartbeatMonitor m : monitores) m.check();
            } catch (Throwable t) {
                System.out.println("Erro na verificação: " + t);
            }
        }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);
    }

    private static void escutar(int k) {
        try {
            System.out.println("[p" + k + "] À escuta no tópico " + Wire.topicData(k));
            ipfs.subscribe(Wire.topicData(k), msg -> {
                try {
                    Batch b = Batch.decode(msg, n);
                    monitores[k].onHeartbeat(b.inc, b.hb);
                    log.log("RECEBIDO;p" + k + ";inc=" + b.inc + ";hb=" + b.hb + ";ultimoSeq=" + b.lastSeq
                            + ";n=" + b.records.size() + ";seq=" + b.seqRange());
                    // Os registos de um lote de incarnação antiga NÃO são descartados: o detetor ignora o
                    // heartbeat, mas os dados continuam válidos (os duplicados são filtrados por k#seq).
                    for (SensorRecord r : b.records) {
                        tratar(k, r);
                    }
                } catch (Exception e) {
                    System.out.println("[p" + k + "] REJEITADO: " + e.getMessage());
                }
            });
        } catch (Exception e) {
            System.out.println("[p" + k + "] Erro na subscrição: " + e.getMessage());
        }
    }

    // Trata um registo da partição k com as regras da COMINDIR03
    static void tratar(int k, SensorRecord r) throws Exception {
        if (r.getSensorId().startsWith("ALARM-")) {
            return;                                   // evita alarmes sobre alarmes
        }

        long lSnap;
        VectorClock vSnap;
        synchronized (LOCK) {
            if (!VISTOS.add(k + "#" + r.getSeq())) {
                return;                               // duplicado (mesma partição e mesmo seq)
            }
            // O processador regista o máximo de TODOS os registos recebidos
            lp = Math.max(lp, r.getLamport());
            vp.merge(r.getVector());
            if (r.getTemperature() < Wire.LIMIAR_ALARME) {
                return;
            }
            lSnap = lp;
            vSnap = vp.copy();                        // cópia: outra thread pode alterar vp depois
        }

        String sensorAlarme = "ALARM-" + r.getSensorId();
        int j = partitioner.of(sensorAlarme);

        // Se o líder de destino é suspeito, o alarme pode perder-se: avisa, mas publica na mesma
        if (monitores[j].state() == HeartbeatMonitor.State.SUSPEITO) {
            System.out.println("AVISO: líder p" + j + " suspeito; alarme " + sensorAlarme + " pode perder-se");
            log.log("AVISO;p" + j + ";lider suspeito, alarme " + sensorAlarme + " pode perder-se");
        }

        ipfs.publish(Wire.topicIn(j), Wire.in(sensorAlarme, r.getTemperature(), lSnap, vSnap));
        System.out.println("ALARME: " + r.getSensorId() + " p" + k + "#" + r.getSeq() + " → p" + j);
    }
}
