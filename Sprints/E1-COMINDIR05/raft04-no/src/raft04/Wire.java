package raft04;

import java.nio.file.Files;
import java.nio.file.Paths;

public final class Wire {
    public static final int LIMITE_RETIDOS = 10;
    public static final int T_TCP_MS = 5000;
    public static final double LIMIAR_ALARME = 28.0;

    // Parâmetros atualizados (prefixo raft04)
    public static final long PERIODO_HB_MS = Long.getLong("raft04.periodo", 2000L);
    public static final int FATOR_TIMEOUT = Integer.getInteger("raft04.fator", 3);
    public static final long VERIFICACAO_MS = 200L;

    // Novos parâmetros de eleição
    public static final long ELEICAO_MIN_MS = Long.getLong("raft04.eleicaoMin", 3 * PERIODO_HB_MS);
    public static final long ELEICAO_MAX_MS = Long.getLong("raft04.eleicaoMax", 6 * PERIODO_HB_MS);
    public static final int GRUPO = 3;

    private Wire() { }

    // Tópicos IPFS (com prefixo raft04)
    public static String topicIn(int k)   { return "raft04-in-p" + k; }
    public static String topicData(int k) { return "raft04-p" + k; }
    public static String topicElection(int k) { return "raft04-eleicao-p" + k; } // NOVO

    // NOVO: Porto de recuperação TCP para o nó i da partição k
    public static int nodePort(int k, int i) {
        return 7000 + (k * 10) + i;
    }

    // NOVO: Simula a partição de rede
    public static boolean isolated(int k, int i) {
        return Files.exists(Paths.get("isolar-p" + k + "-n" + i));
    }

    public static String in(String sensor, double temp, long lamport, VectorClock v) {
        return "IN;" + sensor + ";" + temp + ";" + lamport + ";" + v;
    }

    public static String type(String msg) {
        String m = msg.trim();
        int i = m.indexOf(';');
        if (i <= 0) {
            throw new IllegalArgumentException("Mensagem sem tipo: " + msg);
        }
        return m.substring(0, i);
    }

    public static String body(String msg) {
        String m = msg.trim();
        return m.substring(m.indexOf(';') + 1);
    }
}