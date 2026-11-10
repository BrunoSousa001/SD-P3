package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;

public class HeartbeatMonitor {
    private long lastTerm = -1;
    private long lastHb = -1;
    private long lastTime = 0;
    private String state = "OK";
    private final String id;

    public HeartbeatMonitor(String id) {
        this.id = id;
    }

    public synchronized void onHeartbeat(long term, int leader, long hb) {
        long now = System.currentTimeMillis();
        if (term < lastTerm) return; // Ignora termos antigos

        if (term > lastTerm) {
            if (lastTerm != -1) {
                log("NOVO_LIDER", "termo=" + term + ";lider=n" + leader);
            }
            lastTerm = term;
            lastHb = hb;
            lastTime = now;
            state = "OK";
        } else if (hb > lastHb) {
            if (hb > lastHb + 1) {
                log("HB_PERDIDO", "termo=" + term + ";esperado=" + (lastHb + 1) + ";recebido=" + hb);
            }
            lastHb = hb;
            lastTime = now;
            state = "OK";
        }
    }

    public synchronized void checkTimeout(long now) {
        if (state.equals("OK") && lastTerm != -1 && (now - lastTime > Wire.PERIODO_HB_MS * Wire.FATOR_TIMEOUT)) {
            state = "SUSPEITO";
        }
    }

    private void log(String evento, String detalhe) {
        String linha = System.currentTimeMillis() + ";" + evento + ";" + detalhe;
        try {
            Files.writeString(Paths.get("monitor-" + id + ".txt"), linha + "\n", StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException ignored) {}
    }

    public synchronized boolean isSuspicious() { return state.equals("SUSPEITO"); }
    public synchronized long getLastTerm() { return lastTerm; }
}