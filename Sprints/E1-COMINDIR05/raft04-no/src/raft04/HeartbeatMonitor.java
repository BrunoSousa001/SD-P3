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
    private final int partition;

    public HeartbeatMonitor(String id, int partition) {
        this.id = id;
        this.partition = partition;
    }

    public synchronized void onHeartbeat(long term, int leader, long hb) {
        long now = System.currentTimeMillis();
        if (term < lastTerm) return;
        if (term > lastTerm) {
            if (lastTerm != -1) log("NOVO_LIDER", "p" + partition + ";termo=" + term + ";lider=n" + leader);
            lastTerm = term; lastHb = hb; lastTime = now;
            if (state.equals("SUSPEITO")) { state = "OK"; log("RETOMA", "p" + partition + ";lider=n" + leader); }
        } else if (hb > lastHb) {
            if (hb > lastHb + 1) log("HB_PERDIDO", "p" + partition + ";esperado=" + (lastHb+1) + ";recebido=" + hb);
            lastHb = hb; lastTime = now;
            if (state.equals("SUSPEITO")) { state = "OK"; log("RETOMA", "p" + partition + ";lider=n" + leader); }
        }
    }

    public synchronized void checkTimeout(long now) {
        if (!state.equals("SUSPEITO") && lastTerm != -1 && (now - lastTime > Wire.PERIODO_HB_MS * Wire.FATOR_TIMEOUT)) {
            state = "SUSPEITO";
            log("SUSPEITA", "p" + partition);
        }
    }

    private void log(String evento, String detalhe) {
        String linha = System.currentTimeMillis() + ";" + evento + ";" + detalhe;
        try {
            Files.writeString(Paths.get("monitor-" + id + ".txt"), linha + "\n", StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException ignored) {}
    }

    public synchronized boolean isSuspicious() { return state.equals("SUSPEITO"); }

    public synchronized void logAviso(String detalhe) {
        log("AVISO", "p" + partition + ";" + detalhe);
    }
}