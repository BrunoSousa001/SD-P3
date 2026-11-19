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

    // Processa o cabeçalho de um LOTE (termo, líder, número de heartbeat) para detetar falhas e mudanças de líder
    public synchronized void onHeartbeat(long term, int leader, long hb) {
        long now = System.currentTimeMillis();
        // Lote de termo inferior ao já observado: vem de um líder obsoleto, ignora
        if (term < lastTerm) return;

        // Termo aumentou: houve eleição de novo líder; reinicia contagem de hb e sai de SUSPEITO se necessário
        if (term > lastTerm) {
            if (lastTerm != -1) {
                log("NOVO_LIDER", "p" + partition + ";termo=" + term + ";lider=n" + leader);
            }
            lastTerm = term;
            lastHb = hb;
            lastTime = now;
            if (state.equals("SUSPEITO")) {
                state = "OK";
                log("RETOMA", "p" + partition + ";lider=n" + leader);
            }
        // Mesmo termo: só aceita hb crescente; se houver salto (hb > lastHb + 1) perdeu-se um lote pelo meio
        } else if (hb > lastHb) {
            if (hb > lastHb + 1) {
                log("HB_PERDIDO", "p" + partition + ";esperado=" + (lastHb+1) + ";recebido=" + hb);
            }
            lastHb = hb;
            lastTime = now;
            if (state.equals("SUSPEITO")) {
                state = "OK";
                log("RETOMA", "p" + partition + ";lider=n" + leader);
            }
        }
    }

    // Marca a partição como SUSPEITA se já teve líder e passaram mais de FATOR_TIMEOUT * PERIODO_HB_MS sem heartbeats
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

    public synchronized boolean isSuspicious() {
        return state.equals("SUSPEITO");
    }

    public synchronized void logAviso(String detalhe) {
        log("AVISO", "p" + partition + ";" + detalhe);
    }
}