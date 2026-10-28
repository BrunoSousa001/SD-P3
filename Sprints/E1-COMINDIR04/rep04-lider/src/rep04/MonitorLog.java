package rep04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;

/**
 * Escreve os eventos do detetor na consola e em monitor-<id>.txt, precedidos de System.currentTimeMillis().
 * Formato da linha: instante;evento;p<k>;detalhe
 */
public final class MonitorLog {
    private final String id;
    private final Path path;

    public MonitorLog(String id) {
        this.id = id;
        this.path = Paths.get("monitor-" + id + ".txt");
    }

    // vários threads (uma por partição, mais a de verificação) escrevem no mesmo ficheiro
    public synchronized void log(String evento) {
        String linha = System.currentTimeMillis() + ";" + evento;
        System.out.println("[monitor-" + id + "] " + linha);
        try {
            Files.writeString(path, linha + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.out.println("Erro a escrever " + path + ": " + e.getMessage());
        }
    }
}
