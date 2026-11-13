package raft04;

import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

public class RaftCheck {
    public static void main(String[] args) throws Exception {
        int N = 3;
        Path dir = Paths.get(".");

        // 1. Segurança de Eleição (Nenhum termo com >1 líder)
        Map<String, Integer> lideres = new HashMap<>();
        for (int k = 0; k < N; k++) {
            for (int i = 1; i <= 3; i++) {
                Path p = dir.resolve("eventos-p" + k + "-n" + i + ".txt");
                if (!Files.exists(p)) continue;
                for (String line : Files.readAllLines(p)) {
                    String[] f = line.split(";");
                    if (f[1].equals("LIDER")) {
                        lideres.put("p" + k + "-" + f[3], lideres.getOrDefault("p" + k + "-" + f[3], 0) + 1);
                    }
                }
            }
        }
        boolean safe = true;
        for (var e : lideres.entrySet()) {
            if (e.getValue() > 1) {
                System.out.println("❌ Múltiplos líderes no termo: " + e.getKey());
                safe = false;
            }
        }
        if (safe) System.out.println("✅ Segurança: 0 termos com múltiplos líderes.");

        // 2. Coincidência Final e Truncagens
        for (int k = 0; k < N; k++) {
            int truncados = 0;
            for (int i = 1; i <= 3; i++) {
                Path p = dir.resolve("eventos-p" + k + "-n" + i + ".txt");
                if (Files.exists(p)) {
                    for (String l : Files.readAllLines(p)) {
                        if (l.contains("TRUNCADO")) truncados++;
                    }
                }
            }
            System.out.println("Partição p" + k + ": " + truncados + " eventos TRUNCADO.");
        }
    }
}