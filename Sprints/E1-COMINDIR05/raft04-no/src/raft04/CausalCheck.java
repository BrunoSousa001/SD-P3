package raft04;

import java.nio.file.*;
import java.util.*;

public class CausalCheck {
    public static void main(String[] args) throws Exception {
        int N = args.length > 0 ? Integer.parseInt(args[0]) : 3;
        Path dir = args.length > 1 ? Paths.get(args[1]) : Paths.get(".");

        System.out.println("=== CAUSAL CHECK ===");
        boolean ok = true;

        for (int k = 0; k < N; k++) {
            for (int i = 1; i <= 3; i++) {
                Path p = dir.resolve("no-p" + k + "-n" + i + ".txt");
                if (!Files.exists(p)) continue;
                List<SensorRecord> recs = new ArrayList<>();
                for (String line : Files.readAllLines(p)) {
                    if (!line.isBlank()) recs.add(SensorRecord.fromLine(line, N));
                }

                // Condição V[k] == seq
                for (SensorRecord r : recs) {
                    if (r.getVector().get(k) != r.getSeq()) {
                        System.out.println("❌ V[" + k + "] != seq em p" + k + " n" + i + " seq=" + r.getSeq());
                        ok = false;
                    }
                }

                // Condição de Lamport
                for (int a = 0; a < recs.size(); a++) {
                    for (int b = a + 1; b < recs.size(); b++) {
                        SensorRecord ra = recs.get(a), rb = recs.get(b);
                        if (ra.getSeq() < rb.getSeq() && ra.getLamport() >= rb.getLamport()) {
                            System.out.println("❌ Violação Lamport em p" + k + " n" + i);
                            ok = false;
                        }
                    }
                }
            }
        }
        if (ok) System.out.println("✅ Causalidade mantida (V[k]==seq e Lamport).");
    }
}