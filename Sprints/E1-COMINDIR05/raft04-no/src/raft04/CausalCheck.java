package raft04;

import java.nio.file.*;
import java.util.*;

/**
 * Verificador de causalidade (CausalCheck <N> <pasta>):
 * confirma nos ficheiros no-p<k>-n<i>.txt que V[k] == seq e que o relógio de Lamport cresce com seq.
 */
public class CausalCheck {
    public static void main(String[] args) throws Exception {
        int N = args.length > 0 ? Integer.parseInt(args[0]) : 3;
        Path dir = args.length > 1 ? Paths.get(args[1]) : Paths.get(".");

        System.out.println("=== CAUSAL CHECK ===");
        boolean ok = true;

        // Percorre os ficheiros finais no-p<k>-n<i>.txt de todas as partições e nós
        for (int k = 0; k < N; k++) {
            for (int i = 1; i <= 3; i++) {
                Path p = dir.resolve("no-p" + k + "-n" + i + ".txt");
                if (!Files.exists(p)) continue;
                // Carrega todos os registos do ficheiro do nó para memória
                List<SensorRecord> recs = new ArrayList<>();
                for (String line : Files.readAllLines(p)) {
                    if (!line.isBlank()) recs.add(SensorRecord.fromLine(line, N));
                }

                // Condição V[k] == seq: garante que após cada eleição o líder retomou o vetor do último registo
                for (SensorRecord r : recs) {
                    if (r.getVector().get(k) != r.getSeq()) {
                        System.out.println("❌ V[" + k + "] != seq em p" + k + " n" + i + " seq=" + r.getSeq());
                        ok = false;
                    }
                }

                // Condição de Lamport: se seq_a < seq_b na mesma partição, L(a) tem de ser estritamente menor que L(b)
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