package raft04;

import java.nio.file.*;
import java.util.*;

public class CausalCheck {
    public static void main(String[] args) throws Exception {
        // Exemplo simples: Verifica se o vetor V[k] é igual ao seq
        for (int k = 0; k < 3; k++) {
            Path p = Paths.get("no-p" + k + "-n1.txt");
            if (!Files.exists(p)) continue;
            for (String line : Files.readAllLines(p)) {
                if (line.isBlank()) continue;
                SensorRecord r = SensorRecord.fromLine(line, 3);
                if (r.getVector().get(k) != r.getSeq()) {
                    System.out.println("❌ Violação causal em p" + k + " seq=" + r.getSeq());
                    return;
                }
            }
        }
        System.out.println("✅ Causalidade: V[k] == seq mantém-se.");
    }
}