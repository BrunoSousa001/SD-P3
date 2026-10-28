package rep04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Uso: CausalCheck <N> <pasta>
 * Lê os ficheiros lider-p*.txt da pasta e escreve um relatório sobre os relógios de Lamport e vetoriais.
 */
public class CausalCheck {
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Uso: CausalCheck <N> <pasta>");
            return;
        }
        int n = Integer.parseInt(args[0]);
        Path dir = Paths.get(args[1]);

        // 1. Carrega os registos de cada partição
        Pattern p = Pattern.compile("lider-p(\\d+)\\.txt");
        TreeMap<Integer, List<SensorRecord>> porParticao = new TreeMap<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, "lider-p*.txt")) {
            for (Path f : ds) {
                Matcher m = p.matcher(f.getFileName().toString());
                if (!m.matches()) continue;
                int k = Integer.parseInt(m.group(1));
                List<SensorRecord> lista = new ArrayList<>();
                for (String line : Files.readAllLines(f, StandardCharsets.UTF_8)) {
                    if (!line.isBlank()) lista.add(SensorRecord.fromLine(line, n));
                }
                porParticao.put(k, lista);
            }
        }

        StringBuilder rel = new StringBuilder();
        rel.append("=== CausalCheck (N=").append(n).append(", pasta=").append(dir).append(") ===\n");

        // 2. V[k] == seq em todas as linhas
        long violacoesV = 0, totalRegistos = 0;
        for (Map.Entry<Integer, List<SensorRecord>> e : porParticao.entrySet()) {
            int k = e.getKey();
            long viol = 0;
            for (SensorRecord r : e.getValue()) {
                if (r.getVector().get(k) != r.getSeq()) {
                    viol++;
                    if (viol <= 5) {
                        rel.append("  VIOLAÇÃO V[k]!=seq: p").append(k).append(" seq=").append(r.getSeq())
                           .append(" V=").append(r.getVector()).append('\n');
                    }
                }
            }
            rel.append("p").append(k).append(": ").append(e.getValue().size()).append(" registos, V[k]==seq violado em ")
               .append(viol).append(" linhas\n");
            violacoesV += viol;
            totalRegistos += e.getValue().size();
        }
        rel.append("V[k] == seq em todas as linhas: ").append(violacoesV == 0 ? "SIM" : "NÃO (" + violacoesV + " violações)").append('\n');

        // 3. Classificação dos pares de partições diferentes
        long aAntesB = 0, bAntesA = 0, concorrentes = 0, concorrentesLDistinto = 0, violacoesLamport = 0;
        List<Integer> ks = new ArrayList<>(porParticao.keySet());
        for (int i = 0; i < ks.size(); i++) {
            for (int j = i + 1; j < ks.size(); j++) {
                for (SensorRecord a : porParticao.get(ks.get(i))) {
                    for (SensorRecord b : porParticao.get(ks.get(j))) {
                        VectorClock.Order o = a.getVector().compare(b.getVector());
                        if (o == VectorClock.Order.BEFORE) {
                            aAntesB++;
                            if (!(a.getLamport() < b.getLamport())) violacoesLamport++;
                        } else if (o == VectorClock.Order.AFTER) {
                            bAntesA++;
                            if (!(b.getLamport() < a.getLamport())) violacoesLamport++;
                        } else {
                            concorrentes++;
                            if (a.getLamport() != b.getLamport()) concorrentesLDistinto++;
                        }
                    }
                }
            }
        }
        long causais = aAntesB + bAntesA;
        long pares = causais + concorrentes;
        rel.append("\nPares de registos de partições diferentes: ").append(pares).append('\n');
        rel.append("  a -> b (partição menor antes): ").append(aAntesB).append('\n');
        rel.append("  b -> a (partição maior antes): ").append(bAntesA).append('\n');
        rel.append("  causalmente relacionados: ").append(causais).append('\n');
        rel.append("  concorrentes: ").append(concorrentes).append('\n');
        rel.append("Condição de relógio de Lamport (a -> b implica L(a) < L(b)): ")
           .append(violacoesLamport == 0 ? "VÁLIDA" : "VIOLADA em " + violacoesLamport + " pares").append('\n');
        rel.append("Pares concorrentes com L distinto: ").append(concorrentesLDistinto);
        if (concorrentes > 0) {
            rel.append(String.format(Locale.ROOT, " (%.2f%% dos concorrentes)", 100.0 * concorrentesLDistinto / concorrentes));
        }
        rel.append('\n');

        // 4. Vista de Lamport: todos os registos ordenados por (L, k)
        List<Object[]> todos = new ArrayList<>();
        for (Map.Entry<Integer, List<SensorRecord>> e : porParticao.entrySet()) {
            for (SensorRecord r : e.getValue()) todos.add(new Object[] { e.getKey(), r });
        }
        todos.sort((x, y) -> {
            int c = Long.compare(((SensorRecord) x[1]).getLamport(), ((SensorRecord) y[1]).getLamport());
            return c != 0 ? c : Integer.compare((Integer) x[0], (Integer) y[0]);
        });
        StringBuilder vista = new StringBuilder();
        for (Object[] o : todos) {
            vista.append("L=").append(((SensorRecord) o[1]).getLamport()).append(";p").append(o[0]).append(';')
                 .append(((SensorRecord) o[1]).toLine()).append('\n');
        }
        Files.writeString(dir.resolve("vista-lamport.txt"), vista.toString(), StandardCharsets.UTF_8);
        rel.append("Vista escrita em ").append(dir.resolve("vista-lamport.txt")).append(" (").append(totalRegistos).append(" registos)\n");

        System.out.print(rel);
        Files.writeString(dir.resolve("relatorio-causal.txt"), rel.toString(), StandardCharsets.UTF_8);
    }
}
