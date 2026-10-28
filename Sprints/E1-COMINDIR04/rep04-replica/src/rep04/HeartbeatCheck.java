package rep04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Uso: HeartbeatCheck <N> <pasta> [falhas.txt]
 *
 * Lê hb-p*.txt, lider-p*.txt e monitor-*.txt de uma pasta e escreve o relatório do detetor de falhas.
 * Os instantes de processos diferentes só são comparáveis porque todos correm na mesma máquina.
 */
public class HeartbeatCheck {

    // linha de hb-p<k>.txt: inc;hb;instante;ultimoSeq;n;bytes
    private static final class Hb {
        long inc, hb, instante, ultimoSeq, n, bytes;
    }

    // linha de monitor-<id>.txt: instante;evento;p<k>;detalhe
    private static final class Ev {
        String participante;
        long t;
        String evento;
        int k;
        Map<String, String> det = new HashMap<>();
        long num(String chave, long omissao) {
            String v = det.get(chave);
            if (v == null) return omissao;
            try { return Long.parseLong(v); } catch (NumberFormatException e) { return omissao; }
        }
    }

    private static final StringBuilder REL = new StringBuilder();

    private static void out(String s) {
        REL.append(s).append('\n');
    }

    private static String f(double v) {
        return String.format(Locale.ROOT, "%.1f", v);
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Uso: HeartbeatCheck <N> <pasta> [falhas.txt]");
            return;
        }
        int n = Integer.parseInt(args[0]);
        Path dir = Paths.get(args[1]);
        Path falhasPath = args.length > 2 ? Paths.get(args[2]) : null;

        // --- carregamento ---
        TreeMap<Integer, List<Hb>> hbs = new TreeMap<>();
        TreeMap<Integer, List<SensorRecord>> registos = new TreeMap<>();
        TreeMap<String, List<Ev>> eventos = new TreeMap<>();

        Pattern pHb = Pattern.compile("hb-p(\\d+)\\.txt");
        Pattern pLider = Pattern.compile("lider-p(\\d+)\\.txt");
        Pattern pMon = Pattern.compile("monitor-(.+)\\.txt");
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir)) {
            for (Path p : ds) {
                String nome = p.getFileName().toString();
                Matcher m;
                if ((m = pHb.matcher(nome)).matches()) {
                    List<Hb> l = new ArrayList<>();
                    for (String line : Files.readAllLines(p, StandardCharsets.UTF_8)) {
                        if (line.isBlank()) continue;
                        String[] x = line.trim().split(";");
                        Hb h = new Hb();
                        h.inc = Long.parseLong(x[0]); h.hb = Long.parseLong(x[1]); h.instante = Long.parseLong(x[2]);
                        h.ultimoSeq = Long.parseLong(x[3]); h.n = Long.parseLong(x[4]); h.bytes = Long.parseLong(x[5]);
                        l.add(h);
                    }
                    hbs.put(Integer.parseInt(m.group(1)), l);
                } else if ((m = pLider.matcher(nome)).matches()) {
                    List<SensorRecord> l = new ArrayList<>();
                    for (String line : Files.readAllLines(p, StandardCharsets.UTF_8)) {
                        if (!line.isBlank()) l.add(SensorRecord.fromLine(line, n));
                    }
                    registos.put(Integer.parseInt(m.group(1)), l);
                } else if ((m = pMon.matcher(nome)).matches()) {
                    List<Ev> l = new ArrayList<>();
                    for (String line : Files.readAllLines(p, StandardCharsets.UTF_8)) {
                        if (line.isBlank()) continue;
                        String[] x = line.trim().split(";");
                        if (x.length < 3) continue;
                        Ev e = new Ev();
                        e.participante = m.group(1);
                        e.t = Long.parseLong(x[0]);
                        e.evento = x[1];
                        e.k = Integer.parseInt(x[2].substring(1));
                        for (int i = 3; i < x.length; i++) {
                            int eq = x[i].indexOf('=');
                            if (eq > 0) e.det.put(x[i].substring(0, eq), x[i].substring(eq + 1));
                            else e.det.put("txt", x[i]);
                        }
                        l.add(e);
                    }
                    l.sort(Comparator.comparingLong(e -> e.t));
                    eventos.put(m.group(1), l);
                }
            }
        }
        for (List<Hb> l : hbs.values()) l.sort(Comparator.<Hb>comparingLong(h -> h.inc).thenComparingLong(h -> h.hb));

        out("=== HeartbeatCheck (N=" + n + ", pasta=" + dir + ") ===");

        // --- 1. Heartbeats por partição e incarnação ---
        out("\n## 1. Lotes por partição e incarnação");
        for (Map.Entry<Integer, List<Hb>> e : hbs.entrySet()) {
            int k = e.getKey();
            TreeMap<Long, List<Hb>> porInc = new TreeMap<>();
            for (Hb h : e.getValue()) porInc.computeIfAbsent(h.inc, x -> new ArrayList<>()).add(h);
            long totalLotes = 0, totalVazios = 0, totalRegs = 0, maxRegs = 0;
            double somaInt = 0; long maxInt = 0, nInt = 0;
            for (Map.Entry<Long, List<Hb>> ei : porInc.entrySet()) {
                List<Hb> l = ei.getValue();
                long vazios = 0, regs = 0, mx = 0;
                double somaI = 0; long mxI = 0, nI = 0;
                for (int i = 0; i < l.size(); i++) {
                    Hb h = l.get(i);
                    if (h.n == 0) vazios++;
                    regs += h.n; mx = Math.max(mx, h.n);
                    if (i > 0) {
                        long d = h.instante - l.get(i - 1).instante;
                        somaI += d; mxI = Math.max(mxI, d); nI++;
                    }
                }
                out(String.format(Locale.ROOT, "p%d inc=%d: %d lotes, %d vazios (%s%%), registos/lote média=%s máx=%d, intervalo entre heartbeats média=%s ms máx=%d ms",
                        k, ei.getKey(), l.size(), vazios, f(100.0 * vazios / l.size()), f((double) regs / l.size()), mx,
                        nI > 0 ? f(somaI / nI) : "-", mxI));
                totalLotes += l.size(); totalVazios += vazios; totalRegs += regs; maxRegs = Math.max(maxRegs, mx);
                somaInt += somaI; maxInt = Math.max(maxInt, mxI); nInt += nI;
            }
            out(String.format(Locale.ROOT, "p%d TOTAL: %d lotes, %d vazios (%s%%), registos/lote média=%s máx=%d, intervalo média=%s ms máx=%d ms",
                    k, totalLotes, totalVazios, f(100.0 * totalVazios / Math.max(1, totalLotes)),
                    f((double) totalRegs / Math.max(1, totalLotes)), maxRegs, nInt > 0 ? f(somaInt / nInt) : "-", maxInt));
        }

        // --- 2. Suspeitas por participante e partição ---
        out("\n## 2. Suspeitas (falha real = sem novo heartbeat da mesma incarnação; falsa = seguida de RETOMA com o mesmo inc)");
        long totalReais = 0, totalFalsas = 0;
        // para o ponto 3: suspeitas reais por participante e partição, com instante
        Map<String, List<long[]>> reaisPorPartPart = new TreeMap<>();   // "participante|k" -> [instante]
        for (Map.Entry<String, List<Ev>> e : eventos.entrySet()) {
            String part = e.getKey();
            Map<Integer, Long> incAtual = new HashMap<>();
            List<Ev> evs = e.getValue();
            for (int i = 0; i < evs.size(); i++) {
                Ev ev = evs.get(i);
                if (ev.evento.equals("RECEBIDO")) {
                    incAtual.put(ev.k, ev.num("inc", -1));
                } else if (ev.evento.equals("SUSPEITA")) {
                    long incSusp = incAtual.getOrDefault(ev.k, -1L);
                    boolean falsa = false;
                    for (int j = i + 1; j < evs.size(); j++) {
                        Ev r = evs.get(j);
                        if (r.evento.equals("RETOMA") && r.k == ev.k) {
                            falsa = r.num("inc", -2) == incSusp;
                            break;
                        }
                    }
                    // silêncio desde o último heartbeat PUBLICADO antes da suspeita
                    long ultimoHb = -1;
                    for (Hb h : hbs.getOrDefault(ev.k, Collections.emptyList())) {
                        if (h.instante <= ev.t) ultimoHb = Math.max(ultimoHb, h.instante);
                    }
                    out(String.format(Locale.ROOT, "%s p%d t=%d: %s (silêncio medido pelo detetor=%d ms; desde o último heartbeat publicado=%s ms)",
                            part, ev.k, ev.t, falsa ? "FALSA SUSPEITA" : "FALHA REAL", ev.num("silencio_ms", -1),
                            ultimoHb >= 0 ? String.valueOf(ev.t - ultimoHb) : "?"));
                    if (falsa) totalFalsas++;
                    else {
                        totalReais++;
                        reaisPorPartPart.computeIfAbsent(part + "|" + ev.k, x -> new ArrayList<>()).add(new long[] { ev.t });
                    }
                }
            }
        }
        out("Total: " + totalReais + " suspeitas de falha real, " + totalFalsas + " falsas suspeitas");

        // --- 3. Tempo de deteção (se falhas.txt) ---
        if (falhasPath != null) {
            out("\n## 3. Tempo de deteção de cada falha provocada (" + falhasPath.getFileName() + ")");
            List<long[]> falhas = new ArrayList<>();   // {k, instante}
            for (String line : Files.readAllLines(falhasPath, StandardCharsets.UTF_8)) {
                if (line.isBlank()) continue;
                String[] x = line.trim().split(";");
                falhas.add(new long[] { Integer.parseInt(x[0].trim().substring(1)), Long.parseLong(x[1].trim()) });
            }
            Map<Integer, List<Double>> tempos = new TreeMap<>();
            for (long[] fa : falhas) {
                int k = (int) fa[0];
                long tf = fa[1];
                for (Map.Entry<String, List<long[]>> e : reaisPorPartPart.entrySet()) {
                    String[] pk = e.getKey().split("\\|");
                    if (Integer.parseInt(pk[1]) != k) continue;
                    long melhor = Long.MAX_VALUE;
                    for (long[] s : e.getValue()) if (s[0] >= tf && s[0] < melhor) melhor = s[0];
                    if (melhor != Long.MAX_VALUE) {
                        double seg = (melhor - tf) / 1000.0;
                        out(String.format(Locale.ROOT, "falha p%d em t=%d: %s suspeitou após %.2f s", k, tf, pk[0], seg));
                        tempos.computeIfAbsent(k, x -> new ArrayList<>()).add(seg);
                    } else {
                        out("falha p" + k + " em t=" + tf + ": " + pk[0] + " não suspeitou");
                    }
                }
            }
            for (Map.Entry<Integer, List<Double>> e : tempos.entrySet()) {
                double mn = Collections.min(e.getValue()), mx = Collections.max(e.getValue());
                double md = e.getValue().stream().mapToDouble(Double::doubleValue).average().orElse(0);
                out(String.format(Locale.ROOT, "p%d: tempo de deteção mínimo=%.2f s médio=%.2f s máximo=%.2f s (%d medições)",
                        e.getKey(), mn, md, mx, e.getValue().size()));
            }
            long timeout = Wire.PERIODO_HB_MS * Wire.FATOR_TIMEOUT;
            out(String.format(Locale.ROOT, "Intervalo previsto (]TIMEOUT-Δ, TIMEOUT+VERIFICACAO_MS]) com os parâmetros atuais: ]%.1f s, %.1f s]",
                    (timeout - Wire.PERIODO_HB_MS) / 1000.0, (timeout + Wire.VERIFICACAO_MS) / 1000.0));
        }

        // --- 4. Latência de lote ---
        out("\n## 4. Latência de lote (instante do RECEBIDO que contém o registo menos o instante do registo)");
        for (Map.Entry<Integer, List<SensorRecord>> e : registos.entrySet()) {
            int k = e.getKey();
            double soma = 0; long max = 0, cnt = 0, semLote = 0;
            for (Map.Entry<String, List<Ev>> pe : eventos.entrySet()) {
                // o produtor só vê cabeçalhos: não indica o intervalo de seq
                Map<Long, Long> recebidoEm = new HashMap<>();
                boolean temIntervalos = false;
                for (Ev ev : pe.getValue()) {
                    if (!ev.evento.equals("RECEBIDO") || ev.k != k) continue;
                    String seq = ev.det.get("seq");
                    if (seq == null || seq.equals("-")) continue;
                    temIntervalos = true;
                    String[] ab = seq.split("\\.\\.");
                    long a = Long.parseLong(ab[0]), b = Long.parseLong(ab[1]);
                    for (long s = a; s <= b; s++) recebidoEm.putIfAbsent(s, ev.t);
                }
                if (!temIntervalos) continue;
                double somaP = 0; long maxP = 0, cntP = 0;
                for (SensorRecord r : e.getValue()) {
                    Long t = recebidoEm.get(r.getSeq());
                    if (t == null) { semLote++; continue; }
                    long lat = t - r.getTimestamp();
                    somaP += lat; maxP = Math.max(maxP, lat); cntP++;
                }
                if (cntP > 0) out(String.format(Locale.ROOT, "p%d, %s: %d registos, latência média=%s ms máx=%d ms", k, pe.getKey(), cntP, f(somaP / cntP), maxP));
                soma += somaP; max = Math.max(max, maxP); cnt += cntP;
            }
            if (cnt > 0) out(String.format(Locale.ROOT, "p%d TOTAL: %d pares (registo, participante), latência média=%s ms máx=%d ms (registos sem RECEBIDO que os contenha, p.ex. recuperados por TCP: %d)",
                    k, cnt, f(soma / cnt), max, semLote));
            else out("p" + k + ": sem dados de latência");
        }

        // --- 5. HB_PERDIDO e RECUPERACAO ---
        out("\n## 5. Eventos HB_PERDIDO, REINICIO e RECUPERACAO");
        for (Map.Entry<String, List<Ev>> e : eventos.entrySet()) {
            long perd = 0, rec = 0, rein = 0;
            for (Ev ev : e.getValue()) {
                if (ev.evento.equals("HB_PERDIDO")) perd++;
                else if (ev.evento.equals("RECUPERACAO")) rec++;
                else if (ev.evento.equals("REINICIO")) rein++;
            }
            out(e.getKey() + ": HB_PERDIDO=" + perd + " REINICIO=" + rein + " RECUPERACAO=" + rec);
        }

        // --- 6. Bytes por registo e mensagens ---
        out("\n## 6. Custo em bytes e número de mensagens no tópico de dados");
        long msgsRegTotal = 0, msgsLoteTotal = 0, msgsLoteNaoVazios = 0;
        double bytesLoteSoma = 0, bytesRegSoma = 0; long registosLote = 0, registosReg = 0;
        for (Map.Entry<Integer, List<Hb>> e : hbs.entrySet()) {
            for (Hb h : e.getValue()) {
                msgsLoteTotal++;
                if (h.n > 0) { bytesLoteSoma += h.bytes; registosLote += h.n; msgsLoteNaoVazios++; }
            }
        }
        for (Map.Entry<Integer, List<SensorRecord>> e : registos.entrySet()) {
            for (SensorRecord r : e.getValue()) {
                // REG equivalente da COMINDIR03: REG;<endereço>;<porto>;<linha do registo>
                String reg = "REG;127.0.0.1;" + (Wire.PORTO_BASE + e.getKey()) + ";" + r.toLine();
                bytesRegSoma += reg.getBytes(StandardCharsets.UTF_8).length;
                registosReg++;
            }
        }
        msgsRegTotal = registosReg;
        out("Mensagens no tópico de dados: COMINDIR03 (um REG por registo) = " + msgsRegTotal
                + "; COMINDIR04 (LOTE) = " + msgsLoteTotal + " (" + msgsLoteNaoVazios + " com registos, " + (msgsLoteTotal - msgsLoteNaoVazios) + " vazios)");
        if (registosLote > 0) out(String.format(Locale.ROOT, "Bytes por registo transportado em LOTE (incluindo a quota do cabeçalho; lotes não vazios) = %s", f(bytesLoteSoma / registosLote)));
        if (registosReg > 0) out(String.format(Locale.ROOT, "Bytes por registo em REG individual (COMINDIR03, endereço 127.0.0.1) = %s", f(bytesRegSoma / registosReg)));

        System.out.print(REL);
        Files.writeString(dir.resolve("relatorio-heartbeat.txt"), REL.toString(), StandardCharsets.UTF_8);
    }
}
