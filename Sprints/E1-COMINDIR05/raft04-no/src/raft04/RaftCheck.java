package raft04;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/**
 * Ferramenta de análise a posteriori (RaftCheck <N> <pasta> [falhas.txt]).
 * Reconstrói eleições, verifica segurança e monotonicidade de termos, mede indisponibilidades,
 * analisa eventos TRUNCADO, confirma coincidência final dos 3 nós e conta leituras perdidas.
 */
public class RaftCheck {
    public static void main(String[] args) throws Exception {
        int N = args.length > 0 ? Integer.parseInt(args[0]) : 3;
        Path dir = args.length > 1 ? Paths.get(args[1]) : Paths.get(".");
        Path falhasFile = args.length > 2 ? Paths.get(args[2]) : dir.resolve("falhas.txt");

        System.out.println("=== RELATÓRIO RAFT CHECK COMPLETO ===");

        Map<Integer, Map<Long, List<String>>> cands = new HashMap<>();
        Map<Integer, Map<Long, List<String>>> votos = new HashMap<>();
        Map<Integer, Map<Long, String>> lids = new HashMap<>();
        Map<Integer, Map<Long, Long>> tCand = new HashMap<>();
        Map<Integer, Map<Long, Long>> tLid = new HashMap<>();
        Map<Integer, Integer> ultimoLider = new HashMap<>();

        long[] primeiroArranquePart = new long[N];
        long[] primeiroLiderPart = new long[N];
        Arrays.fill(primeiroArranquePart, Long.MAX_VALUE);
        Arrays.fill(primeiroLiderPart, Long.MAX_VALUE);
        long primeiroArranqueGlobal = Long.MAX_VALUE;

        Map<String, Integer> lideresPorTermo = new HashMap<>();
        boolean monotono = true;

        // 1. Leitura dos eventos de cada nó para reconstruir eleições, segurança, monotonicidade e tempo até ao 1.º líder
        for (int k = 0; k < N; k++) {
            cands.put(k, new TreeMap<>());
            votos.put(k, new TreeMap<>());
            lids.put(k, new TreeMap<>());
            tCand.put(k, new HashMap<>());
            tLid.put(k, new HashMap<>());

            for (int i = 1; i <= 3; i++) {
                Path ev = dir.resolve("eventos-p" + k + "-n" + i + ".txt");
                if (!Files.exists(ev)) continue;

                long maxTermoNo = 0;
                for (String line : Files.readAllLines(ev, StandardCharsets.UTF_8)) {
                    if (line.isBlank()) continue;
                    String[] f = line.split(";", 4);
                    long t = Long.parseLong(f[0]);
                    String evType = f[1];
                    String det = f.length > 3 ? f[3] : "";

                    // Regista o primeiro instante em que algum nó da partição arrancou
                    if (evType.equals("ARRANQUE")) {
                        primeiroArranquePart[k] = Math.min(primeiroArranquePart[k], t);
                        primeiroArranqueGlobal = Math.min(primeiroArranqueGlobal, t);
                    // Nova candidatura: verifica se o termo não regrediu face ao máximo já visto neste nó
                    } else if (evType.equals("CANDIDATO")) {
                        long termo = Long.parseLong(det.split("=")[1].split(";")[0]);
                        if (termo < maxTermoNo) monotono = false;
                        maxTermoNo = Math.max(maxTermoNo, termo);
                        cands.get(k).computeIfAbsent(termo, x -> new ArrayList<>()).add("n" + i);
                        tCand.get(k).putIfAbsent(termo, t);
                    // Resposta a pedido de voto: se foi concedido, associa o voto ao candidato nesse termo
                    } else if (evType.equals("VOTO")) {
                        long termo = Long.parseLong(det.split("termo=")[1].split(";")[0]);
                        if (termo < maxTermoNo) monotono = false;
                        maxTermoNo = Math.max(maxTermoNo, termo);
                        if (det.contains("concedido")) {
                            String cand = det.split("candidato=")[1].split(";")[0];
                            votos.get(k).computeIfAbsent(termo, x -> new ArrayList<>()).add(cand);
                        }
                    // Eleição vencida: contabiliza líderes por termo (deve ser <= 1) e tempo até ao 1.º líder
                    } else if (evType.equals("LIDER")) {
                        long termo = Long.parseLong(det.split("termo=")[1].split(";")[0]);
                        if (termo < maxTermoNo) monotono = false;
                        maxTermoNo = Math.max(maxTermoNo, termo);
                        lids.get(k).put(termo, "n" + i);
                        tLid.get(k).putIfAbsent(termo, t);
                        ultimoLider.put(k, i);
                        lideresPorTermo.put("p" + k + "-term" + termo, lideresPorTermo.getOrDefault("p" + k + "-term" + termo, 0) + 1);
                        primeiroLiderPart[k] = Math.min(primeiroLiderPart[k], t);
                    // Mudança de termo: confirma que o termo novo é estritamente maior e nunca regrediu (nem entre reinícios)
                    } else if (evType.equals("TERMO")) {
                        String[] tr = det.split("->");
                        long oldT = Long.parseLong(tr[0]);
                        long newT = Long.parseLong(tr[1]);
                        if (newT <= oldT || oldT < maxTermoNo || newT < maxTermoNo) monotono = false;
                        maxTermoNo = Math.max(maxTermoNo, newT);
                    }
                }
            }
        }

        System.out.println("\n--- Histórico de Eleições ---");
        // Percorre os termos observados em cada partição para mostrar candidatos, votos e vencedor (ou voto dividido)
        for (int k = 0; k < N; k++) {
            Set<Long> todos = new TreeSet<>();
            todos.addAll(cands.get(k).keySet());
            todos.addAll(lids.get(k).keySet());
            for (Long t : todos) {
                String l = lids.get(k).get(t);
                List<String> c = cands.get(k).getOrDefault(t, Collections.emptyList());
                List<String> v = votos.get(k).getOrDefault(t, Collections.emptyList());
                // Se houve evento LIDER neste termo, calcula a duração da eleição; senão houve voto dividido / falta de quórum
                if (l != null) {
                    long dur = tLid.get(k).get(t) - tCand.get(k).getOrDefault(t, tLid.get(k).get(t));
                    System.out.println("p" + k + " Termo " + t + ": CAND=" + c + ", VOTOS=" + v + ", LIDER=" + l + " (Dur: " + dur + " ms)");
                } else {
                    System.out.println("p" + k + " Termo " + t + ": CAND=" + c + ", VOTOS=" + v + ", VOTO DIVIDIDO");
                }
            }
        }

        // Segurança: verifica se algum termo na mesma partição teve mais do que 1 líder
        boolean seguro = true;
        for (var e : lideresPorTermo.entrySet()) if (e.getValue() > 1) seguro = false;
        System.out.println("\nSegurança: " + (seguro ? "0 termos com múltiplos líderes." : "FALHOU (múltiplos líderes num termo)"));
        System.out.println("Termos Monotónicos: " + (monotono ? "O termo nunca diminuiu." : "FALHOU (regressão de termo detetada)"));

        System.out.println("\n--- Tempo até ao 1.º Líder por Partição ---");
        for (int k = 0; k < N; k++) {
            if (primeiroLiderPart[k] != Long.MAX_VALUE && primeiroArranqueGlobal != Long.MAX_VALUE) {
                long desdeGlobal = primeiroLiderPart[k] - primeiroArranqueGlobal;
                long desdePart = primeiroLiderPart[k] - primeiroArranquePart[k];
                System.out.println("p" + k + ": " + desdePart + " ms (desde 1.º arranque da partição) | " + desdeGlobal + " ms (desde 1.º arranque global)");
            }
        }

        // 2. Se existir falhas.txt, mede a indisponibilidade até ao 1.º LOTE de termo superior e as leituras enviadas nesse intervalo
        if (Files.exists(falhasFile)) {
            System.out.println("\n--- Indisponibilidades ---");
            List<String> produtorLog = Files.exists(dir.resolve("produtor.txt"))
                    ? Files.readAllLines(dir.resolve("produtor.txt"), StandardCharsets.UTF_8) : Collections.emptyList();

            for (String falha : Files.readAllLines(falhasFile, StandardCharsets.UTF_8)) {
                if (falha.isBlank()) continue;
                String[] f = falha.split(";");
                if (f.length < 3) continue;
                int p = Integer.parseInt(f[0].replace("p", ""));
                long tFalha = Long.parseLong(f[2]);

                // Descobre qual era o termo ativo antes da falha (último LOTE publicado até tFalha)
                long termoNaFalha = 0;
                for (int i = 1; i <= 3; i++) {
                    Path hb = dir.resolve("hb-p" + p + "-n" + i + ".txt");
                    if (!Files.exists(hb)) continue;
                    for (String line : Files.readAllLines(hb, StandardCharsets.UTF_8)) {
                        if (line.isBlank()) continue;
                        String[] h = line.split(";");
                        long tLote = Long.parseLong(h[2]);
                        if (tLote <= tFalha) termoNaFalha = Math.max(termoNaFalha, Long.parseLong(h[0]));
                    }
                }

                // Procura o primeiro LOTE publicado após tFalha por um líder de termo estritamente superior
                long tNovoLote = Long.MAX_VALUE;
                long novoTermo = -1;
                for (int i = 1; i <= 3; i++) {
                    Path hb = dir.resolve("hb-p" + p + "-n" + i + ".txt");
                    if (!Files.exists(hb)) continue;
                    for (String line : Files.readAllLines(hb, StandardCharsets.UTF_8)) {
                        if (line.isBlank()) continue;
                        String[] h = line.split(";");
                        long term = Long.parseLong(h[0]);
                        long tLote = Long.parseLong(h[2]);
                        if (tLote > tFalha && term > termoNaFalha && tLote < tNovoLote) {
                            tNovoLote = tLote;
                            novoTermo = term;
                            break;
                        }
                    }
                }

                // Se encontrou recuperação, calcula a duração da indisponibilidade e conta leituras enviadas nesse intervalo
                if (tNovoLote != Long.MAX_VALUE) {
                    long indisp = tNovoLote - tFalha;
                    List<String> noIntervalo = new ArrayList<>();
                    for (String pl : produtorLog) {
                        if (pl.isBlank()) continue;
                        String[] pf = pl.split(";");
                        if (pf.length >= 4 && pf[1].equals("p" + p)) {
                            long t = Long.parseLong(pf[0]);
                            if (t >= tFalha && t <= tNovoLote) noIntervalo.add(pf[2] + "=" + pf[3]);
                        }
                    }
                    System.out.println("p" + p + " falhou em " + tFalha + " (termo " + termoNaFalha + " -> " + novoTermo
                            + ") | Indisp: " + indisp + " ms | Leituras enviadas no intervalo: " + noIntervalo.size() + " " + noIntervalo);
                }
            }
        }

        // 3. Verifica eventos TRUNCADO (e quantos descartados existem no líder final com outro seq) + coincidência final dos 3 nós
        System.out.println("\n--- Truncagens e Coincidência ---");
        for (int k = 0; k < N; k++) {
            // Constrói o multiconjunto (sensor:temp) do registo final do último líder para cruzar com os descartados
            Map<String, Integer> multisetLider = new HashMap<>();
            if (ultimoLider.containsKey(k)) {
                Path leaderLog = dir.resolve("no-p" + k + "-n" + ultimoLider.get(k) + ".txt");
                if (Files.exists(leaderLog)) {
                    for (String line : Files.readAllLines(leaderLog, StandardCharsets.UTF_8)) {
                        if (line.isBlank()) continue;
                        String[] f = line.split(";");
                        String key = f[2] + ":" + Double.parseDouble(f[3]);
                        multisetLider.merge(key, 1, Integer::sum);
                    }
                }
            }

            int truncados = 0;
            for (int i = 1; i <= 3; i++) {
                Path ev = dir.resolve("eventos-p" + k + "-n" + i + ".txt");
                if (!Files.exists(ev)) continue;
                for (String l : Files.readAllLines(ev, StandardCharsets.UTF_8)) {
                    if (l.contains(";TRUNCADO;")) {
                        truncados++;
                        String det = l.split(";", 4)[3];
                        int noLider = 0;
                        // Verifica quantos dos registos descartados também existem no líder final (com outro seq)
                        if (det.contains("registos=[")) {
                            String raw = det.substring(det.indexOf("registos=[") + 10);
                            raw = raw.substring(0, raw.indexOf(']'));
                            if (!raw.isBlank()) {
                                Map<String, Integer> copia = new HashMap<>(multisetLider);
                                for (String item : raw.split(",")) {
                                    String key = item.trim();
                                    int c = copia.getOrDefault(key, 0);
                                    if (c > 0) {
                                        noLider++;
                                        copia.put(key, c - 1);
                                    }
                                }
                            }
                        }
                        System.out.println("p" + k + " n" + i + ": " + det + " | No líder final com outro seq: " + noLider);
                    }
                }
            }
            if (truncados == 0) System.out.println("p" + k + ": 0 eventos TRUNCADO.");

            // Carrega os ficheiros finais dos 3 nós para verificar coincidência linha a linha até ao menor ultimoSeq
            Map<Long, String> n1 = carregarMapa(dir.resolve("no-p" + k + "-n1.txt"));
            Map<Long, String> n2 = carregarMapa(dir.resolve("no-p" + k + "-n2.txt"));
            Map<Long, String> n3 = carregarMapa(dir.resolve("no-p" + k + "-n3.txt"));
            if (n1.isEmpty() || n2.isEmpty() || n3.isEmpty()) {
                System.out.println("p" + k + ": ficheiro em falta/vazio num dos nós para coincidência.");
                continue;
            }

            long minSeq = Math.min(n1.keySet().stream().max(Long::compare).orElse(0L),
                    Math.min(n2.keySet().stream().max(Long::compare).orElse(0L),
                            n3.keySet().stream().max(Long::compare).orElse(0L)));

            // Compara cada seq de 1 até ao menor ultimoSeq comum entre os 3 nós
            List<Long> divs = new ArrayList<>();
            for (long s = 1; s <= minSeq; s++) {
                String l1 = n1.get(s), l2 = n2.get(s), l3 = n3.get(s);
                if (l1 == null || l2 == null || l3 == null || !l1.equals(l2) || !l1.equals(l3)) divs.add(s);
            }
            System.out.println("p" + k + " (até seq=" + minSeq + "): " + (divs.isEmpty() ? "IDÊNTICOS" : "DIVERGEM em " + divs));
        }

        // 4. Compara o multiconjunto (sensor, temperatura) de produtor.txt com os registos únicos persistidos nos nós da partição
        System.out.println("\n--- Leituras Perdidas Globais ---");
        Map<Integer, Map<String, Integer>> enviadasPorPart = new HashMap<>();
        for (int k = 0; k < N; k++) enviadasPorPart.put(k, new HashMap<>());
        // Conta quantas vezes cada par (sensor;temperatura) foi enviado pelo produtor para cada partição
        if (Files.exists(dir.resolve("produtor.txt"))) {
            for (String line : Files.readAllLines(dir.resolve("produtor.txt"), StandardCharsets.UTF_8)) {
                if (line.isBlank()) continue;
                String[] f = line.split(";");
                if (f.length >= 4) {
                    int k = Integer.parseInt(f[1].replace("p", ""));
                    String key = f[2] + ";" + Double.parseDouble(f[3]);
                    enviadasPorPart.get(k).merge(key, 1, Integer::sum);
                }
            }
        }

        for (int k = 0; k < N; k++) {
            // União por (seq, termo, sensor, temp) para não contar o mesmo registo replicado em 3 nós
            Set<String> uniaoRegistos = new HashSet<>();
            for (int i = 1; i <= 3; i++) {
                Path noFile = dir.resolve("no-p" + k + "-n" + i + ".txt");
                if (!Files.exists(noFile)) continue;
                for (String line : Files.readAllLines(noFile, StandardCharsets.UTF_8)) {
                    if (line.isBlank()) continue;
                    String[] f = line.split(";");
                    uniaoRegistos.add(f[0] + ";" + f[1] + ";" + f[2] + ";" + Double.parseDouble(f[3]));
                }
            }
            // Converte a união num multiconjunto (sensor;temp -> quantidade) e subtrai do total enviado
            Map<String, Integer> gravadas = new HashMap<>();
            for (String u : uniaoRegistos) {
                String[] f = u.split(";", 3);
                gravadas.merge(f[2], 1, Integer::sum);
            }

            int perdidas = 0;
            for (var e : enviadasPorPart.get(k).entrySet()) {
                int env = e.getValue();
                int grav = gravadas.getOrDefault(e.getKey(), 0);
                if (env > grav) perdidas += (env - grav);
            }
            System.out.println("p" + k + ": " + perdidas + " leituras perdidas.");
        }
    }

    private static Map<Long, String> carregarMapa(Path p) throws Exception {
        Map<Long, String> m = new HashMap<>();
        if (!Files.exists(p)) return m;
        for (String line : Files.readAllLines(p, StandardCharsets.UTF_8)) {
            if (line.isBlank()) continue;
            String[] f = line.split(";");
            if (f.length > 0) m.put(Long.parseLong(f[0]), line);
        }
        return m;
    }
}