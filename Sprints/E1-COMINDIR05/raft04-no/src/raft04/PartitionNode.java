package raft04;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Nó de partição (substitui PartitionLeader e Replica da ficha anterior).
 * Cada partição k tem 3 nós idênticos (i = 1..3), um eleito LIDER e os outros SEGUIDORES.
 * Comunica por 3 tópicos IPFS (IN, LOTE, ELEICAO) e mantém um servidor TCP para recuperação/truncagem.
 */
public class PartitionNode {
    private final int partition;
    private final int self;
    private final int n;
    private final RaftState raftState;
    private final IpfsPubSub ipfs = new IpfsPubSub(Wire.IPFS_API);

    private long ultimoSeq = 0;
    private long ultimoTermo = 0;

    private final Path logFile, stateFile, eventFile, hbFile;

    private long hb = 0;
    private final List<SensorRecord> pendentes = new ArrayList<>();
    private ScheduledExecutorService leaderExecutor;
    private VectorClock currentVector;
    private long currentLamport = 0;

    private long expectedSeq = 1;

    public PartitionNode(int partition, int self, int n) throws IOException {
        this.partition = partition;
        this.self = self;
        this.n = n;

        this.logFile = Paths.get("no-p" + partition + "-n" + self + ".txt");
        this.stateFile = Paths.get("estado-p" + partition + "-n" + self + ".txt");
        this.eventFile = Paths.get("eventos-p" + partition + "-n" + self + ".txt");
        this.hbFile = Paths.get("hb-p" + partition + "-n" + self + ".txt");

        carregarEstadoDoFicheiro();

        this.raftState = new RaftState(partition, self, Wire.GRUPO, Wire.ELEICAO_MIN_MS, Wire.ELEICAO_MAX_MS, stateFile,
                msg -> {
                    String[] partes = msg.split(";", 3);
                    logEvent(partes[0], partes.length > 2 ? partes[2] : "");
                });
    }

    // Lê o ficheiro local de registos no arranque para retomar ultimoSeq, ultimoTermo, L e V.
    // Assim, se este nó se tornar líder, L e V continuam a partir do último registo (não recomeçam a zero).
    private void carregarEstadoDoFicheiro() throws IOException {
        // Se já existe histórico em disco, percorre todas as linhas para restaurar o último estado causal
        if (Files.exists(logFile)) {
            List<String> linhas = Files.readAllLines(logFile, StandardCharsets.UTF_8);
            for (String l : linhas) {
                if (l.trim().isEmpty()) continue;
                SensorRecord r = SensorRecord.fromLine(l, n);
                ultimoSeq = r.getSeq();
                ultimoTermo = r.getTerm();
                currentVector = r.getVector().copy();
                currentLamport = r.getLamport();
                expectedSeq = ultimoSeq + 1;
            }
        }
        // Se o ficheiro não existia ou estava vazio, começa com relógio vetorial e Lamport a zero
        if (currentVector == null) {
            currentVector = new VectorClock(n);
            currentLamport = 0;
        }
    }

    // Arranca o servidor TCP de recuperação, as 3 threads de subscrição IPFS e o timer periódico de eleição
    public void start() {
        logEvent("ARRANQUE", "ultimoSeq=" + ultimoSeq + ", ultimoTermo=" + ultimoTermo);
        iniciarServidorTCP();

        // Subscreve leituras (IN), lotes/heartbeats (LOTE) e mensagens de eleição (PEDIDO_VOTO / VOTO)
        new Thread(() -> {
            try { ipfs.subscribe(Wire.topicIn(partition), this::handleIn); } catch (Exception e) { logEvent("ERRO", "sub IN: " + e.getMessage()); }
        }).start();
        new Thread(() -> {
            try { ipfs.subscribe(Wire.topicData(partition), this::handleBatch); } catch (Exception e) { logEvent("ERRO", "sub LOTE: " + e.getMessage()); }
        }).start();
        new Thread(() -> {
            try { ipfs.subscribe(Wire.topicElection(partition), this::handleElection); } catch (Exception e) { logEvent("ERRO", "sub ELEICAO: " + e.getMessage()); }
        }).start();

        // Verifica a cada VERIFICACAO_MS (200 ms) se o prazo de eleição expirou
        ScheduledExecutorService tickExecutor = Executors.newSingleThreadScheduledExecutor();
        tickExecutor.scheduleAtFixedRate(this::checkTick, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);
    }

    // Corre tick() mesmo isolado (permite ao seguidor isolado avançar o termo);
    // apenas bloqueia a publicação no tópico de eleição durante o isolamento.
    private void checkTick() {
        long term = raftState.tick();
        // Só difunde PEDIDO_VOTO se o prazo expirou (term != -1) e o nó NÃO estiver isolado
        if (term != -1 && !Wire.isolated(partition, self)) {
            long uSeq, uTerm;
            synchronized (this) {
                uSeq = ultimoSeq;
                uTerm = ultimoTermo;
            }
            String req = new VoteRequest(term, self, uSeq, uTerm).encode();
            try { ipfs.publish(Wire.topicElection(partition), req); } catch (Exception ignored) {}
        }
    }

    // Receção de leituras IN (tópico raft04-in-p<k>, ativo mesmo quando isolado para simular partição assimétrica).
    // Ordem de locks: adquire sempre primeiro 'this' (PartitionNode) e só depois 'raftState' (evita deadlock).
    private void handleIn(String msg) {
        try {
            // Ignora mensagens que não sejam leituras IN ou que pertençam a outra partição
            if (!msg.startsWith("IN;")) return;
            String[] f = Wire.body(msg).split(";");
            if (f.length != 4) return;
            String sensor = f[0];
            if (Partitioner.getPartition(sensor, n) != partition) return;

            double temp = Double.parseDouble(f[1]);
            long l = Long.parseLong(f[2]);
            VectorClock v = VectorClock.parse(f[3], n);

            synchronized (this) {
                // Verifica papel e termo dentro do mesmo monitor em que atribui seq/termo:
                // garante que nenhuma leitura recebe seq com termo antigo após o nó se demitir.
                long t = raftState.termIfLeader();
                // Se termIfLeader() devolver -1, este nó não é líder (é seguidor ou candidato) -> ignora o IN
                if (t == -1) {
                    logEvent("IGNORADO", "IN recebido mas não sou líder");
                    return;
                }

                // Atualiza relógio de Lamport e relógio vetorial causal (V[k] acompanha exatamente seq)
                currentLamport = Math.max(currentLamport, l) + 1;
                currentVector.merge(v);
                currentVector.tick(partition);

                ultimoSeq++;
                expectedSeq = ultimoSeq + 1;
                SensorRecord r = SensorRecord.now(ultimoSeq, t, sensor, temp, currentLamport, currentVector.copy());
                pendentes.add(r);
                ultimoTermo = t;
                gravarRegisto(r);
            }
        } catch (Exception e) {
            logEvent("ERRO", "IN inválido: " + e.getMessage());
        }
    }

    // Trata pedidos de voto e respostas de voto no tópico raft04-eleicao-p<k> (ignorado se isolado).
    private void handleElection(String msg) {
        // Simulação de partição de rede: nó isolado não recebe mensagens de eleição
        if (Wire.isolated(partition, self)) return;
        try {
            // Caso 1: outro nó pediu voto (PEDIDO_VOTO)
            if (msg.startsWith("PEDIDO_VOTO")) {
                VoteRequest req = VoteRequest.decode(msg);
                if (req.candidate == self) return; // Ignora o próprio pedido difundido pelo PubSub
                RaftState.Vote vote;
                synchronized (this) { // Ordem de locks: this -> raftState
                    vote = raftState.onVoteRequest(req.term, req.candidate, req.lastSeq, req.lastTerm, ultimoSeq, ultimoTermo);
                }
                // Responde sempre (S ou N), para que o candidato saiba também se o seu termo ficou obsoleto
                VoteReply reply = new VoteReply(vote.term, self, req.candidate, vote.granted);
                try { ipfs.publish(Wire.topicElection(partition), reply.encode()); } catch (Exception ignored) {}
            // Caso 2: resposta a um pedido de voto (VOTO)
            } else if (msg.startsWith("VOTO")) {
                VoteReply reply = VoteReply.decode(msg);
                if (reply.voter == self) return;
                // Só processa votos dirigidos a este nó enquanto candidato
                if (reply.candidate == self) {
                    synchronized (this) { // Ordem de locks: this -> raftState; impede IN de entrar antes de becomeLeader()
                        // Se onVote devolver true, atingiu maioria exatamente agora -> assume liderança
                        if (raftState.onVote(reply.term, reply.voter, reply.candidate, reply.granted)) {
                            becomeLeader();
                        }
                    }
                }
            }
        } catch (Exception e) {
            logEvent("ERRO", "Eleição inválida: " + e.getMessage());
        }
    }

    // Trata lotes/heartbeats recebidos no tópico raft04-p<k> (comportamento de seguidor).
    private void handleBatch(String msg) {
        // Simulação de partição de rede: nó isolado não processa lotes
        if (Wire.isolated(partition, self)) return;
        try {
            Batch b = Batch.decode(msg, n);
            if (b.leader == self) return; // Ignora os próprios lotes

            synchronized (this) { // Ordem de locks: this -> raftState
                // Valida o termo do lote no RaftState (demite se b.term > term; rejeita se b.term < term)
                if (raftState.onBatch(b.term, b.leader)) {
                    logEvent("RECEBIDO", "LOTE termo=" + b.term + " hb=" + b.hb);
                    // Deteta divergência se temos mais registos que o líder ou se o termo de ultimoSeq difere
                    if (ultimoSeq > b.lastSeq || (ultimoSeq == b.lastSeq && ultimoTermo != b.lastTerm)) {
                        resolverDivergencia(b.leader);
                        return;
                    }
                    aplicarRegistos(b.records, b.leader);
                    // Se ainda faltam registos até b.lastSeq (ex.: lotes perdidos), recupera por TCP
                    if (ultimoSeq < b.lastSeq) recuperarDoLider(b.leader, ultimoSeq + 1);
                } else {
                    // Lote de termo inferior ao nosso (líder obsoleto) -> descartado
                    logEvent("IGNORADO", "LOTE termo=" + b.term);
                }
            }
        } catch (Exception e) {
            logEvent("ERRO", "LOTE inválido: " + e.getMessage());
        }
    }

    // Resolve divergências quando este nó tem registos de outro termo que não existem no líder atual.
    // Deve ser chamado dentro de synchronized (this).
    private void resolverDivergencia(int leaderId) {
        try {
            // Carrega todos os registos locais para memória para procurar o ponto de corte
            List<SensorRecord> locais = new ArrayList<>();
            if (Files.exists(logFile)) {
                for (String l : Files.readAllLines(logFile, StandardCharsets.UTF_8)) {
                    if (!l.isBlank()) locais.add(SensorRecord.fromLine(l, n));
                }
            }
            if (locais.isEmpty()) return;

            // Identifica o índice onde começa cada bloco contíguo de registos com o mesmo termo
            List<Integer> blockStarts = new ArrayList<>();
            long prevTerm = Long.MIN_VALUE;
            for (int idx = 0; idx < locais.size(); idx++) {
                long t = locais.get(idx).getTerm();
                if (t != prevTerm) {
                    blockStarts.add(idx);
                    prevTerm = t;
                }
            }

            // Recua um bloco de termo de cada vez e pede por TCP a partir do 1.º seq desse bloco,
            // até o primeiro registo devolvido pelo líder coincidir (seq, term) com o local
            int chosenStartIdx = 0;
            List<SensorRecord> doLider = Collections.emptyList();
            for (int b = blockStarts.size() - 1; b >= 0; b--) {
                int startIdx = blockStarts.get(b);
                SensorRecord anchor = locais.get(startIdx);
                doLider = pedirTCP(leaderId, anchor.getSeq()); // lança IOException se falhar
                chosenStartIdx = startIdx;
                if (!doLider.isEmpty()) {
                    SensorRecord first = doLider.get(0);
                    if (first.getSeq() == anchor.getSeq() && first.getTerm() == anchor.getTerm()) {
                        break;
                    }
                }
            }

            // Compara os registos locais com os do líder para achar o primeiro seq divergente (seqCorte)
            long seqCorte = -1;
            int localTailSize = locais.size() - chosenStartIdx;
            int minSize = Math.min(localTailSize, doLider.size());
            for (int j = 0; j < minSize; j++) {
                SensorRecord l = locais.get(chosenStartIdx + j);
                SensorRecord r = doLider.get(j);
                if (l.getSeq() != r.getSeq() || l.getTerm() != r.getTerm() || !l.getSensorId().equals(r.getSensorId())) {
                    seqCorte = r.getSeq();
                    break;
                }
            }

            // Se os prefixos coincidem mas o ficheiro local tem registos a mais (incluindo líder vazio), corta o excesso
            if (seqCorte == -1 && localTailSize > doLider.size()) {
                seqCorte = doLider.isEmpty()
                        ? locais.get(chosenStartIdx).getSeq()
                        : doLider.get(doLider.size() - 1).getSeq() + 1;
            }

            if (seqCorte == -1) return;

            // Separa os registos válidos (< seqCorte) dos registos que vão ser descartados (>= seqCorte)
            long descartados = ultimoSeq - seqCorte + 1;
            List<Long> termosDescartados = new ArrayList<>();
            List<String> registosDescartados = new ArrayList<>();
            List<String> novasLinhas = new ArrayList<>();

            for (SensorRecord r : locais) {
                if (r.getSeq() >= seqCorte) {
                    if (!termosDescartados.contains(r.getTerm())) termosDescartados.add(r.getTerm());
                    registosDescartados.add(r.getSensorId() + ":" + r.getTemperature());
                } else {
                    novasLinhas.add(r.toLine());
                }
            }

            // Reescreve o ficheiro de forma atómica sem os registos divergentes
            Path tmp = Paths.get(logFile.toString() + ".tmp");
            Files.write(tmp, novasLinhas, StandardCharsets.UTF_8);
            Files.move(tmp, logFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

            // Recalcula ultimoTermo, L e V a partir do último registo válido que ficou (para manter V[k]==seq e Lamport)
            if (novasLinhas.isEmpty()) {
                ultimoTermo = 0;
                currentVector = new VectorClock(n);
                currentLamport = 0;
            } else {
                SensorRecord ultimo = SensorRecord.fromLine(novasLinhas.get(novasLinhas.size() - 1), n);
                ultimoTermo = ultimo.getTerm();
                currentVector = ultimo.getVector().copy();
                currentLamport = ultimo.getLamport();
            }

            ultimoSeq = seqCorte - 1;
            expectedSeq = seqCorte;
            logEvent("TRUNCADO", "de=" + seqCorte + ";descartados=" + descartados
                    + ";termos=" + termosDescartados + ";registos=" + registosDescartados);

            // Aplica por cima os registos oficiais recebidos do líder a partir de seqCorte
            for (SensorRecord r : doLider) {
                if (r.getSeq() >= seqCorte) {
                    gravarRegisto(r);
                    expectedSeq = r.getSeq() + 1;
                }
            }
        } catch (IOException e) {
            logEvent("ERRO", "Falha na truncagem: " + e.getMessage());
        }
    }

    // Transição para LIDER: reinicia hb e pendentes e envia o 1.º LOTE imediatamente (afirma liderança).
    // Nota: ultimoSeq, currentLamport e currentVector já vêm atualizados do ficheiro/registos aplicados.
    // Deve ser chamado dentro de synchronized (this).
    private void becomeLeader() {
        hb = 0;
        pendentes.clear();
        if (leaderExecutor != null) leaderExecutor.shutdownNow();
        leaderExecutor = Executors.newSingleThreadScheduledExecutor();
        leaderExecutor.scheduleAtFixedRate(this::enviarLote, 0, Wire.PERIODO_HB_MS, TimeUnit.MILLISECONDS);
    }

    // Publica periodicamente um LOTE (serve simultaneamente de heartbeat e envio de registos pendentes).
    private void enviarLote() {
        if (Wire.isolated(partition, self)) return;
        long t;
        List<SensorRecord> lote;
        long cSeq, cTerm;
        synchronized (this) {
            t = raftState.termIfLeader();
            if (t == -1) { // Se entretanto se demitiu, cancela os envios periódicos
                if (leaderExecutor != null) leaderExecutor.shutdownNow();
                return;
            }
            lote = new ArrayList<>(pendentes);
            pendentes.clear();
            hb++;
            cSeq = ultimoSeq;
            cTerm = ultimoTermo;
        }
        // Publicação IPFS e escrita em hb-* ficam fora do bloco synchronized para não bloquear handleIn
        Batch b = new Batch(t, self, "localhost", Wire.nodePort(partition, self), hb, cSeq, cTerm, lote);
        String enc = b.encode();
        try {
            ipfs.publish(Wire.topicData(partition), enc);
            Files.writeString(hbFile, t + ";" + hb + ";" + System.currentTimeMillis() + ";" + cSeq + ";" + cTerm + ";" + lote.size() + ";" + enc.getBytes(StandardCharsets.UTF_8).length + "\n",
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (Exception ignored) {}
    }

    // Aplica os registos recebidos num lote pela ordem: igual a expectedSeq -> grava; maior -> lacuna (TCP); menor -> verifica duplicado/divergência.
    private void aplicarRegistos(List<SensorRecord> records, int leaderId) {
        for (SensorRecord r : records) {
            // Caso normal: é exatamente o próximo seq esperado
            if (r.getSeq() == expectedSeq) {
                gravarRegisto(r);
                expectedSeq++;
            // Lacuna: saltou números de sequência (perdeu lotes) -> pede os em falta por TCP
            } else if (r.getSeq() > expectedSeq) {
                logEvent("LACUNA", "esperava " + expectedSeq + " mas recebi " + r.getSeq());
                recuperarDoLider(leaderId, expectedSeq);
                return;
            // Já tinha este seq: se o termo for diferente há divergência (truncagem); se for igual é duplicado e ignora
            } else if (r.getSeq() < expectedSeq) {
                try {
                    for (String l : Files.readAllLines(logFile, StandardCharsets.UTF_8)) {
                        if (l.isBlank()) continue;
                        SensorRecord local = SensorRecord.fromLine(l, n);
                        if (local.getSeq() == r.getSeq() && local.getTerm() != r.getTerm()) {
                            resolverDivergencia(leaderId);
                            return;
                        }
                    }
                } catch (IOException ignored) {}
            }
        }
    }

    // Acrescenta um registo ao ficheiro local e atualiza o estado em memória (ultimoSeq, ultimoTermo, L e V)
    private void gravarRegisto(SensorRecord r) {
        try {
            Files.writeString(logFile, r.toLine() + "\n", StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            ultimoSeq = r.getSeq();
            ultimoTermo = r.getTerm();
            currentVector = r.getVector().copy();
            currentLamport = Math.max(currentLamport, r.getLamport());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Cliente TCP de recuperação: pede ao nó leaderId todos os registos a partir de fromSeq
    private List<SensorRecord> pedirTCP(int leaderId, long fromSeq) throws IOException {
        // Se algum dos dois nós estiver isolado, simula falha da ligação TCP
        if (Wire.isolated(partition, self) || Wire.isolated(partition, leaderId)) {
            throw new IOException("Ligação TCP indisponível por isolamento");
        }
        List<SensorRecord> result = new ArrayList<>();
        try (Socket socket = new Socket("localhost", Wire.nodePort(partition, leaderId))) {
            socket.setSoTimeout(Wire.T_TCP_MS);
            try (PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {
                out.println("PEDIDO;" + fromSeq + ";0,0,0");
                String line;
                while ((line = in.readLine()) != null && !line.equals("FIM")) {
                    result.add(SensorRecord.fromLine(line, n));
                }
            }
        }
        return result;
    }

    // Pede registos em atraso ao líder e aplica apenas os que ainda faltam (>= expectedSeq)
    private void recuperarDoLider(int leaderId, long fromSeq) {
        try {
            List<SensorRecord> records = pedirTCP(leaderId, fromSeq);
            for (SensorRecord r : records) {
                if (r.getSeq() >= expectedSeq) {
                    gravarRegisto(r);
                    expectedSeq = r.getSeq() + 1;
                }
            }
            if (!records.isEmpty()) logEvent("RECUPERACAO", "Recuperado até seq=" + (expectedSeq - 1));
        } catch (IOException e) {
            logEvent("ERRO", "Falha na recuperação TCP: " + e.getMessage());
        }
    }

    // Servidor TCP de recuperação (aberto por qualquer nó no arranque para responder a pedidos PEDIDO)
    private void iniciarServidorTCP() {
        new Thread(() -> {
            try (ServerSocket server = new ServerSocket(Wire.nodePort(partition, self))) {
                while (true) {
                    Socket client = server.accept();
                    new Thread(() -> atenderTCP(client)).start();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }

    // Responde a um pedido TCP enviando as linhas com seq >= from seguidas de FIM
    private void atenderTCP(Socket socket) {
        // Se este nó estiver isolado, fecha imediatamente a ligação
        if (Wire.isolated(partition, self)) {
            try { socket.close(); } catch (IOException ignored) {}
            return;
        }
        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
             PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true)) {
            String req = in.readLine();
            if (req != null && req.startsWith("PEDIDO")) {
                long from = req.contains(";")
                        ? Long.parseLong(req.split(";")[1])
                        : Long.parseLong(req.trim().split("\\s+")[1]);
                // Lê o ficheiro sob synchronized(this) para evitar conflitos com escritas/truncagens concorrentes
                List<String> linhas;
                synchronized (this) {
                    linhas = Files.exists(logFile) ? Files.readAllLines(logFile, StandardCharsets.UTF_8) : Collections.emptyList();
                }
                for (String l : linhas) {
                    if (!l.isBlank() && SensorRecord.fromLine(l, n).getSeq() >= from) out.println(l);
                }
                out.println("FIM");
            }
        } catch (Exception ignored) {}
    }

    // Regista o evento na consola e em eventos-p<k>-n<i>.txt com o timestamp em milissegundos
    public void logEvent(String evento, String detalhe) {
        String linha = System.currentTimeMillis() + ";" + evento + ";p" + partition + ";" + detalhe;
        System.out.println("[p" + partition + "-n" + self + "] " + evento + " - " + detalhe);
        try {
            Files.writeString(eventFile, linha + "\n", StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException ignored) {}
    }

    // Ponto de entrada: PartitionNode <k> <i> [N] (por omissão N = 3 partições)
    public static void main(String[] args) throws Exception {
        int k = Integer.parseInt(args[0]);
        int i = Integer.parseInt(args[1]);
        int n = (args.length > 2) ? Integer.parseInt(args[2]) : 3;
        new PartitionNode(k, i, n).start();
        Thread.sleep(Long.MAX_VALUE);
    }
}