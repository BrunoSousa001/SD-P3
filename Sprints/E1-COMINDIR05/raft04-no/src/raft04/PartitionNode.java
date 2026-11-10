package raft04;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

public class PartitionNode {
    private final int partition;
    private final int self;
    private final int n;
    private final RaftState raftState;

    private long ultimoSeq = 0;
    private long ultimoTermo = 0;

    private final Path logFile, stateFile, eventFile, hbFile;

    private long hb = 0;
    private List<SensorRecord> pendentes = new ArrayList<>();
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

        // Correção do erro de compilação:
        this.raftState = new RaftState(partition, self, Wire.GRUPO, Wire.ELEICAO_MIN_MS, Wire.ELEICAO_MAX_MS, stateFile,
                msg -> {
                    String[] partes = msg.split(";", 2);
                    logEvent(partes[0], partes.length > 1 ? partes[1] : "");
                });
    }

    private void carregarEstadoDoFicheiro() throws IOException {
        if (Files.exists(logFile)) {
            List<String> linhas = Files.readAllLines(logFile, StandardCharsets.UTF_8);
            for (String l : linhas) {
                if(l.trim().isEmpty()) continue;
                SensorRecord r = SensorRecord.fromLine(l, n);
                ultimoSeq = r.getSeq();
                ultimoTermo = r.getTerm();
                currentVector = r.getVector().copy();
                currentLamport = r.getLamport();
                expectedSeq = ultimoSeq + 1;
            }
        }
        if (currentVector == null) {
            currentVector = new VectorClock(n);
            currentLamport = 0;
        }
    }

    public void start() {
        logEvent("ARRANQUE", "ultimoSeq=" + ultimoSeq + ", ultimoTermo=" + ultimoTermo);

        iniciarServidorTCP();

        PubSub.subscribe(Wire.topicIn(partition), this::handleIn);
        PubSub.subscribe(Wire.topicData(partition), this::handleBatch);
        PubSub.subscribe(Wire.topicElection(partition), this::handleElection);

        ScheduledExecutorService tickExecutor = Executors.newSingleThreadScheduledExecutor();
        tickExecutor.scheduleAtFixedRate(this::checkTick, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);
    }

    private void checkTick() {
        if (Wire.isolated(partition, self)) return;
        long term = raftState.tick();
        if (term != -1) {
            String req = new VoteRequest(term, self, ultimoSeq, ultimoTermo).encode();
            PubSub.publish(Wire.topicElection(partition), req);
        }
    }

    // 1. IN NÃO É AFETADO PELO ISOLAMENTO (O produtor consegue alcançar-nos)
    private void handleIn(String msg) {
        long t = raftState.termIfLeader();
        if (t == -1) {
            logEvent("IGNORADO", "IN recebido mas não sou líder");
            return;
        }
        synchronized (this) {
            String[] f = Wire.body(msg).split(";");
            String sensor = f[0];
            double temp = Double.parseDouble(f[1]);
            long l = Long.parseLong(f[2]);
            VectorClock v = VectorClock.parse(f[3], n);

            currentLamport = Math.max(currentLamport, l) + 1;
            currentVector.merge(v);
            currentVector.tick(self);

            ultimoSeq++;
            SensorRecord r = SensorRecord.now(ultimoSeq, t, sensor, temp, currentLamport, currentVector.copy());
            pendentes.add(r);
            ultimoTermo = t;
        }
    }

    private void handleElection(String msg) {
        if (Wire.isolated(partition, self)) return; // Eleição respeita isolamento

        if (msg.startsWith("PEDIDO_VOTO")) {
            VoteRequest req = VoteRequest.decode(msg);
            RaftState.Vote vote = raftState.onVoteRequest(req.term, req.candidate, req.lastSeq, req.lastTerm, ultimoSeq, ultimoTermo);
            VoteReply reply = new VoteReply(vote.term, self, req.candidate, vote.granted);
            PubSub.publish(Wire.topicElection(partition), reply.encode());
        } else if (msg.startsWith("VOTO")) {
            VoteReply reply = VoteReply.decode(msg);
            if (reply.candidate == self) {
                boolean becameLeader = raftState.onVote(reply.term, reply.voter, reply.candidate, reply.granted);
                if (becameLeader) {
                    becomeLeader();
                }
            }
        }
    }

    // 2. LOTE RESPEITA ISOLAMENTO
    private void handleBatch(String msg) {
        if (Wire.isolated(partition, self)) return;

        Batch b = Batch.decode(msg, n);
        if (b.leader == self) return;

        if (raftState.onBatch(b.term, b.leader)) {
            logEvent("RECEBIDO", "LOTE termo=" + b.term + " hb=" + b.hb);
            synchronized (this) {
                // 3. LÓGICA DE DIVERGÊNCIA E TRUNCAGEM
                if (ultimoSeq > b.lastSeq || (ultimoSeq == b.lastSeq && ultimoTermo != b.lastTerm)) {
                    logEvent("DIVERGENCIA", "Local seq=" + ultimoSeq + " term=" + ultimoTermo + ", Líder seq=" + b.lastSeq + " term=" + b.lastTerm);
                    truncarAPartirDe(b.leader);
                }

                aplicarRegistos(b.records, b.leader);

                if (ultimoSeq < b.lastSeq) {
                    recuperarDoLider(b.leader, ultimoSeq + 1);
                }
            }
        }
    }

    private void truncarAPartirDe(int leaderId) {
        // Encontra o último bloco de termo local e pede os registos ao líder a partir dali
        long seqParaRecuar = ultimoSeq;
        long termoAnterior = ultimoTermo;
        // Simplificação: recua até encontrar um seq que o líder tenha no mesmo termo
        while (seqParaRecuar > 0) {
            try {
                List<String> linhas = Files.readAllLines(logFile);
                for (String l : linhas) {
                    SensorRecord r = SensorRecord.fromLine(l, n);
                    if (r.getSeq() == seqParaRecuar) {
                        termoAnterior = r.getTerm();
                        break;
                    }
                }
            } catch (IOException ignored) {}

            if (termoAnterior < ultimoTermo) break; // Encontrámos um bloco de termo anterior
            seqParaRecuar--;
        }

        long startTruncar = Math.max(1, seqParaRecuar + 1);
        long descartados = ultimoSeq - startTruncar + 1;

        try {
            List<String> linhas = Files.readAllLines(logFile, StandardCharsets.UTF_8);
            List<String> novasLinhas = new ArrayList<>();
            for (String l : linhas) {
                if (l.trim().isEmpty()) continue;
                SensorRecord r = SensorRecord.fromLine(l, n);
                if (r.getSeq() < startTruncar) {
                    novasLinhas.add(l);
                }
            }
            // Escrita atómica
            Path tmp = Paths.get(logFile.toString() + ".tmp");
            Files.write(tmp, novasLinhas, StandardCharsets.UTF_8);
            Files.move(tmp, logFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

            ultimoSeq = startTruncar - 1;
            expectedSeq = startTruncar;
            logEvent("TRUNCADO", "de=" + startTruncar + ";descartados=" + descartados);

            // Recupera os dados corretos do líder
            recuperarDoLider(leaderId, startTruncar);

        } catch (IOException e) {
            logEvent("ERRO", "Falha ao truncar: " + e.getMessage());
        }
    }

    private synchronized void becomeLeader() {
        logEvent("LIDER", "Tornei-me líder no termo " + raftState.term());
        hb = 0;
        pendentes.clear();
        if (leaderExecutor != null) leaderExecutor.shutdownNow();
        leaderExecutor = Executors.newSingleThreadScheduledExecutor();
        leaderExecutor.scheduleAtFixedRate(this::enviarLote, 0, Wire.PERIODO_HB_MS, TimeUnit.MILLISECONDS);
    }

    private void enviarLote() {
        long t = raftState.termIfLeader();
        if (t == -1) {
            if (leaderExecutor != null) leaderExecutor.shutdownNow();
            return;
        }
        List<SensorRecord> lote;
        long currentLastSeq;
        synchronized (this) {
            lote = new ArrayList<>(pendentes);
            pendentes.clear();
            hb++;
            currentLastSeq = ultimoSeq;
        }
        Batch b = new Batch(t, self, "localhost", Wire.nodePort(partition, self), hb, currentLastSeq, ultimoTermo, lote);
        PubSub.publish(Wire.topicData(partition), b.encode());
        try {
            Files.writeString(hbFile, t + ";" + hb + ";" + System.currentTimeMillis() + ";" + currentLastSeq + ";" + ultimoTermo + ";" + lote.size() + "\n",
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException ignored) {}
    }

    private void aplicarRegistos(List<SensorRecord> records, int leaderId) {
        for (SensorRecord r : records) {
            if (r.getSeq() == expectedSeq) {
                gravarRegisto(r);
                expectedSeq++;
            } else if (r.getSeq() > expectedSeq) {
                logEvent("LACUNA", "esperava " + expectedSeq + " mas recebi " + r.getSeq());
                recuperarDoLider(leaderId, expectedSeq);
                return;
            }
        }
    }

    private void gravarRegisto(SensorRecord r) {
        try {
            Files.writeString(logFile, r.toLine() + "\n", StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            ultimoSeq = r.getSeq();
            ultimoTermo = r.getTerm();
            currentVector = r.getVector().copy();
            currentLamport = Math.max(currentLamport, r.getLamport());
        } catch (IOException e) { e.printStackTrace(); }
    }

    // 4. TCP RESPEITA ISOLAMENTO
    private void recuperarDoLider(int leaderId, long fromSeq) {
        if (Wire.isolated(partition, self) || Wire.isolated(partition, leaderId)) {
            logEvent("ERRO", "Recuperação TCP falhou: isolamento ativo.");
            return;
        }
        int port = Wire.nodePort(partition, leaderId);
        try (Socket socket = new Socket("localhost", port);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            out.println("PEDIDO;" + fromSeq + ";0,0,0");
            String line;
            while ((line = in.readLine()) != null && !line.equals("FIM")) {
                SensorRecord r = SensorRecord.fromLine(line, n);
                if (r.getSeq() >= expectedSeq) {
                    gravarRegisto(r);
                    expectedSeq = r.getSeq() + 1;
                }
            }
            logEvent("RECUPERACAO", "Recuperado até seq=" + (expectedSeq - 1));
        } catch (Exception e) {
            logEvent("ERRO", "Falha na recuperação TCP: " + e.getMessage());
        }
    }

    private void iniciarServidorTCP() {
        new Thread(() -> {
            try (ServerSocket server = new ServerSocket(Wire.nodePort(partition, self))) {
                while (true) {
                    Socket client = server.accept();
                    new Thread(() -> atenderTCP(client)).start();
                }
            } catch (IOException e) { e.printStackTrace(); }
        }).start();
    }

    private void atenderTCP(Socket socket) {
        if (Wire.isolated(partition, self)) {
            try { socket.close(); } catch (IOException ignored) {}
            return;
        }
        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
            String req = in.readLine();
            if (req != null && req.startsWith("PEDIDO;")) {
                long from = Long.parseLong(req.split(";")[1]);
                if (Files.exists(logFile)) {
                    List<String> lines = Files.readAllLines(logFile);
                    for (String l : lines) {
                        SensorRecord r = SensorRecord.fromLine(l, n);
                        if (r.getSeq() >= from) out.println(l);
                    }
                }
                out.println("FIM");
            }
        } catch (Exception ignored) {}
    }

    public void logEvent(String evento, String detalhe) {
        String linha = System.currentTimeMillis() + ";" + evento + ";p" + partition + ";" + detalhe;
        System.out.println("[p" + partition + "-n" + self + "] " + evento + " - " + detalhe);
        try {
            Files.writeString(eventFile, linha + "\n", StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException ignored) {}
    }

    public static void main(String[] args) throws Exception {
        int k = Integer.parseInt(args[0]);
        int i = Integer.parseInt(args[1]);
        int n = (args.length > 2) ? Integer.parseInt(args[2]) : 3;
        new PartitionNode(k, i, n).start();
        Thread.sleep(Long.MAX_VALUE);
    }
}