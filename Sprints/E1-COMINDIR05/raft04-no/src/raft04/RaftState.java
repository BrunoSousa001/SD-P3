package raft04;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

public final class RaftState {

    // Papéis que um nó pode assumir
    public enum Role {
        SEGUIDOR, CANDIDATO, LIDER
    }

    // --- Estado Persistente (Gravado em disco) ---
    private long term = 0;
    private int votedFor = 0; // 0 = ainda não votou neste termo

    // --- Estado Volátil (Apenas na memória) ---
    private Role role = Role.SEGUIDOR;
    private int leader = 0;   // 0 = desconhecido
    private final Set<Integer> votes = new HashSet<>();
    private long deadline;    // System.nanoTime() a partir do qual há eleição

    // --- Configuração e Infraestrutura ---
    private final int partition;
    private final int self;
    private final int groupSize;
    private final long minNanos;
    private final long maxNanos;
    private final Path stateFile;
    private final Consumer<String> log;

    // Construtor: Lê o estado inicial e prepara os tempos
    public RaftState(int partition, int self, int groupSize, long minMs, long maxMs,
                     Path stateFile, Consumer<String> log) throws IOException {
        this.partition = partition;
        this.self = self;
        this.groupSize = groupSize;
        this.minNanos = minMs * 1_000_000L;
        this.maxNanos = maxMs * 1_000_000L;
        this.stateFile = stateFile;
        this.log = log;

        // Leitura inicial do estado persistente
        if (Files.exists(stateFile)) {
            String[] f = Files.readString(stateFile, StandardCharsets.UTF_8).trim().split(";");
            if (f.length >= 2) {
                term = Long.parseLong(f[0]);
                votedFor = Integer.parseInt(f[1]);
            }
        } else {
            // Se o ficheiro não existe, cria-o logo com 0;0
            persist();
        }

        // Sorteia o primeiro prazo de eleição
        resetTimer();
    }

    // Sorteia um novo prazo de eleição
    private void resetTimer() {
        long t = (maxNanos > minNanos)
                ? ThreadLocalRandom.current().nextLong(minNanos, maxNanos + 1)
                : minNanos;
        deadline = System.nanoTime() + t;
    }

    // Classe para devolver a resposta do voto
    public static final class Vote {
        public final long term;
        public final boolean granted;
        public Vote(long term, boolean granted) { this.term = term; this.granted = granted; }
    }

    // Adotar um termo superior
    private void adoptTerm(long newTerm) {
        if (role == Role.LIDER) {
            log.accept("DEMISSAO;p" + partition + ";termo=" + term);
        }
        log.accept("TERMO;p" + partition + ";" + term + "->" + newTerm);
        term = newTerm;
        votedFor = 0; // Limpa o voto antigo
        role = Role.SEGUIDOR;
        leader = 0;
        persist();
    }

    // Processar um PEDIDO_VOTO
    public synchronized Vote onVoteRequest(long cTerm, int candidate, long cLastSeq, long cLastTerm,
                                           long myLastSeq, long myLastTerm) {
        // 1. Se o termo do candidato for maior, adota
        if (cTerm > term) {
            adoptTerm(cTerm);
        }

        // 2. Verifica se o candidato está atualizado
        boolean upToDate = cLastTerm > myLastTerm
                || (cLastTerm == myLastTerm && cLastSeq >= myLastSeq);

        // 3. As 3 condições
        boolean granted = cTerm == term
                && (votedFor == 0 || votedFor == candidate)
                && upToDate;

        // 4. Se conceder, grava no disco
        if (granted) {
            votedFor = candidate;
            persist();
            resetTimer();
        }

        log.accept("VOTO;p" + partition + ";termo=" + term + ";candidato=n" + candidate
                + ";" + (granted ? "concedido" : "recusado"));

        return new Vote(term, granted);
    }

    // VOTO recebido: devolve true apenas no instante em que este nó se torna líder
    public synchronized boolean onVote(long vTerm, int voter, int candidate, boolean granted) {
        // 1. Se o voto trouxer um termo superior, adota-o e demite-se
        if (vTerm > term) {
            adoptTerm(vTerm);
            return false;
        }

        // 2. Só processa o voto se eu for candidato neste termo, se o voto for para mim e se for "Sim"
        if (role != Role.CANDIDATO || vTerm != term || candidate != self || !granted) {
            return false;
        }

        // 3. Adiciona o voto ao conjunto (o Set ignora se o votante já lá estiver)
        votes.add(voter);

        // 4. Verifica a maioria (para grupo 3: > 3/2 -> > 1 -> 2 votos)
        if (votes.size() > groupSize / 2) {
            role = Role.LIDER;
            leader = self;
            log.accept("LIDER;p" + partition + ";termo=" + term + ";votos=" + votes);
            return true; // Devolve true para o PartitionNode começar a enviar Lotes
        }

        return false;
    }

    // LOTE de outro nó: devolve true se o lote deve ser aplicado pelo PartitionNode
    public synchronized boolean onBatch(long bTerm, int bLeader) {
        // 1. Lote com termo inferior (atrasado): ignorar
        if (bTerm < term) {
            return false;
        }

        // 2. Lote com termo superior: adotar o termo (isto já muda o papel para SEGUIDOR e limpa o voto)
        if (bTerm > term) {
            adoptTerm(bTerm);
        }

        // 3. Se eu for líder no mesmo termo, é um erro grave
        if (role == Role.LIDER) {
            log.accept("ERRO;p" + partition + ";dois lideres no termo " + term);
            return false;
        }

        // 4. Se eu era candidato, agora reconheço o líder e passo a seguidor
        role = Role.SEGUIDOR;

        // 5. Atualizar o líder se for diferente do que tinha guardado
        if (leader != bLeader) {
            leader = bLeader;
            log.accept("NOVO_LIDER;p" + partition + ";termo=" + term + ";lider=n" + bLeader);
        }

        // 6. Recebemos um heartbeat válido, reiniciamos o despertador
        resetTimer();
        return true;
    }

    public synchronized long tick() {
        if (role == Role.LIDER || System.nanoTime() < deadline) {
            return -1;
        }

        term++;
        votedFor = self;
        persist(); // Grava no disco antes de mudar o papel

        role = Role.CANDIDATO;
        leader = 0;
        votes.clear();
        votes.add(self);
        resetTimer();

        log.accept("CANDIDATO;p" + partition + ";termo=" + term);

        return term;
    }

    // --- Persistência ---
    private void persist() {
        try {
            Path tmp = stateFile.resolveSibling(stateFile.getFileName() + ".tmp");
            Files.writeString(tmp, term + ";" + votedFor + "\n", StandardCharsets.UTF_8);
            Files.move(tmp, stateFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // --- Getters para o teste ---
    public synchronized long term() { return term; }
    public synchronized int votedFor() { return votedFor; }
    public synchronized Role role() { return role; }
    public synchronized int votesCount() { return votes.size(); }
    public synchronized long deadline() { return deadline; }

    // Método apenas para testar o caso "já sou líder"
    public synchronized void forceLeaderForTesting() {
        this.role = Role.LIDER;
    }

    public synchronized int leader() { return leader; }
    // Devolve o termo atual se for líder, ou -1 se não for
    public synchronized long termIfLeader() {
        return role == Role.LIDER ? term : -1;
    }
}