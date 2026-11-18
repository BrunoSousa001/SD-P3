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

    // Adotar um termo superior: garante que um líder/candidato obsoleto se demite imediatamente
    // e que votedFor volta a 0 porque um novo termo é uma nova eleição (evita perder o direito a votar no termo novo).
    private void adoptTerm(long newTerm) {
        if (role == Role.LIDER) {
            log.accept("DEMISSAO;p" + partition + ";termo=" + term);
        }
        log.accept("TERMO;p" + partition + ";" + term + "->" + newTerm);
        term = newTerm;
        votedFor = 0; // Limpa o voto antigo (novo termo = nova eleição)
        role = Role.SEGUIDOR;
        leader = 0;
        persist();
    }

    // Processar um PEDIDO_VOTO de outro candidato
    public synchronized Vote onVoteRequest(long cTerm, int candidate, long cLastSeq, long cLastTerm,
                                           long myLastSeq, long myLastTerm) {
        // 1. Se o termo do candidato for maior, adota-o (e se era líder/candidato, demite-se para SEGUIDOR)
        if (cTerm > term) {
            adoptTerm(cTerm);
        }

        // 2. Restrição de voto (upToDate): impede eleger um nó a quem faltam registos já presentes noutros nós.
        // Compara primeiro o termo do último registo e, em caso de empate, o seq.
        boolean upToDate = cLastTerm > myLastTerm
                || (cLastTerm == myLastTerm && cLastSeq >= myLastSeq);

        // 3. As 3 condições de voto: mesmo termo, ainda não votou (ou repete no mesmo candidato por perda de rede) e atualizado
        boolean granted = cTerm == term
                && (votedFor == 0 || votedFor == candidate)
                && upToDate;

        // 4. Persiste ANTES de devolver a resposta para que, se reiniciar a seguir, não possa votar noutro candidato no mesmo termo
        if (granted) {
            votedFor = candidate;
            persist();
            resetTimer(); // Renova o prazo de eleição para não iniciar outra eleição logo de seguida
        }

        log.accept("VOTO;p" + partition + ";termo=" + term + ";candidato=n" + candidate
                + ";" + (granted ? "concedido" : "recusado"));

        return new Vote(term, granted);
    }

    // VOTO recebido: devolve true apenas no instante em que este nó atinge maioria e passa a líder
    public synchronized boolean onVote(long vTerm, int voter, int candidate, boolean granted) {
        // 1. Se a resposta trouxer um termo superior, adota-o e demite-se
        if (vTerm > term) {
            adoptTerm(vTerm);
            return false;
        }

        // 2. Só processa se continuar CANDIDATO no mesmo termo, o voto for para si e tiver sido concedido
        if (role != Role.CANDIDATO || vTerm != term || candidate != self || !granted) {
            return false;
        }

        // 3. Adiciona o votante ao Set (idempotente: votos repetidos do mesmo nó não contam duas vezes)
        votes.add(voter);

        // 4. Maioria estrita: floor(G/2) + 1 (em G=3 são necessários > 1, isto é, 2 votos)
        if (votes.size() > groupSize / 2) {
            role = Role.LIDER;
            leader = self;
            log.accept("LIDER;p" + partition + ";termo=" + term + ";votos=" + votes);
            return true; // Devolve true para o PartitionNode arrancar o envio periódico de LOTEs
        }

        return false;
    }

    // LOTE de outro nó: devolve true se o lote deve ser aplicado pelo PartitionNode
    public synchronized boolean onBatch(long bTerm, int bLeader) {
        // 1. Lote de termo inferior vem de um líder obsoleto: ignorar
        if (bTerm < term) {
            return false;
        }

        // 2. Lote de termo superior: adota o termo (demite líder/candidato antigo e limpa votedFor)
        if (bTerm > term) {
            adoptTerm(bTerm);
        }

        // 3. Se eu ainda fosse líder no mesmo termo, seria violação da segurança (impossível com maioria + 1 voto/termo)
        if (role == Role.LIDER) {
            log.accept("ERRO;p" + partition + ";dois lideres no termo " + term);
            return false;
        }

        // 4. Um candidato que recebe LOTE válido do termo atual reconhece o líder e passa a seguidor
        role = Role.SEGUIDOR;

        // 5. Atualiza o líder conhecido e regista mudança de liderança
        if (leader != bLeader) {
            leader = bLeader;
            log.accept("NOVO_LIDER;p" + partition + ";termo=" + term + ";lider=n" + bLeader);
        }

        // 6. Heartbeat válido recebido: renova o prazo aleatório de eleição
        resetTimer();
        return true;
    }

    // Executado periodicamente a cada VERIFICACAO_MS: se o prazo expirou e não somos líder, inicia eleição
    public synchronized long tick() {
        if (role == Role.LIDER || System.nanoTime() < deadline) {
            return -1;
        }

        term++;
        votedFor = self;
        persist(); // Grava em disco ANTES de publicar PEDIDO_VOTO (evita perder o voto próprio após crash)

        role = Role.CANDIDATO;
        leader = 0;
        votes.clear();
        votes.add(self);
        resetTimer(); // Sorteia novo prazo aleatório para resolver eventuais votos divididos

        log.accept("CANDIDATO;p" + partition + ";termo=" + term);

        return term;
    }

    // --- Persistência atómica ---
    // Escreve num ficheiro temporário e move com ATOMIC_MOVE para evitar ficheiro corrompido a meio de uma escrita em crash.
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