package raft04;

import java.nio.file.Files;
import java.nio.file.Path;

public class TestVoteRequest {
    public static void main(String[] args) throws Exception {
        System.out.println("=== TESTE 2: ONVOTEREQUEST ===");
        Path file = Path.of("estado-teste-voto.txt");

        // --- PREPARAÇÃO: Criar um nó e forçá-lo para o Termo 2, tendo votado no n3 ---
        Files.deleteIfExists(file);
        RaftState no = new RaftState(0, 1, 3, 6000, 12000, file, System.out::println);
        no.onVoteRequest(2, 3, 5, 1, 5, 1); // Adota termo 2 e vota no n3
        System.out.println("(Preparado: Termo 2, VotouEm 3)\n");

        // Caso 1: Termo inferior
        var r1 = no.onVoteRequest(1, 2, 5, 1, 5, 1);
        System.out.println("Caso 1 (Termo inferior 1): Decisao=" + r1.granted + " | Termo=" + no.term() + " | VotouEm=" + no.votedFor());

        // Caso 2: Termo superior (3), mas desatualizado
        var r2 = no.onVoteRequest(3, 2, 4, 1, 5, 1);
        System.out.println("Caso 2 (Termo superior 3, atrasado): Decisao=" + r2.granted + " | Termo=" + no.term() + " | VotouEm=" + no.votedFor());

        // Caso 3: Termo superior (4) e atualizado
        var r3 = no.onVoteRequest(4, 2, 6, 1, 5, 1);
        System.out.println("Caso 3 (Termo superior 4, atualizado): Decisao=" + r3.granted + " | Termo=" + no.term() + " | VotouEm=" + no.votedFor());

        // Caso 4: Repetição para o mesmo candidato (n2)
        var r4 = no.onVoteRequest(4, 2, 6, 1, 5, 1);
        System.out.println("Caso 4 (Repetido n2): Decisao=" + r4.granted + " | Termo=" + no.term() + " | VotouEm=" + no.votedFor());

        // Caso 5: Outro candidato (n3) no mesmo termo
        var r5 = no.onVoteRequest(4, 3, 6, 1, 5, 1);
        System.out.println("Caso 5 (Outro candidato n3): Decisao=" + r5.granted + " | Termo=" + no.term() + " | VotouEm=" + no.votedFor() + "\n");

        Files.deleteIfExists(file);
    }
}