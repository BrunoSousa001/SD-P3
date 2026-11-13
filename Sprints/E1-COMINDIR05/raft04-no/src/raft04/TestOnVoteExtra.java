package raft04;

import java.nio.file.Files;
import java.nio.file.Path;

public class TestOnVoteExtra {
    public static void main(String[] args) throws Exception {
        Path file = Path.of("estado-teste-extra.txt");
        Files.deleteIfExists(file);

        // --- TESTE A: groupSize = 5 ---
        System.out.println("--- TESTE A: Grupo de 5 nós (precisa de 3 votos) ---");
        RaftState no5 = new RaftState(0, 1, 5, 10, 10, file, System.out::println);
        Thread.sleep(50);
        no5.tick(); // Candidato, votos={1}

        System.out.println("Voto do n2:");
        boolean r1 = no5.onVote(1, 2, 1, true);
        System.out.println("Retorno: " + r1 + " (Esperado: false, pois só tem 2 votos)");

        System.out.println("Voto do n3:");
        boolean r2 = no5.onVote(1, 3, 1, true);
        System.out.println("Retorno: " + r2 + " (Esperado: true, atingiu 3 votos e tornou-se LIDER)\n");

        // --- TESTE B: Votos duplicados não inflacionam ---
        // --- TESTE B CORRIGIDO: Repetição no groupSize=5 ---
        System.out.println("--- TESTE B: Repetição do mesmo votante (n2) em grupo de 5 ---");
        Files.deleteIfExists(file);
        RaftState no5b = new RaftState(0, 1, 5, 10, 10, file, System.out::println);
        Thread.sleep(50);
        no5b.tick(); // Candidato, votos={1}

        System.out.println("n2 vota pela 1ª vez:");
        no5b.onVote(1, 2, 1, true);
        System.out.println("Tamanho dos votos: " + no5b.votesCount() + " (Esperado: 2, ainda não é líder)");

        System.out.println("n2 vota pela 2ª vez (duplicado):");
        no5b.onVote(1, 2, 1, true);
        System.out.println("Tamanho dos votos: " + no5b.votesCount() + " (Esperado: 2, o Set ignora o duplicado)");

        System.out.println("n3 vota agora:");
        boolean r3b = no5b.onVote(1, 3, 1, true);
        System.out.println("Tamanho dos votos: " + no5b.votesCount() + " (Esperado: 3)");
        System.out.println("Retorno: " + r3b + " (Esperado: true, agora sim é líder)");

        Files.deleteIfExists(file);
    }
}