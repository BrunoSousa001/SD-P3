package raft04;

import java.nio.file.Files;
import java.nio.file.Path;

public class TestRaftState {
    public static void main(String[] args) throws Exception {
        System.out.println("=== TESTE 1: CRIAÇÃO E LEITURA DO ESTADO ===");

        Path file1 = Path.of("estado-teste1.txt");
        Files.deleteIfExists(file1);
        RaftState no1 = new RaftState(0, 1, 3, 10000, 10000, file1, System.out::println);

        System.out.println("1. Criação (Esperado 0;0): " + Files.readString(file1).trim());

        Files.deleteIfExists(file1);
        Files.writeString(file1, "5;2\n");
        RaftState no2 = new RaftState(0, 1, 3, 10000, 10000, file1, System.out::println);
        System.out.println("2. Leitura (Esperado 5, 2): Termo=" + no2.term() + ", VotouEm=" + no2.votedFor() + "\n");

        Files.deleteIfExists(file1);
    }
}