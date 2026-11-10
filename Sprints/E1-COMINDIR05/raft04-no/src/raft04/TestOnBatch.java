package raft04;

import java.nio.file.Files;
import java.nio.file.Path;

public class TestOnBatch {
    public static void main(String[] args) throws Exception {
        Path file = Path.of("estado-teste-onbatch.txt");
        System.out.println("=== TESTE 4: ONBATCH (RECEÇÃO DE LOTE) ===");

        // --- CASO 1: Termo Inferior ---
        Files.deleteIfExists(file);
        RaftState no1 = new RaftState(0, 1, 3, 1000, 1000, file, System.out::println);
        no1.onBatch(5, 2); // Adota o termo 5 e define n2 como líder
        boolean r1 = no1.onBatch(4, 3); // Recebe lote antigo (termo 4) do n3
        System.out.println("Caso 1 (Termo inferior): Retorno=" + r1 + " | Termo=" + no1.term() + " | Líder=n" + no1.leader());
        System.out.println("Esperado: Retorno=false | Termo=5 | Líder=n2 (ignorou o n3)\n");

        // --- CASO 2: Termo Superior ---
        Files.deleteIfExists(file);
        RaftState no2 = new RaftState(0, 1, 3, 1000, 1000, file, System.out::println);
        no2.onBatch(5, 2); // Termo 5, líder n2
        boolean r2 = no2.onBatch(6, 3); // Recebe lote do futuro (termo 6) do n3
        System.out.println("Caso 2 (Termo superior): Retorno=" + r2 + " | Termo=" + no2.term() + " | Líder=n" + no2.leader());
        System.out.println("Esperado: Retorno=true | Termo=6 | Líder=n3 (adotou o novo líder)\n");

        // --- CASO 3: Mesmo Termo (Candidato a Seguidor) ---
        Files.deleteIfExists(file);
        // Cria um nó e força-o a ser candidato
        RaftState no3 = new RaftState(0, 1, 3, 10, 10, file, System.out::println);
        Thread.sleep(50);
        no3.tick(); // Torna-se candidato no termo 1
        System.out.println("(Estado atual: CANDIDATO, Termo 1)");

        boolean r3 = no3.onBatch(1, 2); // Recebe lote do n2 no mesmo termo
        System.out.println("Caso 3 (Mesmo termo): Retorno=" + r3 + " | Papel=" + no3.role() + " | Líder=n" + no3.leader());
        System.out.println("Esperado: Retorno=true | Papel=SEGUIDOR | Líder=n2 (desistiu de ser candidato)\n");

        Files.deleteIfExists(file);
    }
}