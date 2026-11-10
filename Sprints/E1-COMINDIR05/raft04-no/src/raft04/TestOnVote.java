package raft04;

import java.nio.file.Files;
import java.nio.file.Path;

public class TestOnVote {
    public static void main(String[] args) throws Exception {
        Path file = Path.of("estado-teste-voto2.txt");

        // --- PREPARAÇÃO: Criar um candidato ---
        Files.deleteIfExists(file);
        RaftState no = new RaftState(0, 1, 3, 10, 10, file, System.out::println);
        Thread.sleep(50);
        no.tick(); // Inicia candidatura. Termo=1, Votos={1}, Papel=CANDIDATO
        System.out.println("--- CANDIDATO INICIADO: Termo 1, Votos={1} ---\n");

        // CASO 1: Voto próprio (repetido)
        System.out.println("Caso 1: Voto próprio (voter=1)");
        boolean r1 = no.onVote(1, 1, 1, true);
        System.out.println("Esperado: Retorno=false, Papel=CANDIDATO (tamanho do Set continua 1)\n");

        // CASO 2: Voto recusado
        System.out.println("Caso 2: Voto recusado de n3");
        boolean r2 = no.onVote(1, 3, 1, false);
        System.out.println("Esperado: Retorno=false, Papel=CANDIDATO (o voto 'Não' é ignorado)\n");

        // CASO 3: Voto duplicado de outro nó (antes de chegar o voto que dá maioria)
        System.out.println("Caso 3: Voto duplicado (voter=2)");
        boolean r3 = no.onVote(1, 2, 1, true);
        System.out.println("Retorno: " + r3 + " (Já atinge maioria de 2! Papel passa a LIDER)\n");

        // Como já somos líder, temos de reiniciar o nó para testar o Caso 4 e 5
        Files.deleteIfExists(file);
        no = new RaftState(0, 1, 3, 10, 10, file, System.out::println);
        Thread.sleep(50);
        no.tick();

        // CASO 4: Voto repetido do mesmo nó (idempotência na contagem)
        System.out.println("Caso 4: Voto repetido de n2");
        no.onVote(1, 2, 1, true); // Já somos líder aqui, mas o Set tem {1, 2}
        System.out.println("Esperado: Não quebra nada, continua LIDER.\n");

        // Reiniciar para o caso 5
        Files.deleteIfExists(file);
        no = new RaftState(0, 1, 3, 10, 10, file, System.out::println);
        Thread.sleep(50);
        no.tick(); // Termo 1, CANDIDATO

        // CASO 5: Voto com termo superior
        System.out.println("Caso 5: Voto com termo superior (termo=2)");
        boolean r5 = no.onVote(2, 2, 1, true);
        System.out.println("Esperado: Retorno=false, Papel=SEGUIDOR, Termo=2, Votos=0");

        Files.deleteIfExists(file);
    }
}