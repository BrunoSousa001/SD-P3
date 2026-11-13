package raft04;

public class TestMessages {
    public static void main(String[] args) {
        System.out.println("=== TESTE 5: MENSAGENS DE ELEIÇÃO ===");

        // --- ROUND-TRIP VÁLIDO: VoteRequest ---
        VoteRequest req = new VoteRequest(5, 2, 10, 3);
        String reqEncoded = req.encode();
        VoteRequest reqDecoded = VoteRequest.decode(reqEncoded);
        System.out.println("VoteRequest Encode: " + reqEncoded);
        System.out.println("VoteRequest Decode ok? " + (reqDecoded.term == 5 && reqDecoded.candidate == 2 && reqDecoded.lastSeq == 10 && reqDecoded.lastTerm == 3) + "\n");

        // --- ROUND-TRIP VÁLIDO: VoteReply (S) ---
        VoteReply repS = new VoteReply(5, 3, 2, true);
        String repSEncoded = repS.encode();
        VoteReply repSDecoded = VoteReply.decode(repSEncoded);
        System.out.println("VoteReply(S) Encode: " + repSEncoded);
        System.out.println("VoteReply(S) Decode ok? " + (repSDecoded.term == 5 && repSDecoded.voter == 3 && repSDecoded.candidate == 2 && repSDecoded.granted) + "\n");

        // --- ROUND-TRIP VÁLIDO: VoteReply (N) ---
        VoteReply repN = new VoteReply(6, 1, 2, false);
        String repNEncoded = repN.encode();
        VoteReply repNDecoded = VoteReply.decode(repNEncoded);
        System.out.println("VoteReply(N) Encode: " + repNEncoded);
        System.out.println("VoteReply(N) Decode ok? " + (repNDecoded.term == 6 && repNDecoded.voter == 1 && repNDecoded.candidate == 2 && !repNDecoded.granted) + "\n");

        // --- TESTES DE REJEIÇÃO (Mensagens malformadas) ---
        System.out.println("--- TESTES DE REJEIÇÃO ---");

        testarErro(() -> VoteRequest.decode("VOTO;5;2;10;3"), "Prefixo errado no VoteRequest");
        testarErro(() -> VoteRequest.decode("PEDIDO_VOTO;5;2;10"), "Falta de campo no VoteRequest");
        testarErro(() -> VoteRequest.decode("PEDIDO_VOTO;abc;2;10;3"), "Tipo errado no VoteRequest");

        testarErro(() -> VoteReply.decode("VOTO;5;3;2;X"), "Letra inválida no VoteReply (não é S nem N)");
        testarErro(() -> VoteReply.decode("PEDIDO_VOTO;5;3;2;S"), "Prefixo errado no VoteReply");
        testarErro(() -> VoteReply.decode("VOTO;5;xyz;2;S"), "Tipo errado no VoteReply");
    }

    // Método auxiliar para apanhar as exceções e imprimir sucesso
    private static void testarErro(Runnable teste, String descricao) {
        try {
            teste.run();
            System.out.println("❌ FALHOU: " + descricao + " (não lançou exceção)");
        } catch (IllegalArgumentException e) {
            System.out.println("✅ APANHADO: " + descricao + " -> " + e.getMessage());
        }
    }
}