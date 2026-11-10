package raft04;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class TestBatchWire {
    public static void main(String[] args) throws Exception {
        System.out.println("=== TESTE 6: BATCH E WIRE ===");

        // --- GRUPO 1: BATCH (Round-trip e Cabeçalho) ---
        System.out.println("\n--- GRUPO 1: BATCH (Round-trip) ---");
        VectorClock vc = new VectorClock(3);
        vc.tick(0); // Simula um relógio [1,0,0]

        List<SensorRecord> regs = new ArrayList<>();
        regs.add(new SensorRecord(1, 5, "S01", 25.0, 1000L, 1, vc));
        regs.add(new SensorRecord(2, 5, "S02", 26.0, 1001L, 2, vc));

        Batch original = new Batch(5, 2, "127.0.0.1", 7002, 10, 2, 5, regs);
        String encoded = original.encode();

        // Teste 1.1: Round-trip Completo
        Batch decoded = Batch.decode(encoded, 3);
        boolean rtOk = decoded.term == 5 && decoded.leader == 2 && decoded.hb == 10
                && decoded.lastSeq == 2 && decoded.lastTerm == 5
                && decoded.records.size() == 2 && decoded.records.get(1).getSeq() == 2;
        System.out.println("1.1 Round-trip completo: " + (rtOk ? "✅ SUCESSO" : "❌ FALHOU"));

        // Teste 1.2: DecodeHeader
        Batch header = Batch.decodeHeader(encoded);
        boolean hdOk = header.term == 5 && header.leader == 2 && header.count == 2 && header.records.isEmpty();
        System.out.println("1.2 DecodeHeader (sem registos): " + (hdOk ? "✅ SUCESSO" : "❌ FALHOU"));

        // --- GRUPO 2: BATCH (Rejeições de Malformados) ---
        System.out.println("\n--- GRUPO 2: BATCH (Rejeições) ---");

        // Teste 2.1: n inconsistente (diz que tem 3, mas só tem 2)
        String badN = "LOTE;5;2;127.0.0.1;7002;10;2;5;3\n" +
                "1;5;S01;25.0;1000;1;1,0,0\n" +
                "2;5;S02;26.0;1001;2;1,0,0";
        testarErroBatch(() -> Batch.decode(badN, 3), "n inconsistente (cabeçalho=3, real=2)");

        // Teste 2.2: Tipo inválido no cabeçalho
        String badType = "LOTE;abc;2;127.0.0.1;7002;10;2;5;0";
        testarErroBatch(() -> Batch.decode(badType, 3), "Termo 'abc' no cabeçalho");

        // Teste 2.3: Registo com falta de campos
        String badRecord = "LOTE;5;2;127.0.0.1;7002;10;2;5;1\n" +
                "1;5;S01;25.0;1000"; // Faltam o L e o V
        testarErroBatch(() -> Batch.decode(badRecord, 3), "Registo incompleto (faltam campos)");

        // --- GRUPO 3: WIRE (Defaults e Isolamento) ---
        System.out.println("\n--- GRUPO 3: WIRE (Defaults e Isolamento) ---");

        // Teste 3.1: Defaults de tempo
        boolean defaultsOk = Wire.PERIODO_HB_MS == 2000 && Wire.ELEICAO_MIN_MS == 6000
                && Wire.ELEICAO_MAX_MS == 12000 && Wire.GRUPO == 3;
        System.out.println("3.1 Defaults (2000, 6000, 12000, GRUPO=3): " + (defaultsOk ? "✅ SUCESSO" : "❌ FALHOU"));

        // Teste 3.2: Isolamento
        Path isolamento = Path.of("isolar-p0-n1");
        Files.deleteIfExists(isolamento);

        boolean isoFalso = Wire.isolated(0, 1); // Não deve estar isolado
        Files.createFile(isolamento);
        boolean isoVerdadeiro = Wire.isolated(0, 1); // Agora deve estar isolado
        Files.deleteIfExists(isolamento); // Limpeza

        System.out.println("3.2 isolated() false->true: " + ((!isoFalso && isoVerdadeiro) ? "✅ SUCESSO" : "❌ FALHOU"));
    }

    private static void testarErroBatch(Runnable teste, String descricao) {
        try {
            teste.run();
            System.out.println("❌ FALHOU: " + descricao + " (não lançou exceção)");
        } catch (IllegalArgumentException e) {
            System.out.println("✅ APANHADO: " + descricao);
        }
    }
}