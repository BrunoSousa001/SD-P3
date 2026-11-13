package raft04;

public final class VoteRequest {
    public final long term;
    public final int candidate;
    public final long lastSeq;
    public final long lastTerm;

    public VoteRequest(long term, int candidate, long lastSeq, long lastTerm) {
        this.term = term;
        this.candidate = candidate;
        this.lastSeq = lastSeq;
        this.lastTerm = lastTerm;
    }

    public String encode() {
        return "PEDIDO_VOTO;" + term + ";" + candidate + ";" + lastSeq + ";" + lastTerm;
    }

    public static VoteRequest decode(String msg) {
        String[] f = msg.strip().split(";");

        // Validação 1: Prefixo e número de campos
        if (f.length != 5 || !f[0].equals("PEDIDO_VOTO")) {
            throw new IllegalArgumentException("PEDIDO_VOTO malformado: " + msg);
        }

        try {
            // Validação 2: Tipos (conversão)
            long term = Long.parseLong(f[1]);
            int candidate = Integer.parseInt(f[2]);
            long lastSeq = Long.parseLong(f[3]);
            long lastTerm = Long.parseLong(f[4]);

            return new VoteRequest(term, candidate, lastSeq, lastTerm);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("PEDIDO_VOTO com números inválidos: " + msg, e);
        }
    }
}