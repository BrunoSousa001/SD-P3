package raft04;

public final class VoteReply {
    public final long term;
    public final int voter;
    public final int candidate;
    public final boolean granted;

    public VoteReply(long term, int voter, int candidate, boolean granted) {
        this.term = term;
        this.voter = voter;
        this.candidate = candidate;
        this.granted = granted;
    }

    public String encode() {
        return "VOTO;" + term + ";" + voter + ";" + candidate + ";" + (granted ? "S" : "N");
    }

    public static VoteReply decode(String msg) {
        String[] f = msg.strip().split(";");

        // Validação 1: Prefixo e número de campos
        if (f.length != 5 || !f[0].equals("VOTO")) {
            throw new IllegalArgumentException("VOTO malformado: " + msg);
        }

        // Validação 2: O último campo tem de ser S ou N
        if (!f[4].equals("S") && !f[4].equals("N")) {
            throw new IllegalArgumentException("VOTO com decisão inválida (deve ser S ou N): " + msg);
        }

        try {
            // Validação 3: Tipos
            long term = Long.parseLong(f[1]);
            int voter = Integer.parseInt(f[2]);
            int candidate = Integer.parseInt(f[3]);
            boolean granted = f[4].equals("S");

            return new VoteReply(term, voter, candidate, granted);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("VOTO com números inválidos: " + msg, e);
        }
    }
}