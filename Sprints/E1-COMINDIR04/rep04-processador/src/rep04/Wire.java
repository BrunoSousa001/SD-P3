package rep04;

public final class Wire {
    public static final int LIMITE_RETIDOS = 10;   // acima deste número de retidos, a réplica recupera por TCP
    public static final int T_TCP_MS = 5000;       // tempo máximo (timeout) de 5s para ligação e leitura TCP
    public static final int PORTO_BASE = 7000;     // porto de recuperação do líder da partição k: 7000 + k
    public static final double LIMIAR_ALARME = 28.0; // temperatura a partir da qual o processador gera um alarme

    // Parâmetros do detetor de falhas (ajustáveis por propriedades de sistema: -Drep04.periodo=... -Drep04.fator=...)
    public static final long PERIODO_HB_MS = Long.getLong("rep04.periodo", 5000L);    // Δ: período do heartbeat/lote
    public static final int FATOR_TIMEOUT = Integer.getInteger("rep04.fator", 3);     // TIMEOUT = FATOR_TIMEOUT × Δ
    public static final long VERIFICACAO_MS = 1000L;                                  // período da thread de verificação

    private Wire() { } // Construtor privado para impedir instanciar esta classe utilitária

    // Tópicos IPFS:
    public static String topicIn(int k)   { return "rep04-in-p" + k; } // Tópico de entrada onde o produtor/processador envia ao líder
    public static String topicData(int k) { return "rep04-p" + k; }    // Tópico de dados onde o líder publica os LOTE

    // Mensagem de entrada: "IN;S01;22.5;<L>;<V>"  (inalterada)
    public static String in(String sensor, double temp, long lamport, VectorClock v) {
        return "IN;" + sensor + ";" + temp + ";" + lamport + ";" + v;
    }

    // Extrai o tipo da mensagem ("IN" ou "LOTE")
    public static String type(String msg) {
        String m = msg.trim();
        int i = m.indexOf(';');
        if (i <= 0) {
            throw new IllegalArgumentException("Mensagem sem tipo: " + msg);
        }
        return m.substring(0, i);
    }

    // Conteúdo da mensagem sem o tipo
    public static String body(String msg) {
        String m = msg.trim();
        return m.substring(m.indexOf(';') + 1);
    }
}
