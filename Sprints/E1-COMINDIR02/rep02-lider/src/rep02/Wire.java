package rep02;

public final class Wire {
    public static final int LIMITE_RETIDOS = 10;   // acima deste número de retidos, a réplica recupera por TCP
    public static final int T_TCP_MS = 5000;       // tempo máximo (timeout) de 5s para ligação e leitura TCP
    public static final int PORTO_BASE = 7000;     // porto de recuperação do líder da partição k: 7000 + k

    private Wire() { } // Construtor privado para impedir instanciar esta classe utilitária

    // Tópicos IPFS:
    public static String topicIn(int k)   { return "rep02-in-p" + k; } // Tópico de entrada onde o produtor envia ao líder
    public static String topicData(int k) { return "rep02-p" + k; }    // Tópico de dados onde o líder propaga às réplicas

    // Mensagem de entrada: "IN;S01;22.5"
    public static String in(String sensor, double temp) { 
        return "IN;" + sensor + ";" + temp; 
    }

    // Mensagem de propagação: "REG;<endereço>;<porto>;<seq>;<sensor>;<temperatura>;<instante>"
    // O líder cola o seu IP e Porto TCP para a réplica saber a quem ligar na recuperação!
    public static String reg(String host, int port, SensorRecord r) {
        return "REG;" + host + ";" + port + ";" + r.toLine();
    }

    // Extrai o tipo da mensagem ("IN" ou "REG")
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

    // Divide uma mensagem REG em { endereço, porto, linha_do_registo }
    public static String[] regParts(String msg) {
        String[] f = msg.trim().split(";", 4);
        if (f.length != 4 || !f[0].equals("REG")) {
            throw new IllegalArgumentException("REG mal formado: " + msg);
        }
        return new String[] { f[1], f[2], f[3] };
    }
}
