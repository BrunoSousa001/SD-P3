package tpf;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

public final class Registo {

    private final long lsn;
    private final long instante;
    private final String autor;
    private final String canal;
    private final String texto;

    public Registo(long lsn, long instante, String autor, String canal, String texto) {
        this.lsn = lsn;
        this.instante = instante;
        this.autor = Objects.requireNonNull(autor, "autor não pode ser nulo");
        this.canal = Objects.requireNonNull(canal, "canal não pode ser nulo");
        this.texto = Objects.requireNonNull(texto, "texto não pode ser nulo");
    }

    // --- Getters ---
    public long getLsn()         { return lsn; }
    public long getInstante()    { return instante; }
    public String getAutor()     { return autor; }
    public String getCanal()     { return canal; }
    public String getTexto()     { return texto; }

    // --- Formato de texto para o ficheiro registos.log (exigido na Issue #21) ---
    // Formato: lsn;instante;autor;canal;texto_em_Base64
    public String toLogLine() {
        String textoBase64 = Base64.getEncoder().encodeToString(texto.getBytes(StandardCharsets.UTF_8));
        return lsn + ";" + instante + ";" + autor + ";" + canal + ";" + textoBase64;
    }

    public static Registo fromLogLine(String linha) {
        String[] partes = linha.split(";", 5);
        if (partes.length < 5) {
            throw new IllegalArgumentException("Linha de registo inválida: " + linha);
        }
        long lsn = Long.parseLong(partes[0].trim());
        long instante = Long.parseLong(partes[1].trim());
        String autor = partes[2].trim();
        String canal = partes[3].trim();
        String texto = new String(Base64.getDecoder().decode(partes[4].trim()), StandardCharsets.UTF_8);

        return new Registo(lsn, instante, autor, canal, texto);
    }

    // --- Formato binário para guardar dentro dos Chunks ---
    public void writeTo(DataOutputStream out) throws IOException {
        out.writeLong(lsn);
        out.writeLong(instante);
        out.writeUTF(autor);
        out.writeUTF(canal);
        out.writeUTF(texto);
    }

    public static Registo readFrom(DataInputStream in) throws IOException {
        long lsn = in.readLong();
        long instante = in.readLong();
        String autor = in.readUTF();
        String canal = in.readUTF();
        String texto = in.readUTF();
        return new Registo(lsn, instante, autor, canal, texto);
    }

    @Override
    public String toString() {
        return "Registo[LSN=" + lsn + ", autor=" + autor + ", canal=" + canal + ", texto=\"" + texto + "\"]";
    }
}