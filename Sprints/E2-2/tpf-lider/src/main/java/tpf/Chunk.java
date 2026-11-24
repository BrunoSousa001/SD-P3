package tpf;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class Chunk {

    private final List<Registo> registos = new ArrayList<>();
    private long primeiroInstante = 0;
    private int tamanhoBytesEstimado = 4; // Começa com 4 bytes para guardar o contador de registos
    private boolean selado = false;

    public Chunk() {}

    /**
     * Adiciona um registo ao chunk.
     * Devolve true se este registo fez o chunk atingir ou ultrapassar o limite de 64 KB (devendo ser selado).
     */
    public synchronized boolean adicionar(Registo r) {
        if (selado) {
            throw new IllegalStateException("O chunk já foi selado e é imutável.");
        }

        if (registos.isEmpty()) {
            primeiroInstante = System.currentTimeMillis();
        }

        registos.add(r);

        // Estimativa do tamanho em bytes deste registo no formato binário
        int bytesDesteRegisto = 8 + 8 + (2 + r.getAutor().length() * 2)
                + (2 + r.getCanal().length() * 2)
                + (2 + r.getTexto().length() * 3);
        tamanhoBytesEstimado += bytesDesteRegisto;

        // Se ultrapassou os 64 KB, marca para selar imediatamente
        if (tamanhoBytesEstimado >= Wire.CHUNK_MAX_BYTES) {
            selar();
            return true;
        }

        return false;
    }

    public synchronized void selar() {
        this.selado = true;
    }

    public synchronized boolean isSelado()          { return selado; }
    public synchronized boolean isEmpty()           { return registos.isEmpty(); }
    public synchronized int getQuantidade()         { return registos.size(); }
    public synchronized long getPrimeiroInstante()  { return primeiroInstante; }
    public synchronized List<Registo> getRegistos() { return Collections.unmodifiableList(new ArrayList<>(registos)); }

    public synchronized long getPrimeiroLsn() {
        return registos.isEmpty() ? 0 : registos.get(0).getLsn();
    }

    public synchronized long getUltimoLsn() {
        return registos.isEmpty() ? 0 : registos.get(registos.size() - 1).getLsn();
    }

    /**
     * Converte os registos em bytes e adiciona padding de zeros até ser múltiplo de PAD_BYTES (4096).
     */
    public synchronized byte[] serializarComPadding() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(baos)) {
            // 1. Escreve a quantidade de registos que estão neste chunk
            out.writeInt(registos.size());
            // 2. Escreve cada um dos registos
            for (Registo r : registos) {
                r.writeTo(out);
            }
        }

        byte[] payload = baos.toByteArray();
        int tamanhoAtual = payload.length;

        // 3. Calcula quanto padding (zeros) é preciso adicionar para atingir múltiplo de 4096 bytes
        int resto = tamanhoAtual % Wire.PAD_BYTES;
        int paddingLen = (resto == 0) ? 0 : (Wire.PAD_BYTES - resto);

        byte[] resultadoComPadding = new byte[tamanhoAtual + paddingLen];
        System.arraycopy(payload, 0, resultadoComPadding, 0, tamanhoAtual);
        // O resto do array fica automaticamente preenchido com zeros (padding)

        return resultadoComPadding;
    }

    /**
     * Recontrói a lista de registos ignorando o padding no final.
     */
    public static List<Registo> desserializar(byte[] dadosComPadding) throws IOException {
        List<Registo> lista = new ArrayList<>();
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(dadosComPadding))) {
            int totalRegistos = in.readInt();
            for (int i = 0; i < totalRegistos; i++) {
                lista.add(Registo.readFrom(in));
            }
        }
        return lista;
    }
}