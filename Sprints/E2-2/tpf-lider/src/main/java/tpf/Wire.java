package tpf;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;

public final class Wire {

    // --- Constantes exigidas pela especificação (Issue #21) ---
    public static final int CHUNK_MAX_BYTES = Integer.getInteger("tpf.chunkMaxBytes", 65536); // 64 KB
    public static final int PAD_BYTES       = Integer.getInteger("tpf.padBytes", 4096);       // 4 KB
    public static final long SELAGEM_MS     = Long.getLong("tpf.selagemMs", 5000L);           // 5 segundos

    public static final String MASTER_KEY_FILE = "master.key";
    public static final int KEY_SIZE_BYTES     = 32; // 256 bits

    private Wire() {} // Classe utilitária, não se instancia

    /**
     * Carrega a master.key do disco. Se não existir, gera uma chave nova de 32 bytes.
     * Recusa o arranque se o ficheiro existir mas não tiver exatamente 32 bytes.
     */
    public static byte[] carregarOuGerarMasterKey() throws IOException {
        Path path = Paths.get(MASTER_KEY_FILE);

        if (Files.exists(path)) {
            byte[] key = Files.readAllBytes(path);
            if (key.length != KEY_SIZE_BYTES) {
                throw new IllegalStateException("master.key inválida: tem " + key.length
                        + " bytes, mas são exigidos exatamente " + KEY_SIZE_BYTES + " bytes!");
            }
            return key;
        }

        // Se não existir, gera 32 bytes criptograficamente seguros
        byte[] key = new byte[KEY_SIZE_BYTES];
        new SecureRandom().nextBytes(key);
        Files.write(path, key);

        // Restringe permissões de leitura só para o dono (requisito de segurança)
        File f = path.toFile();
        f.setReadable(false, false);
        f.setReadable(true, true);
        f.setWritable(false, false);
        f.setWritable(true, true);

        System.out.println("Nova master.key gerada com sucesso (32 bytes).");
        return key;
    }
}