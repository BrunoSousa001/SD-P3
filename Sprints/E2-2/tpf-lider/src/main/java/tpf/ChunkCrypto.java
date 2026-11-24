package tpf;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;

public final class ChunkCrypto {

    public static final int KEY_VERSION = 1;
    public static final int SALT_BYTES  = 32;
    public static final int NONCE_BYTES = 12;
    public static final int TAG_BITS    = 128; // 16 bytes

    private ChunkCrypto() {}

    /**
     * Representa o resultado da selagem criptográfica de um chunk.
     */
    public record Sealed(String chunkId, byte[] ciphertext, String hdrLine) {}

    /**
     * Cifra os dados de um chunk com padding usando HKDF-SHA256 + AES-256-GCM.
     */
    public static Sealed cifrar(byte[] plaintextComPadding, byte[] masterKey) throws Exception {
        SecureRandom rng = new SecureRandom();

        // 1. Gera salt (32 bytes) e nonce (12 bytes) aleatórios e únicos para este chunk
        byte[] salt = new byte[SALT_BYTES];
        byte[] nonce = new byte[NONCE_BYTES];
        rng.nextBytes(salt);
        rng.nextBytes(nonce);

        // 2. Deriva a chave-filha DEK (32 bytes) usando HKDF-SHA256 com info = "tpf-chunk-v1"
        byte[] dek = hkdfSha256(masterKey, salt, "tpf-chunk-v1".getBytes(StandardCharsets.UTF_8), 32);

        // 3. Constrói os AAD (Additional Authenticated Data): "tpf-v1" + keyVersion (4 bytes) + salt + nonce
        byte[] aad = construirAad(KEY_VERSION, salt, nonce);

        // 4. Cifra com AES-256-GCM
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        SecretKeySpec keySpec = new SecretKeySpec(dek, "AES");
        GCMParameterSpec gcmSpec = new GCMParameterSpec(TAG_BITS, nonce);
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, gcmSpec);
        cipher.updateAAD(aad);
        byte[] ciphertext = cipher.doFinal(plaintextComPadding);

        // 5. chunkId = SHA-256 em hexadecimal do texto cifrado (incluindo a tag)
        String chunkId = sha256Hex(ciphertext);

        // 6. Linha do ficheiro de cabeçalho (.hdr): keyVersion;salt;nonce em Base64
        String saltB64 = Base64.getEncoder().encodeToString(salt);
        String nonceB64 = Base64.getEncoder().encodeToString(nonce);
        String hdrLine = KEY_VERSION + ";" + saltB64 + ";" + nonceB64;

        return new Sealed(chunkId, ciphertext, hdrLine);
    }

    /**
     * Abre (decifra) um chunk selado.
     * Verifica por ordem: 1. Hash contra o id; 2. Versão da chave; 3. Autenticação GCM.
     */
    public static byte[] open(String chunkId, byte[] ciphertext, String hdrLine, byte[] masterKey) throws Exception {
        // 1. Verifica integridade do hash do ficheiro
        String hashCalculado = sha256Hex(ciphertext);
        if (!hashCalculado.equalsIgnoreCase(chunkId)) {
            throw new SecurityException("Adulteração detetada: o SHA-256 do ficheiro não coincide com o chunkId!");
        }

        // 2. Lê e valida o cabeçalho (.hdr)
        String[] partes = hdrLine.trim().split(";");
        if (partes.length < 3) {
            throw new IllegalArgumentException("Cabeçalho (.hdr) inválido: " + hdrLine);
        }
        int keyVersion = Integer.parseInt(partes[0].trim());
        if (keyVersion != KEY_VERSION) {
            throw new SecurityException("Versão da chave desconhecida: " + keyVersion);
        }

        byte[] salt = Base64.getDecoder().decode(partes[1].trim());
        byte[] nonce = Base64.getDecoder().decode(partes[2].trim());

        // 3. Deriva a mesma DEK a partir da master.key e do salt
        byte[] dek = hkdfSha256(masterKey, salt, "tpf-chunk-v1".getBytes(StandardCharsets.UTF_8), 32);

        // 4. Reconstrói os AAD
        byte[] aad = construirAad(keyVersion, salt, nonce);

        // 5. Decifra com AES-256-GCM (o GCM valida a tag de autenticação)
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        SecretKeySpec keySpec = new SecretKeySpec(dek, "AES");
        GCMParameterSpec gcmSpec = new GCMParameterSpec(TAG_BITS, nonce);
        cipher.init(Cipher.DECRYPT_MODE, keySpec, gcmSpec);
        cipher.updateAAD(aad);

        // Se algum byte foi alterado, doFinal lança AEADBadTagException
        return cipher.doFinal(ciphertext);
    }

    // --- Métodos Criptográficos Auxiliares (HKDF e SHA-256) ---

    private static byte[] construirAad(int keyVersion, byte[] salt, byte[] nonce) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write("tpf-v1".getBytes(StandardCharsets.UTF_8));
        baos.write(ByteBuffer.allocate(4).putInt(keyVersion).array());
        baos.write(salt);
        baos.write(nonce);
        return baos.toByteArray();
    }

    /**
     * Implementação standard de HKDF-SHA256 (RFC 5869) para extração e expansão de 1 bloco (32 bytes).
     */
    public static byte[] hkdfSha256(byte[] ikm, byte[] salt, byte[] info, int length) throws Exception {
        // Step 1: Extract (PRK = HMAC-SHA256(salt, IKM))
        Mac mac = Mac.getInstance("HmacSHA256");
        SecretKeySpec saltKey = new SecretKeySpec(salt, "HmacSHA256");
        mac.init(saltKey);
        byte[] prk = mac.doFinal(ikm);

        // Step 2: Expand (OKM = HMAC-SHA256(PRK, info || 0x01))
        mac.init(new SecretKeySpec(prk, "HmacSHA256"));
        mac.update(info);
        mac.update((byte) 0x01);
        byte[] okm = mac.doFinal();

        return Arrays.copyOf(okm, length);
    }

    public static String sha256Hex(byte[] dados) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(dados);
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}