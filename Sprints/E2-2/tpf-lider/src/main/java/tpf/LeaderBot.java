package tpf;

import net.dv8tion.jda.api.JDABuilder;
import net.dv8tion.jda.api.entities.Member;
import net.dv8tion.jda.api.events.interaction.command.SlashCommandInteractionEvent;
import net.dv8tion.jda.api.events.message.MessageReceivedEvent;
import net.dv8tion.jda.api.events.session.ReadyEvent;
import net.dv8tion.jda.api.hooks.ListenerAdapter;
import net.dv8tion.jda.api.interactions.commands.OptionType;
import net.dv8tion.jda.api.interactions.commands.build.Commands;
import net.dv8tion.jda.api.requests.GatewayIntent;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.List;
import java.util.concurrent.*;

public class LeaderBot extends ListenerAdapter {

    // --- Configuração fixa do servidor de teste ---
    private static final String GUILD_ID      = "1175518345102118952";
    private static final String CANAL_ARQUIVO = "arquivo";
    private static final String ROLE_ADMIN    = "admin-arquivo";

    private static final Path LOG_FILE     = Paths.get("registos.log");
    private static final Path EVENTOS_FILE = Paths.get("eventos-lider.txt");
    private static final Path SELADOS_DIR  = Paths.get("selados");

    // --- Estado do Líder (Escrita Local) ---
    private final byte[] masterKey;
    private long ultimoLsn = 0;
    private Chunk chunkAberto = new Chunk();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> tarefaSelagem = null;

    public LeaderBot() throws Exception {
        // 1. Carrega ou gera a master.key (32 bytes)
        this.masterKey = Wire.carregarOuGerarMasterKey();

        // 2. Garante que a pasta selados/ existe
        Files.createDirectories(SELADOS_DIR);

        // 3. Lê o último LSN de registos.log para continuar de onde parou sem saltos
        this.ultimoLsn = carregarUltimoLsn();
        System.out.println("Líder iniciado com sucesso. Último LSN retomado: " + ultimoLsn);
    }

    // ========================  ARRANQUE  ========================

    public static void main(String[] args) throws Exception {
        String token = carregarToken();
        if (token == null || token.isBlank()) {
            System.err.println("Token não configurado. Usa -Dtoken=... ou BOT_TOKEN=... no .env");
            System.exit(1);
        }

        LeaderBot bot = new LeaderBot();

        JDABuilder.createDefault(token)
                .enableIntents(GatewayIntent.MESSAGE_CONTENT, GatewayIntent.GUILD_MESSAGES)
                .addEventListeners(bot)
                .build();
    }

    /** Carrega o token por ordem: propriedade de sistema → variável de ambiente → ficheiro .env */
    private static String carregarToken() throws Exception {
        String token = System.getProperty("token");
        if (token != null) return token;

        token = System.getenv("BOT_TOKEN");
        if (token != null) return token;

        for (Path p : new Path[]{Path.of(".env"), Path.of("..", ".env"), Path.of("..", "..", ".env")}) {
            if (Files.exists(p)) {
                for (String line : Files.readAllLines(p)) {
                    if (line.startsWith("BOT_TOKEN=")) {
                        return line.substring("BOT_TOKEN=".length()).trim();
                    }
                }
            }
        }
        return null;
    }

    // =================  MOTOR DE REGISTO LOCAL (Issue #21)  =======

    /**
     * Regista duravelmente uma mensagem: LSN sequencial + fsync no registos.log + Chunk aberto.
     */
    public synchronized long registar(String autor, String canal, String texto) throws Exception {
        long lsn = ++ultimoLsn;
        long instante = System.currentTimeMillis();

        Registo registo = new Registo(lsn, instante, autor, canal, texto);

        // 1. Escreve no registos.log e força gravação no disco com fsync (FileChannel.force)
        escreverRegistoComFsync(registo);

        // 2. Regista o evento REGISTO no eventos-lider.txt
        logEvento("REGISTO", "lsn=" + lsn + ";autor=" + autor + ";canal=" + canal);

        // 3. Se for o 1.º registo no chunk aberto, agenda a selagem para daqui a 5 segundos
        if (chunkAberto.isEmpty()) {
            agendarSelagemPorTempo();
        }

        // 4. Adiciona ao chunk aberto
        boolean atingiuLimite = chunkAberto.adicionar(registo);

        // 5. Se o registo fez ultrapassar os 64 KB, sela imediatamente por tamanho
        if (atingiuLimite) {
            cancelarAgendamento();
            selarEGravarChunk();
        }

        return lsn;
    }

    private void escreverRegistoComFsync(Registo r) throws IOException {
        String linha = r.toLogLine() + "\n";
        byte[] bytes = linha.getBytes(StandardCharsets.UTF_8);

        try (FileOutputStream fos = new FileOutputStream(LOG_FILE.toFile(), true);
             FileChannel channel = fos.getChannel()) {
            fos.write(bytes);
            channel.force(true); // <--- O OBRIGATÓRIO FSYNC EXIGIDO NA ISSUE #21!
        }
    }

    private synchronized void selarEGravarChunk() {
        if (chunkAberto.isEmpty()) return;

        try {
            chunkAberto.selar();
            long deLsn = chunkAberto.getPrimeiroLsn();
            long ateLsn = chunkAberto.getUltimoLsn();

            // 1. Serializa com padding de múltiplos de 4096 bytes
            byte[] plaintext = chunkAberto.serializarComPadding();

            // 2. Cifra com HKDF + AES-256-GCM usando a master.key
            ChunkCrypto.Sealed sealed = ChunkCrypto.cifrar(plaintext, masterKey);

            // 3. Grava selados/<chunkId>.bin e selados/<chunkId>.hdr
            Path binFile = SELADOS_DIR.resolve(sealed.chunkId() + ".bin");
            Path hdrFile = SELADOS_DIR.resolve(sealed.chunkId() + ".hdr");
            Files.write(binFile, sealed.ciphertext());
            Files.writeString(hdrFile, sealed.hdrLine() + "\n", StandardCharsets.UTF_8);

            // 4. Regista o evento SELADO
            logEvento("SELADO", "chunkId=" + sealed.chunkId() + ";de=" + deLsn
                    + ";ate=" + ateLsn + ";tamanho=" + sealed.ciphertext().length);

            System.out.println("Chunk selado com sucesso: " + sealed.chunkId()
                    + " [LSN " + deLsn + ".." + ateLsn + "] (" + sealed.ciphertext().length + " bytes)");

            // 5. Abre um novo chunk vazio
            this.chunkAberto = new Chunk();

        } catch (Exception e) {
            System.err.println("Erro ao selar chunk: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void agendarSelagemPorTempo() {
        cancelarAgendamento();
        tarefaSelagem = scheduler.schedule(() -> {
            synchronized (this) {
                if (!chunkAberto.isEmpty()) {
                    selarEGravarChunk();
                }
            }
        }, Wire.SELAGEM_MS, TimeUnit.MILLISECONDS);
    }

    private void cancelarAgendamento() {
        if (tarefaSelagem != null && !tarefaSelagem.isDone()) {
            tarefaSelagem.cancel(false);
            tarefaSelagem = null;
        }
    }

    private long carregarUltimoLsn() throws IOException {
        if (!Files.exists(LOG_FILE)) return 0;
        List<String> linhas = Files.readAllLines(LOG_FILE, StandardCharsets.UTF_8);
        for (int i = linhas.size() - 1; i >= 0; i--) {
            String l = linhas.get(i).trim();
            if (!l.isBlank()) {
                try {
                    return Registo.fromLogLine(l).getLsn();
                } catch (Exception ignored) {}
            }
        }
        return 0;
    }

    private void logEvento(String evento, String detalhe) {
        String linha = System.currentTimeMillis() + ";" + evento + ";" + detalhe + "\n";
        try {
            Files.writeString(EVENTOS_FILE, linha, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException ignored) {}
    }

    // =================  REGISTO DE COMANDOS DISCORD  ============

    @Override
    public void onReady(ReadyEvent event) {
        var guild = event.getJDA().getGuildById(GUILD_ID);
        if (guild == null) {
            System.err.println("Servidor não encontrado: " + GUILD_ID);
            return;
        }

        guild.updateCommands().addCommands(
                Commands.slash("registar", "Registar um texto no arquivo")
                        .addOption(OptionType.STRING, "texto", "Texto a registar", true),

                Commands.slash("estado", "Ver o estado atual do arquivo"),

                Commands.slash("config", "Configurar o fator de replicação")
                        .addOption(OptionType.INTEGER, "r", "Novo fator de replicação", true),

                Commands.slash("recuperar", "Recuperar registos por intervalo de LSN")
                        .addOption(OptionType.INTEGER, "de", "LSN inicial (inclusive)", true)
                        .addOption(OptionType.INTEGER, "ate", "LSN final (inclusive)", true),

                Commands.slash("replica-token", "Gerar um token de registo para uma nova réplica"),

                Commands.slash("replica-remover", "Remover uma réplica registada")
                        .addOption(OptionType.STRING, "id", "Identificador da réplica a remover", true),

                Commands.slash("rajada", "Enviar uma rajada de registos de teste")
                        .addOption(OptionType.INTEGER, "n", "Número de registos", true)
                        .addOption(OptionType.INTEGER, "intervaloms", "Intervalo entre registos (ms)", true)
        ).queue(
                cmds -> System.out.println("Comandos registados no servidor: " + cmds.size()),
                err  -> System.err.println("Erro ao registar comandos: " + err.getMessage())
        );

        System.out.println("Bot online: " + event.getJDA().getSelfUser().getName());
    }

    // ================  SLASH COMMANDS  ==========================

    @Override
    public void onSlashCommandInteraction(SlashCommandInteractionEvent event) {
        String cmd = event.getName();

        // Verificação de permissão para comandos de administração
        if (cmd.equals("config") || cmd.equals("replica-token")
                || cmd.equals("replica-remover") || cmd.equals("rajada")) {

            Member member = event.getMember();
            boolean temPermissao = member != null
                    && member.getRoles().stream().anyMatch(r -> r.getName().equals(ROLE_ADMIN));

            if (!temPermissao) {
                event.reply("❌ Sem permissão. É necessário o papel `" + ROLE_ADMIN + "`.")
                        .setEphemeral(true).queue();
                return;
            }
        }

        switch (cmd) {
            case "registar" -> {
                String texto = event.getOption("texto").getAsString();
                try {
                    long lsn = registar(event.getUser().getName(), "slash", texto);
                    event.reply("Registado com LSN " + lsn).queue();
                } catch (Exception e) {
                    event.reply("❌ Erro ao registar: " + e.getMessage()).setEphemeral(true).queue();
                }
            }

            case "estado" -> event.reply("`/estado` recebido — ainda não implementado").queue();

            case "config" -> {
                long r = event.getOption("r").getAsLong();
                if (r < 1) {
                    event.reply("❌ Valor inválido: r deve ser ≥ 1").setEphemeral(true).queue();
                    return;
                }
                event.reply("`/config` recebido: r=" + r + " — ainda não implementado").queue();
            }

            case "recuperar" -> {
                long de  = event.getOption("de").getAsLong();
                long ate = event.getOption("ate").getAsLong();
                if (de < 1 || ate < 1) {
                    event.reply("❌ Valor inválido: de e ate devem ser ≥ 1").setEphemeral(true).queue();
                    return;
                }
                if (de > ate) {
                    event.reply("❌ Intervalo inválido: de=" + de + " > ate=" + ate).setEphemeral(true).queue();
                    return;
                }
                event.reply("`/recuperar` recebido: de=" + de + ", ate=" + ate + " — ainda não implementado").queue();
            }

            case "replica-token" -> event.reply("`/replica-token` recebido — ainda não implementado").queue();

            case "replica-remover" -> {
                String id = event.getOption("id").getAsString();
                event.reply("`/replica-remover` recebido: id=\"" + id + "\" — ainda não implementado").queue();
            }

            case "rajada" -> {
                long n = event.getOption("n").getAsLong();
                long intervaloMs = event.getOption("intervaloms").getAsLong();
                if (n < 1 || intervaloMs < 0) {
                    event.reply("❌ Parâmetros inválidos para rajada").setEphemeral(true).queue();
                    return;
                }

                event.reply("⚡ A iniciar rajada de " + n + " registos (intervalo=" + intervaloMs + "ms)...").queue();

                // Executa em thread separada para não bloquear a resposta do Discord
                new Thread(() -> {
                    try {
                        for (int i = 1; i <= n; i++) {
                            registar("admin", "rajada", "Registo sintético " + i);
                            if (intervaloMs > 0) Thread.sleep(intervaloMs);
                        }
                    } catch (Exception e) {
                        System.err.println("Erro na rajada: " + e.getMessage());
                    }
                }).start();
            }

            default -> event.reply("Comando desconhecido: " + cmd).queue();
        }
    }

    // ================  MENSAGENS NO CANAL #arquivo  =============

    @Override
    public void onMessageReceived(MessageReceivedEvent event) {
        if (event.getAuthor().isBot()) return;
        if (!event.getChannel().getName().equals(CANAL_ARQUIVO)) return;

        String autor = event.getAuthor().getName();
        String conteudo = event.getMessage().getContentRaw();

        try {
            long lsn = registar(autor, CANAL_ARQUIVO, conteudo);
            event.getChannel().sendMessage("Registado com LSN " + lsn).queue();
        } catch (Exception e) {
            event.getChannel().sendMessage("❌ Erro ao registar: " + e.getMessage()).queue();
        }
    }
}