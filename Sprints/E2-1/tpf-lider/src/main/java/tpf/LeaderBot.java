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

import java.nio.file.Files;
import java.nio.file.Path;

public class LeaderBot extends ListenerAdapter {

    // --- Configuração fixa do servidor de teste ---
    private static final String GUILD_ID      = "1175518345102118952";
    private static final String CANAL_ARQUIVO = "arquivo";
    private static final String ROLE_ADMIN    = "admin-arquivo";

    // ========================  ARRANQUE  ========================

    public static void main(String[] args) throws Exception {
        String token = carregarToken();
        if (token == null || token.isBlank()) {
            System.err.println("Token não configurado. Usa -Dtoken=... ou BOT_TOKEN=... no .env");
            System.exit(1);
        }

        JDABuilder.createDefault(token)
                .enableIntents(GatewayIntent.MESSAGE_CONTENT, GatewayIntent.GUILD_MESSAGES)
                .addEventListeners(new LeaderBot())
                .build();
    }

    /** Carrega o token por ordem: propriedade de sistema → variável de ambiente → ficheiro .env */
    private static String carregarToken() throws Exception {
        // 1. Propriedade de sistema: java -Dtoken=...
        String token = System.getProperty("token");
        if (token != null) return token;

        // 2. Variável de ambiente
        token = System.getenv("BOT_TOKEN");
        if (token != null) return token;

        // 3. Ficheiro .env (na pasta do projeto ou na pasta-pai)
        for (Path p : new Path[]{Path.of(".env"), Path.of("..", ".env")}) {
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

    // =================  REGISTO DE COMANDOS  ====================

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

        // --- Verificação de permissão para comandos de administração ---
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

        // --- Despacho por comando ---
        switch (cmd) {
            case "registar" -> {
                String texto = event.getOption("texto").getAsString();
                event.reply("`/registar` recebido: texto=\"" + texto + "\" — ainda não implementado").queue();
            }

            case "estado" ->
                    event.reply("`/estado` recebido — ainda não implementado").queue();

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

            case "replica-token" ->
                    event.reply("`/replica-token` recebido — ainda não implementado").queue();

            case "replica-remover" -> {
                String id = event.getOption("id").getAsString();
                event.reply("`/replica-remover` recebido: id=\"" + id + "\" — ainda não implementado").queue();
            }

            case "rajada" -> {
                long n           = event.getOption("n").getAsLong();
                long intervaloMs = event.getOption("intervaloms").getAsLong();
                if (n < 1) {
                    event.reply("❌ Valor inválido: n deve ser ≥ 1").setEphemeral(true).queue();
                    return;
                }
                if (intervaloMs < 0) {
                    event.reply("❌ Valor inválido: intervaloMs deve ser ≥ 0").setEphemeral(true).queue();
                    return;
                }
                event.reply("`/rajada` recebido: n=" + n + ", intervaloMs=" + intervaloMs
                        + " — ainda não implementado").queue();
            }

            default -> event.reply("Comando desconhecido: " + cmd).queue();
        }
    }

    // ================  MENSAGENS NO CANAL #arquivo  =============

    @Override
    public void onMessageReceived(MessageReceivedEvent event) {
        // Ignorar mensagens de bots (incluindo as do próprio bot)
        if (event.getAuthor().isBot()) return;

        // Ignorar mensagens que não sejam no canal #arquivo
        if (!event.getChannel().getName().equals(CANAL_ARQUIVO)) return;

        String autor    = event.getAuthor().getName();
        String conteudo = event.getMessage().getContentRaw();

        event.getChannel().sendMessage(
                "Mensagem detetada de **" + autor + "**: \"" + conteudo + "\" — registo provisório"
        ).queue();

        System.out.println("MENSAGEM em #" + CANAL_ARQUIVO + ": autor=" + autor + " texto=" + conteudo);
    }
}
