import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
/**
 * Servidor UDP com garantia de ordenação ao nível da aplicação.
 * Porto fixo: 6789
 */
public class UDPServer {
    public static void main(String[] args) {
        DatagramSocket aSocket = null;
        int serverPort = 6789;
        // Estado mínimo do servidor:
        // L guarda o número de sequência da ÚLTIMA mensagem aceite em ordem.
        // Inicia a 0 porque nenhuma mensagem (1, 2, ...) foi ainda recebida.
        int L = 0;
        try {
            // 1. Criação do socket associado ao porto fixo 6789
            aSocket = new DatagramSocket(serverPort);
            System.out.println(">>> Servidor UDP em execução no porto " + serverPort + " <<<");
            System.out.println("Estado inicial: L = " + L + " (à espera da mensagem 1)\n");
            byte[] buffer = new byte[1000];
            while (true) {
                // Prepara o pacote para receção
                DatagramPacket request = new DatagramPacket(buffer, buffer.length);
                aSocket.receive(request); // Bloqueia até chegar um datagrama
                // Extrai APENAS os bytes efetivamente recebidos (usando getLength())
                String receivedText = new String(request.getData(), 0, request.getLength(), StandardCharsets.UTF_8).trim();
                System.out.println("[Recebido de " + request.getAddress() + ":" + request.getPort() + "] -> \"" + receivedText + "\"");
                String replyText;
                // Validação e Parsing do formato: <N>,<Mensagem>
                int commaIndex = receivedText.indexOf(',');
                if (commaIndex == -1) {
                    // Mensagem mal formada: sem vírgula (não faz crash nem altera L)
                    System.out.println("  [AVISO] Mensagem mal formada (sem vírgula). Descartada.");
                    replyText = "waitingfor," + (L + 1);
                } else {
                    String seqStr = receivedText.substring(0, commaIndex).trim();
                    try {
                        int N = Integer.parseInt(seqStr);
                        // Regra de Decisão da Aplicação:
                        // A mensagem só é aceite se for estritamente a próxima esperada: N == L + 1
                        if (N == L + 1) {
                            // Mensagem em ordem: atualiza o estado L e faz Echo da mensagem
                            L = N;
                            replyText = receivedText; // Ecoa a mensagem original
                            System.out.println("  [ACEITE] Mensagem em ordem! Novo estado: L = " + L);
                        } else {
                            // Mensagem fora de ordem (N != L + 1): rejeita e pede a que falta
                            replyText = "waitingfor," + (L + 1);
                            System.out.println("  [REJEITADA] Fora de ordem (recebido N=" + N + ", esperado " + (L + 1) + "). Mantém: L = " + L);
                        }
                    } catch (NumberFormatException e) {
                        // Mensagem mal formada: N não é um número inteiro (não faz crash)
                        System.out.println("  [AVISO] Mensagem mal formada (N não é número). Descartada.");
                        replyText = "waitingfor," + (L + 1);
                    }
                }
                // Envia a resposta de volta para o IP e Porto de quem enviou o pedido
                byte[] replyBytes = replyText.getBytes(StandardCharsets.UTF_8);
                DatagramPacket reply = new DatagramPacket(
                        replyBytes,
                        replyBytes.length,
                        request.getAddress(),
                        request.getPort()
                );
                aSocket.send(reply);
                System.out.println("  [Enviada resposta] -> \"" + replyText + "\"\n");
            }
        } catch (SocketException e) {
            System.err.println("Erro no Socket: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("Erro de E/S: " + e.getMessage());
        } finally {
            if (aSocket != null && !aSocket.isClosed()) {
                aSocket.close();
            }
        }
    }
}