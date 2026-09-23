import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

/**
 * Cliente UDP interativo com suporte a modo automático e manual de numeração.
 */
public class UDPClient {

    public static void main(String[] args) {
        DatagramSocket aSocket = null;
        Scanner scanner = new Scanner(System.in);

        try {
            // O cliente cria o socket sem porto específico (o SO atribui um porto efémero livre)
            aSocket = new DatagramSocket();
            InetAddress serverHost = InetAddress.getByName("localhost");
            int serverPort = 6789;

            System.out.println("==================================================");
            System.out.println("              CLIENTE UDP INICIADO               ");
            System.out.println("==================================================");
            System.out.println("Escolha o modo de numeração:");
            System.out.println("1 - Modo Automático (numeração sequencial 1, 2, 3...)");
            System.out.println("2 - Modo Manual (você define o N para testar desordenação)");
            System.out.print("Opção (1 ou 2): ");

            String modeChoice = scanner.nextLine().trim();
            boolean isAutoMode = !modeChoice.equals("2");

            System.out.println("\nModo selecionado: " + (isAutoMode ? "AUTOMÁTICO" : "MANUAL"));
            System.out.println("Para sair a qualquer momento, escreva: 'sair' ou 'exit'\n");

            int autoSeqNumber = 1;

            while (true) {
                int seqNumber;
                String messageText;

                if (isAutoMode) {
                    System.out.print("[Auto #" + autoSeqNumber + "] Mensagem a enviar: ");
                    messageText = scanner.nextLine();

                    if (messageText.equalsIgnoreCase("sair") || messageText.equalsIgnoreCase("exit")) {
                        break;
                    }

                    seqNumber = autoSeqNumber++;
                } else {
                    System.out.print("[Manual] Digite o número de sequência N (ou 'sair'): ");
                    String inputSeq = scanner.nextLine().trim();

                    if (inputSeq.equalsIgnoreCase("sair") || inputSeq.equalsIgnoreCase("exit")) {
                        break;
                    }

                    try {
                        seqNumber = Integer.parseInt(inputSeq);
                    } catch (NumberFormatException e) {
                        System.out.println("Erro: Deve introduzir um número inteiro válido para N!\n");
                        continue;
                    }

                    System.out.print("[Manual #" + seqNumber + "] Texto da mensagem: ");
                    messageText = scanner.nextLine();

                    if (messageText.equalsIgnoreCase("sair") || messageText.equalsIgnoreCase("exit")) {
                        break;
                    }
                }

                // Construção do payload no formato: <N>,<Mensagem>
                String packetPayload = seqNumber + "," + messageText;
                byte[] sendData = packetPayload.getBytes(StandardCharsets.UTF_8);

                // DatagramPacket de envio: dados, comprimento real dos dados, IP destino e Porto destino
                DatagramPacket request = new DatagramPacket(sendData, sendData.length, serverHost, serverPort);
                aSocket.send(request);

                // Preparação para receção da resposta
                byte[] buffer = new byte[1000];
                DatagramPacket reply = new DatagramPacket(buffer, buffer.length);

                aSocket.receive(reply);

                // IMPORTANTE (CA2): Usar getLength() para ler apenas os bytes recebidos!
                // Se usássemos 'new String(reply.getData())', leríamos o buffer de 1000 bytes
                // com lixo ou caracteres nulos residuais.
                String replyContent = new String(reply.getData(), 0, reply.getLength(), StandardCharsets.UTF_8).trim();

                // Diferenciação entre Echo normal e pedido de retransmissão waitingfor (CA3)
                if (replyContent.startsWith("waitingfor,")) {
                    String expectedSeq = replyContent.substring("waitingfor,".length()).trim();
                    System.out.println(">> [ALERTA DE DESORDENAÇÃO] O servidor rejeitou a mensagem!");
                    System.out.println(">> Resposta: " + replyContent + " (O servidor está à espera da mensagem " + expectedSeq + ")\n");
                } else {
                    System.out.println(">> [SUCESSO - ECHO RECEBIDO]");
                    System.out.println(">> Resposta do Servidor: \"" + replyContent + "\"\n");
                }
            }

            System.out.println("Cliente terminado com sucesso.");

        } catch (SocketException e) {
            System.err.println("Erro no Socket do Cliente: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("Erro de E/S no Cliente: " + e.getMessage());
        } finally {
            if (aSocket != null && !aSocket.isClosed()) {
                aSocket.close();
            }
            scanner.close();
        }
    }
}