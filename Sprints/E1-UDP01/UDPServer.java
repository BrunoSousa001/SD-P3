import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UDPServer {
    
    // 4.1 Estruturas de dados (estáticas para serem acedidas no método)
    private static List<String> listaRececao = new ArrayList<>();
    private static Map<Integer, String> estruturaTemporaria = new HashMap<>();

    public static void main(String[] args) {
        DatagramSocket aSocket = null;
        int serverPort = 6789;
        int L = 0; // Número da última mensagem aceite e entregue em ordem

        try {
            aSocket = new DatagramSocket(serverPort);
            System.out.println(">>> Servidor UDP (Entrega em Cascata) no porto " + serverPort + " <<<");
            System.out.println("Estado inicial: L = " + L + "\n");
            
            byte[] buffer = new byte[1000];
            
            while (true) {
                DatagramPacket request = new DatagramPacket(buffer, buffer.length);
                aSocket.receive(request); 
                
                String receivedText = new String(request.getData(), 0, request.getLength(), StandardCharsets.UTF_8).trim();
                System.out.println("\n[Recebido] -> \"" + receivedText + "\"");
                
                String replyText;
                int commaIndex = receivedText.indexOf(',');
                
                if (commaIndex == -1) {
                    System.out.println("  [AVISO] Mensagem mal formada. Descartada.");
                    replyText = "waitingfor," + (L + 1);
                } else {
                    String seqStr = receivedText.substring(0, commaIndex).trim();
                    try {
                        int N = Integer.parseInt(seqStr);
                        
                        // 4.2 Isolamento da lógica no novo método
                        int novoL = processDeliveredMessages(L, N, receivedText);
                        
                        // Verifica se a mensagem provocou o avanço de L
                        if (novoL == L) {
                            replyText = "waitingfor," + (L + 1);
                            System.out.println("  [RETIDA/REJEITADA] Fora de ordem. Mantém L = " + L);
                        } else {
                            L = novoL;
                            replyText = receivedText; // Comportamento de echo da mensagem atual
                            System.out.println("  [ENTREGUE] Sucesso! Novo estado L = " + L);
                        }
                        
                        // Imprimir estado conforme exigido na Tarefa 4.2
                        System.out.println("  -> L Atual: " + L);
                        System.out.println("  -> Estrutura Temporária (Chaves): " + estruturaTemporaria.keySet());
                        
                    } catch (NumberFormatException e) {
                        System.out.println("  [AVISO] Mensagem mal formada (N não numérico).");
                        replyText = "waitingfor," + (L + 1);
                    }
                }
                
                byte[] replyBytes = replyText.getBytes(StandardCharsets.UTF_8);
                DatagramPacket reply = new DatagramPacket(
                        replyBytes, replyBytes.length, request.getAddress(), request.getPort()
                );
                aSocket.send(reply);
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

    /**
     * Processes delivered messages
     * @return the last message processed in order
     */
    public static int processDeliveredMessages(int nLastMessageInOrder, int nCurrentMessage, String currentMessage) {
        List<String> entreguesNestePasso = new ArrayList<>();

        // Se é a mensagem exatamente esperada
        if (nCurrentMessage == nLastMessageInOrder + 1) {
            // 1. Entrega a mensagem atual
            listaRececao.add(currentMessage);
            entreguesNestePasso.add(currentMessage);
            nLastMessageInOrder++;

            // 2. Entrega em CASCATA das mensagens guardadas na estrutura temporária
            // Continua a verificar se a próxima mensagem já chegou antes do tempo
            while (estruturaTemporaria.containsKey(nLastMessageInOrder + 1)) {
                // Remove da temporária (não queremos duplicados nem gastar memória)
                String msgGuardada = estruturaTemporaria.remove(nLastMessageInOrder + 1);
                
                // Entrega efetivamente
                listaRececao.add(msgGuardada);
                entreguesNestePasso.add(msgGuardada);
                nLastMessageInOrder++;
            }
        } 
        // Se for uma mensagem no futuro, guarda-a temporariamente
        else if (nCurrentMessage > nLastMessageInOrder + 1) {
            estruturaTemporaria.put(nCurrentMessage, currentMessage);
        }
        // Nota: se nCurrentMessage <= nLastMessageInOrder, é um duplicado já entregue e é simplesmente ignorado no estado.

        System.out.println("  -> Mensagens entregues neste passo: " + entreguesNestePasso);
        
        return nLastMessageInOrder; // Retorna o novo L (ou o antigo se nada mudou)
    }
}