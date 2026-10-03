package tcp01;

import java.io.*;
import java.net.*;

public class TCPClient {
    public static void main(String[] args) {
        Socket s = null;
        try {
            int serverPort = 7896;
            s = new Socket("localhost", serverPort);

            // Stream para enviar objetos
            ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());

            // Stream para ler a resposta textual
            DataInputStream in = new DataInputStream(s.getInputStream());


            // Cria o Place e a Person
            Place place = new Place("3500-000", "Viseu");
            // Cria e envia a Person
            Person p = new Person("Ana", place, 30);

            System.out.println("Initials: " + p.getInitials());

            out.writeObject(p);
            out.flush();

            // Lê a resposta (nome da pessoa)
            String data = in.readUTF();
            System.out.println("Received: " + data);

        } catch (UnknownHostException e) {
            System.out.println("Sock: " + e.getMessage());
        } catch (EOFException e) {
            System.out.println("EOF: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        } finally {
            if (s != null) {
                try {
                    s.close();
                } catch (IOException e) {
                    System.out.println("close: " + e.getMessage());
                }
            }
        }
    }
}