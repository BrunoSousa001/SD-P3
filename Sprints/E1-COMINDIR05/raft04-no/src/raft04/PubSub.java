package raft04;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public class PubSub {
    private static final Map<String, List<Consumer<String>>> subscribers = new ConcurrentHashMap<>();

    public static void subscribe(String topic, Consumer<String> callback) {
        subscribers.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>()).add(callback);
    }

    public static void publish(String topic, String message) {
        // Verifica se o nó está isolado (não implementado aqui diretamente, tratado no PartitionNode)
        List<Consumer<String>> list = subscribers.get(topic);
        if (list != null) {
            for (Consumer<String> c : list) {
                new Thread(() -> c.accept(message)).start(); // Thread para simular rede assíncrona
            }
        }
    }
}