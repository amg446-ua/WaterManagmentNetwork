import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Scanner;
import org.apache.kafka.clients.producer.ProducerRecord;

public class MenuOperador_Thread extends Thread {
    
    public void run(){
        Properties producerProp = new Properties();
        producerProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProp);

        Scanner sc = new Scanner(System.in);

        while(true){
            System.out.println("\n--- MENÚ CENTRAL ---");
            System.out.println("1) Bloquear una WS");
            System.out.println("2) Activar una WS");
            System.out.println("3) Iniciar riego en una WS");
            System.out.print("Opción: ");
            String opcion = sc.nextLine();

            if(opcion.equals("1")){
                System.out.println("ID de la estación a bloquear (o 'TODAS' para todas): ");
                String id = sc.nextLine();
                
                if(id.equals("TODAS")){
                    String mensaje = "TODAS#BLOQUEAR";
                    producer.send(new ProducerRecord<>("ordenes-central", mensaje));
                    producer.flush();
                    System.out.println("[WM_Central] Orden de bloqueo enviada a TODAS las estaciones");

                } else if (WM_Central_Thread.buscarEstadoID_BBDD(id) == null) {
                    System.out.println("[WM_Central] ERROR: la estación " + id + " no existe en la BBDD");

                } else if(WM_Central_Thread.buscarEstadoID_BBDD(id).equals("DESACTIVADA")){
                    System.out.println("[WM_Central] ERROR: la estación " + id + " está DESACTIVADA");

                } else {
                    String mensaje = id + "#BLOQUEAR";
                    producer.send(new ProducerRecord<>("ordenes-central", mensaje));
                    producer.flush();
                    System.out.println("[WM_Central] Orden de bloqueo enviada a " + id);
                }
            }
            else if(opcion.equals("2")){
                System.out.println("ID de la estación a activar (o 'TODAS' para todas): ");
                String id = sc.nextLine();

                if(id.equals("TODAS")){
                    String mensaje = "TODAS#ACTIVAR";
                    producer.send(new ProducerRecord<>("ordenes-central", mensaje));
                    producer.flush();
                    System.out.println("[WM_Central] Orden de activación enviada a TODAS las estaciones");

                } else if (WM_Central_Thread.buscarEstadoID_BBDD(id) == null) {
                    System.out.println("[WM_Central] ERROR: la estación " + id + " no existe en la BBDD.");

                } else if(WM_Central_Thread.buscarEstadoID_BBDD(id).equals("DESACTIVADA")){
                    System.out.println("[WM_Central] ERROR: la estación " + id + " está DESACTIVADA");

                } else {
                    String mensaje = id + "#ACTIVAR";
                    producer.send(new ProducerRecord<>("ordenes-central", mensaje));
                    producer.flush();
                    System.out.println("[WM_Central] Orden de activación enviada a " + id);
                }
            }
            else if(opcion.equals("3")){
                System.out.println("ID de la estación para ordenar riego: ");
                String id = sc.nextLine();

                if(id.equals("TODAS")){
                    String mensaje = "TODAS#INICIAR_RIEGO";
                    producer.send(new ProducerRecord<>("ordenes-central", mensaje));
                    producer.flush();
                    System.out.println("[WM_Central] Orden de iniciar riego enviada a TODAS las estaciones");

                } else if (WM_Central_Thread.buscarEstadoID_BBDD(id) == null) {
                    System.out.println("[WM_Central] ERROR: la estación " + id + " no existe en la BBDD.");
                } else if(WM_Central_Thread.buscarEstadoID_BBDD(id).equals("DESACTIVADA")){
                    System.out.println("[WM_Central] ERROR: la estación " + id + " está DESACTIVADA");
                } else {
                    String mensaje = id + "#INICIAR_RIEGO";
                    producer.send(new ProducerRecord<>("ordenes-central", mensaje));
                    producer.flush();
                    System.out.println("[WM_Central] Orden de inicio de riego enviada a " + id);
                }
            }
            else{
                System.out.println("Opción no reconocida");
            }
        }
    }
}
