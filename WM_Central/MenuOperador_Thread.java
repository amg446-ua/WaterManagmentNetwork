import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Scanner;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class MenuOperador_Thread extends Thread {
    
    private String broker;

    public MenuOperador_Thread(String broker){
        this.broker = broker;
    }

    public void run(){
        while (true) {
            try{
                Properties producerProp = new Properties();
                producerProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.broker);
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
                            // Obtenemos todas las estaciones de la BBDD que pueden ser activadas
                            List<String> estacionesValidas = WM_Central_Thread.obtenerEstacionesActivables(); // O la lógica que tengas para listar IDs

                            if (estacionesValidas == null || estacionesValidas.isEmpty()) {
                                System.out.println("[WM_Central] No hay estaciones disponibles para activar.");
                            } else {
                                int totalEstaciones = estacionesValidas.size();
                                CountDownLatch latchMasivo = new CountDownLatch(totalEstaciones);

                                // Registramos el latch para cada estación individual en el mapa compartido
                                for (String estId : estacionesValidas) {
                                    WM_Central_Thread.estacionesPendientesActivacion.put(estId, latchMasivo);
                                }

                                String mensaje = "TODAS#ACTIVAR";
                                producer.send(new ProducerRecord<>("ordenes-central", mensaje));
                                producer.flush();
                                System.out.println("[WM_Central] Orden de activación enviada a TODAS las estaciones");

                                try {
                                    // Esperamos a que TODAS respondan o salte el timeout global (ej. 5 o 6 segundos para dar margen)
                                    boolean recibidoTodo = latchMasivo.await(6, TimeUnit.SECONDS);

                                    if (!recibidoTodo) {
                                        System.out.println("[WM_Central] ADVERTENCIA: Algunas estaciones no respondieron a la orden de activación a tiempo.");
                                    } else {
                                        System.out.println("[WM_Central] Todas las estaciones (" + totalEstaciones + ") han respondido y están activas correctamente.");
                                    }
                                } catch (InterruptedException e) {
                                    System.err.println("[WM_Central] Espera masiva interrumpida.");
                                    Thread.currentThread().interrupt();
                                } finally {
                                    // Limpiamos todas del mapa
                                    for (String estId : estacionesValidas) {
                                        WM_Central_Thread.estacionesPendientesActivacion.remove(estId);
                                    }
                                }
                            }
                        } else if (WM_Central_Thread.buscarEstadoID_BBDD(id) == null) {
                            System.out.println("[WM_Central] ERROR: la estación " + id + " no existe en la BBDD.");

                        } else if(WM_Central_Thread.buscarEstadoID_BBDD(id).equals("DESACTIVADA")){
                            System.out.println("[WM_Central] ERROR: la estación " + id + " está DESACTIVADA");

                        } else {

                            // Creamos y registramos el Latch de sincronización para esta estación específica
                            CountDownLatch latch = new CountDownLatch(1);
                            WM_Central_Thread.estacionesPendientesActivacion.put(id, latch);

                            String mensaje = id + "#ACTIVAR";
                            producer.send(new ProducerRecord<>("ordenes-central", mensaje));
                            producer.flush();
                            System.out.println("[WM_Central] Orden de activación enviada a " + id);

                            try {
                                //Esperamos la confirmación por socket con un timeout de 4 segundos
                                boolean recibido = latch.await(4, TimeUnit.SECONDS);

                                if (!recibido) {
                                    System.out.println("[WM_Central] ERROR: la estación " + id + " no respondió a la orden de activación. Puede estar desconectada.");
                                } else {
                                    System.out.println("[WM_Central] Estación " + id + " confirmada y activa correctamente.");
                                }
                            } catch (InterruptedException e) {
                                System.err.println("[WM_Central] Espera interrumpida para la estación " + id);
                                Thread.currentThread().interrupt();
                            } finally {
                                // Limpiamos el mapa para evitar fugas de memoria
                                WM_Central_Thread.estacionesPendientesActivacion.remove(id);
                            }
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
            } catch (Exception e) {
                System.err.println("[MenuOperador_Thread] ERROR fatal: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

}
