import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Scanner;


public class KafkaPeticiones_Thread extends Thread{

    private String broker;

    public KafkaPeticiones_Thread(String broker){
        this.broker = broker;
    }

    public void run() {
        while(true){
            try {
                Properties props = new Properties();
                props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.broker);
                props.put(ConsumerConfig.GROUP_ID_CONFIG, "central-peticiones-group");
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
                consumer.subscribe(Collections.singletonList("peticiones-riego"));

                Properties producerProps = new Properties();
                producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.broker);
                producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

                KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

                System.out.println("[WM_Central] Escuchando 'peticiones-riego'...");

                while (true) {
                    ConsumerRecords<String, String> registros = consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, String> registro : registros) {
                        String mensaje = registro.value();
                        String[] partes = mensaje.split("#");

                        if (partes.length < 2) {
                            System.out.println("[WM_Central] Petición mal formada: " + mensaje);
                            continue;
                        }

                        String id_operario = partes[0];
                        String id_estacion = partes[1];

                        System.out.println("[WM_Central] Petición de riego: operario " + id_operario + " -> estación " + id_estacion);

                        if(!WM_Central_Thread.validarOperator(id_operario)){
                            System.err.println("[WM_Central] denegado operario: " + id_operario + "estacion -> "+ id_estacion);
                            String respuesta = id_operario + "#" + id_estacion + "#DENEGADO#Operario no registrado";
                            producer.send(new ProducerRecord<>("respuestas-riego", respuesta));
                            producer.flush();

                            /* 
                            Scanner sc = new Scanner(System.in);
                            System.out.println("QUIERE REGISTRARSE (s/n):");
                            String op = sc.nextLine();

                            if(op.equals("s")){
                                System.out.println("Nombre operario: ");
                                String nombre = sc.nextLine();
                                WM_Central_Thread.registrar_operario(id_operario, nombre);
                            }
                            */
                            continue;
                        }

                        String estado = WM_Central_Thread.buscarEstadoID_BBDD(id_estacion);
                        
                        if (estado == null) {
                            String respuesta = id_operario + "#" + id_estacion + "#DENEGADO#Estación no registrada";
                            producer.send(new ProducerRecord<>("respuestas-riego", respuesta));
                            System.out.println("[WM_Central] Denegado: estación no existe.");

                        } else if (!estado.equals("DISPONIBLE")) {
                            String respuesta = id_operario + "#" + id_estacion + "#DENEGADO#Estación no disponible (" + estado + ")";
                            producer.send(new ProducerRecord<>("respuestas-riego", respuesta));
                            System.out.println("[WM_Central] Denegado: estación en estado " + estado);

                        } else {
                            String respuesta = id_operario + "#" + id_estacion + "#AUTORIZADO";
                            producer.send(new ProducerRecord<>("respuestas-riego", respuesta));

                            WM_Central_Thread.setOperarioActual(id_estacion, id_operario);

                            String orden = id_estacion + "#INICIAR_RIEGO";
                            producer.send(new ProducerRecord<>("ordenes-central", orden));

                            System.out.println("[WM_Central] Autorizado. Orden enviada a " + id_estacion);
                        }

                        producer.flush();
                        
                    }
                }
            }catch (Exception e) {
                System.err.println("[KafkaPeticiones_Thread] ERROR fatal: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
}
