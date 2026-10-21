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
import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumer_Thread extends Thread{

    private String broker;

    public KafkaConsumer_Thread(String broker){
        this.broker = broker;
    }

    public void run() {
        while(true){
            try{
                Properties props = new Properties();
                props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.broker);
                props.put(ConsumerConfig.GROUP_ID_CONFIG, "central-group");
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
                consumer.subscribe(Arrays.asList("datos-riego", "fin-riego"));
                
                Properties producerProps = new Properties();
                producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.broker);
                producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

                KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

                System.out.println("[WM_Central] Consumidor Kafka escuchando 'datos-riego' y 'fin-riego'...");

                while (true) {
                    ConsumerRecords<String, String> registros = consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, String> registro : registros) {
                        String mensaje = registro.value();
                        String topic = registro.topic();
                        String[] partes = mensaje.split("#");

                        if (topic.equals("datos-riego") && partes.length >= 3) {
                            String id_estacion = partes[0];
                            Double caudal = Double.parseDouble(partes[1]);
                            Double volumen = Double.parseDouble(partes[2]);
                            System.out.println("[WM_Central] " + id_estacion + " regando -> Caudal: " + caudal + " L/min, Volumen: " + volumen + " L");
                            WM_Central_Thread.updateConexion_BBDD("REGANDO", id_estacion);
                            WM_Central_Thread.actualizarCaudalVolumen(id_estacion, caudal, volumen);

                        } else if (topic.equals("fin-riego") && partes.length >= 2) {
                            String id_estacion = partes[0];
                            String volumen_total = partes[1];
                            String motivo = partes[2];

                            String id_operario = WM_Central_Thread.getOperarioActual(id_estacion);

                            if(motivo.equals("BLOQUEO")){
                                WM_Central_Thread.updateConexion_BBDD("FUERA_DE_SERVICIO", id_estacion);
                            } else {
                                WM_Central_Thread.updateConexion_BBDD("DISPONIBLE", id_estacion);
                            }

                            if(motivo.equals("NORMAL"))
                                System.out.println("[WM_Central] " + id_estacion + " ha finalizado el riego. Volumen total: " + volumen_total + " L");
                            else
                                System.out.println("[WM_Central] " + id_estacion + " ha sido bloqueada. Volumen total: " + volumen_total + " L");
                            WM_Central_Thread.setOperarioActual(id_estacion, null);
                            WM_Central_Thread.actualizarCaudalVolumen(id_estacion, 0.0, 0.0);

                            if (id_operario != null) {
                                String resumen = id_operario + "#" + id_estacion + "#" + volumen_total;
                                producer.send(new ProducerRecord<>("resumen-riego", resumen));
                                producer.flush();
                            }
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("[KafkaConsumer_Thread] ERROR fatal: " + e.getMessage());
                e.printStackTrace();
            }
        }
        
    }


    
}
