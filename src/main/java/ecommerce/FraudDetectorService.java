package ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class FraudDetectorService {


    public static void main(String[] args) {
        var frudService = new FraudDetectorService();
        var service = new KafkaService(FraudDetectorService.class.getSimpleName(),"ECOMMERCE_NEW_SERVICE",
                frudService::parse);
        service.run();
    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("------------------------------------------------");
        System.out.println("Processadno new order, checando por uma fraude");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        try{
            Thread.sleep(5000);
        }catch(InterruptedException e ){
            e.printStackTrace();
        }
        System.out.println("Ordem foi processada");
    }




    private static Properties properties(){
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudDetectorService.class.getSimpleName());
        //ID ÚNICO QUE ESTOU GERENDO NESSA LINHA
        //properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, FraudDetectorService.class.getSimpleName() + "-" + UUID.randomUUID().toString());
        return properties;
    }
}
