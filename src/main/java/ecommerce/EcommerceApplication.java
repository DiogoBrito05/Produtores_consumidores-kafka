package ecommerce;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@SpringBootApplication
public class EcommerceApplication {

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		try(var dispatcher = new KafkaDispatcher()){
			for (var i = 0; i<10; i++) {

				var key = UUID.randomUUID().toString();
				var value = key + ",67523,1234";
				dispatcher.send("ECOMMERCE_NEW_ORDER", value, value);
				var email = "Thank you for order! We are processing your order!";
				//var emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", key, email);
				dispatcher.send("ECOMMERCE_SEND_EMAIL", key, email);
			}
		};


	}

}






