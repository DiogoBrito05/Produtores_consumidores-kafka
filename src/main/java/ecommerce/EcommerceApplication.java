package ecommerce;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;


@SpringBootApplication
public class EcommerceApplication {

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		try(var orderDispatcher = new KafkaDispatcher<Order>()) {
			try (var emailDispatcher = new KafkaDispatcher<String>()) {
				for (var i = 0; i < 10; i++) {

					var userId = UUID.randomUUID().toString();
					var orderId = UUID.randomUUID().toString();
					var amount = new BigDecimal(Math.random() * 5000 + 1);
					var order = new Order(userId, orderId, amount);
					orderDispatcher.send("ECOMMERCE_NEW_ORDER", userId, order);
					var email = "Thank you for order! We are processing your order!";
					//var emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", key, email);
					emailDispatcher.send("ECOMMERCE_SEND_EMAIL", userId, email);
				}
			}
		}

	}

}






