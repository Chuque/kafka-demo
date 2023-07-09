package com.chuque.kafkademo;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@SpringBootApplication
public class KafkaDemoApplication implements CommandLineRunner {

	private static final String TOPIC = "meu-topico";
	private static final String BOOTSTRAP_SERVERS = "localhost:9092";

	public static void main(String[] args) {
		SpringApplication.run(KafkaDemoApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		// Configura as propriedades do produtor
		Properties producerProps = new Properties();
		producerProps.put("bootstrap.servers", BOOTSTRAP_SERVERS);
		producerProps.put("key.serializer", StringSerializer.class.getName());
		producerProps.put("value.serializer", StringSerializer.class.getName());

		// Cria um produtor Kafka
		KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

		// Envia uma mensagem para o tópico
		String message = "Olá, Kafka!";
		ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC, message);
		producer.send(producerRecord);
		producer.flush();

		// Configura as propriedades do consumidor
		Properties consumerProps = new Properties();
		consumerProps.put("bootstrap.servers", BOOTSTRAP_SERVERS);
		consumerProps.put("key.deserializer", StringDeserializer.class.getName());
		consumerProps.put("value.deserializer", StringDeserializer.class.getName());
		consumerProps.put("group.id", "meu-grupo");

		// Cria um consumidor Kafka
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);

		// Inscreve o consumidor no tópico
		consumer.subscribe(Collections.singletonList(TOPIC));

		// Aguarda e imprime as mensagens recebidas
		ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(5));
		consumerRecords.forEach(consumerRecord -> System.out.println("Mensagem recebida: " + consumerRecord.value()));

		// Fecha o consumidor e o produtor
		consumer.close();
		producer.close();
	}
}
