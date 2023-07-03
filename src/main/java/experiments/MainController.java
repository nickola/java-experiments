// Package
package experiments;

// Java
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Properties;
import java.time.Duration;

// Spring Framework
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.beans.factory.annotation.Value;

// Kafka
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

@RestController
public class MainController {
  @Value("${kafka.bootstrap.servers}")
  private String kafkaBootstrapServers;

  @Value("${kafka.security.protocol}")
  private String kafkaSecurityProtocol;

  @Value("${kafka.sasl.mechanism}")
  private String kafkaSaslMechanism;

  @Value("${kafka.sasl.jaas.config}")
  private String kafkaSaslJaasConfig;

  @Value("${kafka.acks}")
  private String kafkaAcks;

  @Value("${kafka.serializer}")
  private String kafkaSerializer;

  @Value("${kafka.deserializer}")
  private String kafkaDeserializer;

  @Value("${kafka.topic}")
  private String kafkaTopic;

  @Value("${kafka.consumer.group}")
  private String kafkaConsumerGroup;

  private String messageTemplate = "key: %s, value: %s";

  private Properties getKafkaProperties() {
    Properties properties = new Properties();

    properties.put("bootstrap.servers", this.kafkaBootstrapServers);
    properties.put("acks", this.kafkaAcks);

    properties.put("security.protocol", this.kafkaSecurityProtocol);
    properties.put("sasl.mechanism", this.kafkaSaslMechanism);
    properties.put("sasl.jaas.config", this.kafkaSaslJaasConfig);

    properties.put("key.serializer", this.kafkaSerializer);
    properties.put("key.deserializer", this.kafkaDeserializer);
    properties.put("value.serializer", this.kafkaSerializer);
    properties.put("value.deserializer", this.kafkaDeserializer);

    properties.put("group.id", this.kafkaConsumerGroup);
    properties.put("auto.offset.reset", "earliest");

    return properties;
  }

  private boolean produceMessage(String key, String value) {
    Properties properties = this.getKafkaProperties();

    try {
      KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
      producer.send(new ProducerRecord<>(this.kafkaTopic, key, value));
      producer.close();
    }
    catch (Exception exception) {
      System.out.println("produceMessage error: " + exception.getMessage());
      return false;
    }

    return true;
  }

  private List<String> consumeMessages() {
    Properties properties = this.getKafkaProperties();
    List<String> result = new ArrayList<String>();

    try {
      KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
      consumer.subscribe(Arrays.asList(this.kafkaTopic));

      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(250));

      for (ConsumerRecord<String, String> record : records) {
        result.add(String.format(this.messageTemplate, record.key(), record.value()));
      }

      consumer.commitSync();
      consumer.unsubscribe();
    }
    catch (Exception exception) {
      System.out.println("getMessages error: " + exception.getMessage());
    }

    return result;
  }

  // Home
  @GetMapping("/")
  public String index() {
    String html = (
      "<html>" +
      "<head>" +
        "<title>Experiments</title>" +
        "<style>" +
          "html { margin: 0; padding: 0; width: 100%; height: 100%; }" +
          "body { margin: 0; padding: 0; width: 100%; height: 100%; }" +
          ".text { width: 100%; height: 100%; display: flex; flex-flow: column; justify-content: center; align-items: center; }" +
          ".title { text-transform: uppercase; }" +
          ".action { padding-top: .25em; }" +
        "</style>" +
      "</head>" +
      "<body>" +
        "<div class=\"text\">" +
          "<div class=\"title\">Java Experiments</div>" +
          "<div class=\"action\"><a href=\"/produce/key1/value1\">Produce Kafka message</div></a>" +
          "<div class=\"action\"><a href=\"/consume\">Consume Kafka messages</div></a>" +
        "</div>" +
      "</body>" +
      "</html>"
    );

    return html;
  }

  // Produce
  @GetMapping(value="/produce/{key}/{value}")
  public String produce(@PathVariable(value="key") String key, @PathVariable(value="value") String value) {
    boolean success = this.produceMessage(key, value);
    String output = success ? "Success: Message produced" : "Error: Message not produced";

    return "<pre>\n" + output + "\n</pre>";
  }

  // Consume
  @GetMapping(value="/consume")
  public String consume() {
    List<String> messages = this.consumeMessages();
    String output = "";

    if (messages.size() > 0) {
      String outputMessages = "";

      for (String message : messages) {
        outputMessages += "\n  - " + message;
      }

      output = "Messages:" + outputMessages;
    }
    else {
      output = "No messages";
    }

    return "<pre>\n" + output + "\n</pre>";
  }
}
