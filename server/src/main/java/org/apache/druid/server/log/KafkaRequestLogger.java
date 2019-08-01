package org.apache.druid.server.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.RequestLogLine;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

public class KafkaRequestLogger implements RequestLogger{

  private static final Logger LOG = new Logger(KafkaRequestLogger.class);

  private final ObjectMapper mapper;
  private final String topic;
  private final String bootstrapServers;
  private KafkaSinker kafkaSinker;

  public KafkaRequestLogger(
          ObjectMapper mapper,
          String topic,
          String bootstrapServers
  )
  {
    this.mapper = mapper;
    this.topic = topic;
    this.bootstrapServers = bootstrapServers;
  }

  @Override
  public void logNativeQuery(RequestLogLine requestLogLine) throws IOException
  {
    try {
      KafkaMessageJson kafkaMessageJson = new KafkaMessageJson(requestLogLine, mapper);
      kafkaSinker.send(kafkaMessageJson.getQueryId(), kafkaMessageJson.toString());
    } catch (RuntimeException re) {
      LOG.error(re, "Error sending native druid job audit json to kafka");
    }
  }

  @Override
  public void logSqlQuery(RequestLogLine requestLogLine) throws IOException
  {
    try {
      KafkaMessageJsonForSQL kafkaMessageJsonForSQL = new KafkaMessageJsonForSQL(requestLogLine);
      kafkaSinker.send(kafkaMessageJsonForSQL.getQueryId(), kafkaMessageJsonForSQL.toString());
    } catch (RuntimeException re) {
      LOG.error(re, "Error sending sql druid job audit json to kafka");
    }
  }

  @LifecycleStart
  @Override
  public void start()
  {
    try {
      LOG.info("broker.list: %s, sinker.topic: %s", bootstrapServers, topic);
      kafkaSinker = new KafkaSinker(bootstrapServers, topic);
    } catch (Exception e) {
      LOG.error("%s", e);
    }
  }

  @Override
  public String toString()
  {
    return "KafkaRequestLogger{" +
            "topic=" + topic +
            ", bootstrapServers='" + bootstrapServers + '\'' +
            '}';
  }


  public class KafkaSinker
  {
    private KafkaProducer<String, String> producer;
    private String brokerlist;
    private String topic;

    public void init()
    {
      if (null == producer) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", brokerlist);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(properties);
      }
    }

    public KafkaSinker(String brokerlist, String topic) throws Exception
    {
      if (brokerlist == null || topic == null) {
        throw new Exception("brokerlist and topic cannot be null");
      }

      this.brokerlist = brokerlist;
      this.topic = topic;
      init();
    }

    public void send(String key, String message)
    {
      if (key == null || message.length() == 0) {
        return;
      }

      ProducerRecord<String, String> producerRecord =
              new ProducerRecord<>(topic, key, message);
      producer.send(producerRecord);
      new Thread(() -> flush()).start();
    }

    public void flush()
    {
      producer.flush();
    }
  }

}