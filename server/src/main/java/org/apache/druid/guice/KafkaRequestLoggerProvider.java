package org.apache.druid.guice;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.log.KafkaRequestLogger;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.server.log.RequestLoggerProvider;

/**
 */
@JsonTypeName("kafka")
public class KafkaRequestLoggerProvider implements RequestLoggerProvider {

  private static final Logger log = new Logger(KafkaRequestLoggerProvider.class);

  @JacksonInject
  @Json
  public ObjectMapper mapper;

  @JsonProperty
  public String topic = null;

  @JsonProperty
  public String bootstrapServers = null;

  @Override
  public RequestLogger get()
  {
    KafkaRequestLogger logger = new KafkaRequestLogger(mapper, topic, bootstrapServers);
    log.debug(new Exception("Stack trace"), "Creating %s at", logger);
    return logger;
  }

}
