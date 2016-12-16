package me.cmath.streamingestor;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;


public class StreamServer extends Thread {
  private int _maxTuples;
  private int _duration;
  private BufferedReader _sourceBuffer;
  private BufferedOutputStream _output;
  private String _kafkaServer;
  private RateLimiter _rateLimiter;
  private final int END_OF_STREAM_SIG = 0;
  private long _startTime;
  private AtomicInteger _cosumedTuples;
  private String _topic;

  public StreamServer(String kafkaServer, RateLimiter rateLimiter, long startTime, int duration,
      BufferedReader dataSource, AtomicInteger consumedTuples, int maxTupels, String topic) {

    _duration = duration;
    _sourceBuffer = dataSource;
    _rateLimiter = rateLimiter;
    _kafkaServer = kafkaServer;
    _startTime = startTime;
    _cosumedTuples = consumedTuples;
    _maxTuples = maxTupels;
    _topic = topic;
    this.start();
  }

  private Properties kafkaConfig(){
      Properties props = new Properties();

      props.put("metadata.broker.list", _kafkaServer);
      props.put("bootstrap.servers", _kafkaServer);
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      props.put("request.required.acks", "1");

      return props;
  }

  public void run() {
    try {

      String tuple;
      int localTuples = 0;
      Producer<String, String> producer = new KafkaProducer<>(kafkaConfig());
      while ((tuple = _sourceBuffer.readLine()) != null) {
        _rateLimiter.acquire();
        _cosumedTuples.incrementAndGet();
        if (_maxTuples != -1 && _cosumedTuples.get() > _maxTuples) {
          break;
        }

        producer.send(new ProducerRecord<String, String>(_topic,tuple));

        localTuples++;
        if (_maxTuples == -1 && System.currentTimeMillis() - _startTime > _duration) {
          break;
        }
      }
      //Close producer
      producer.close();
      System.exit(0);
    } catch (EOFException e) {
      System.out.println("EOF:" + e.getMessage());
    } catch (IOException e) {
      System.out.println("IO:" + e.getMessage());
    }
  }
}

