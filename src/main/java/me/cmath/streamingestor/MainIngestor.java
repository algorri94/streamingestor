package me.cmath.streamingestor;

import com.google.common.util.concurrent.RateLimiter;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;


public class MainIngestor {

  public static final String APP_NAME = "StreamIngestor";
  private String _kafkaServer = "localhost:9093";
  private int _throughPut = 10000;
  private int _duration = 3000;
  private int _maxTupels = -1;
  private String _dataSourceFile = "";
  private AtomicInteger _consumedTuples;
  private String _topic;

  public MainIngestor(String kafkaServer, int throughPut, int duration, int maxTupels, String dataSourceFile, String topic) {
    _kafkaServer = (kafkaServer.equals("")) ? _kafkaServer : kafkaServer;
    _topic = (topic.equals("")) ? _topic : topic;
    _throughPut = (throughPut == 0) ? _throughPut : throughPut;
    _duration = (duration == 0) ? _duration : duration;
    _dataSourceFile = dataSourceFile;
    _consumedTuples = new AtomicInteger(0);
    _maxTupels = maxTupels;
  }

  public void startStream() {
    try {
      BufferedReader dataSource = new BufferedReader(new FileReader(new File(_dataSourceFile)));
      RateLimiter rateLimiter = RateLimiter.create(_throughPut);
      long startTime = System.currentTimeMillis();
      StreamServer c = new StreamServer(_kafkaServer, rateLimiter, startTime, _duration, dataSource, _consumedTuples, _maxTupels, _topic);
      if (System.currentTimeMillis() - startTime > _duration + 3000) {
          printStats();
      }
    } catch (IOException e) {
      System.out.println("Error: " + e.getMessage());
    } catch (Exception e) {
      System.out.println("Shutting down due to interruption..");
    }
  }

  public void printStats() {
    double actualTP = (double) _consumedTuples.get() / (_duration / 1000.0);
    System.out.println("Shutting down server..");
    System.out.println("Consumed tuples: " + _consumedTuples.get());
    System.out.println("Actual throughput: " + actualTP + " tuples/sec");
  }

  public static void main(String[] args) {
    int throughPut = 0, duration = 0, maxTuples = -1;
    String dataSourceFile = null, kafkaServer = null, topic = null;

    Options options = new Options();

    options.addOption("h", "help", false, "Show this dialog");
    options.addOption("p", "port", true, "The kafka cluster brokers addresses and ports <address:port,address:port...>");
    options.addOption("t", "throughput", true, "The amount of tuples per second that will be streamed");
    options.addOption("d", "duration", true, "The amount of time in ms that tuples will be streamed");
    options.addOption("f", "file", true, "The input file with tuples separated by line breaks");
    options.addOption("m", "maxtuples", true, "Max tuples to read from file. This will override any time contraints and close the stream when all tuples have been streamed.");
    options.addOption("T", "topic", true, "Kafka topic to push the data to");

    CommandLineParser parser = new BasicParser();
    HelpFormatter formater = new HelpFormatter();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);

      if (cmd.hasOption("h")) {
        formater.printHelp(APP_NAME, options);
        System.exit(0);
      }

      if (cmd.hasOption("m")) {
        maxTuples = Integer.parseInt(cmd.getOptionValue("m"));
      }
      if (cmd.hasOption("p")) {
        kafkaServer = cmd.getOptionValue("p");
      }
      if (cmd.hasOption("t")) {
        throughPut = Integer.parseInt(cmd.getOptionValue("t"));
      }
      if (cmd.hasOption("d")) {
        duration = Integer.parseInt(cmd.getOptionValue("d"));
      }
      if (cmd.hasOption("T")){
        topic = cmd.getOptionValue("T");
      } else {
        System.err.println("Please specify a topic");
        System.exit(1);
      }
      if (cmd.hasOption("f")) {
        dataSourceFile = cmd.getOptionValue("f");
      } else {
        System.err.println("Please specify an input file");
        System.exit(1);
      }
    } catch (Exception e) {
      formater.printHelp(APP_NAME, options);
      System.exit(0);
    }

    final MainIngestor mainIngestor = new MainIngestor(kafkaServer, throughPut, duration, maxTuples, dataSourceFile, topic);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        mainIngestor.printStats();
      }
    });
    mainIngestor.startStream();


  }
}
