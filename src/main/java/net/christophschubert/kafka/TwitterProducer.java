package net.christophschubert.kafka;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterProducer {

    private static final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    public static final String TOPIC_CONFIG = "topic";
    public static final String TERMS_CONFIG = "terms";
    public static final String CONSUMER_KEY_CONFIG = "consumer.key";
    public static final String CONSUMER_SECRET_CONFIG = "consumer.secret";
    public static final String TOKEN_CONFIG = "token";
    public static final String TOKEN_SECRET_CONFIG = "token.secret";


    static List<String> getTerms(Properties settings) {
        if (!settings.containsKey(TERMS_CONFIG)) {
            return List.of("kafka", "summer"); // default terms
        }
        return Arrays.asList(settings.getProperty(TERMS_CONFIG).split(","));
    }

    static String getStringOrFail(Properties settings, String key) {
        return Objects.requireNonNull(settings.getProperty(key), "Missing config " + key);
    }

    static Authentication getAuthentication(Properties settings) {
        return new OAuth1(
                getStringOrFail(settings, CONSUMER_KEY_CONFIG),
                getStringOrFail(settings, CONSUMER_SECRET_CONFIG),
                getStringOrFail(settings, TOKEN_CONFIG),
                getStringOrFail(settings, TOKEN_SECRET_CONFIG)
        );
    }

    static Client buildTwitterClient(Properties settings, BlockingQueue<String> msgQueue) {
        //set up endpoint and terms to track
        final StatusesFilterEndpoint hoseBirdEndpoint = new StatusesFilterEndpoint();
        final List<String> terms = getTerms(settings);
        hoseBirdEndpoint.trackTerms(terms);

        final var builder = new ClientBuilder()
                .hosts(new HttpHosts(Constants.STREAM_HOST))
                .authentication(getAuthentication(settings))
                .endpoint(hoseBirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
        return builder.build();
    }

    public static void main(String... args) throws InterruptedException, IOException {

        final Properties settings = new Properties();
        if (args.length > 0) {
            settings.load(new FileReader(args[0]));
        } else {
            System.err.println("required argument property path is missing");
            System.exit(1);
        }

        // Set up a blocking queue: adjust capacity to fit throughput of your stream if needed
        final BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(10000);
        final var hoseBirdClient = buildTwitterClient(settings, msgQueue);
        hoseBirdClient.connect();
        logger.info("Connected to twitter API to track terms '{}'", getTerms(settings));

        // set up Kafka producer
        final var stringSerializer = new StringSerializer();
        final var producer = new KafkaProducer<>(settings, stringSerializer, stringSerializer);

        final var kafkaTopic = Objects.toString(settings.get(TOPIC_CONFIG), "feed_raw");
        while (!hoseBirdClient.isDone()) {
            final var msg = msgQueue.take().stripTrailing();
            final var producerRecord = new ProducerRecord<String, String>(kafkaTopic,null, msg);
            producer.send(producerRecord, ((metadata, exception) -> {
                if (exception != null) {
                    logger.error("Error producing message", exception);
                } else {
                    logger.info("send {}", msg);
                }
            }));
        }
        producer.close();
    }
}
