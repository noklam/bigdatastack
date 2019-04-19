package com.github.kafka.twitter_api;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    String consumerKey = "6iR7IETh47U8DGrk5mfedvdYm";
    String consumerSecret = "m4jE2CxYqcxubeAdozTHKfaw6bS3t9uVJT8dVeHXPUcz6GCRhe";
    String token = "960017551940923392-YQFoCPOdpyCzJMzEElaPF5FiEY3RxVw";
    String secret = "FFWZDrriU5oVH8poVNai6cLaSKmBso3my8jXH40zjzOxF";
    List<String> terms = Lists.newArrayList("kafka");

    public TwitterProducer(){}

    public static void main(String[] args){
        new TwitterProducer().run();
    }

    private void run() {

        logger.info("Setup");
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        // create a twitter client
        Client client = createTwitterClient(msgQueue);
        // Attempts to establish a connection.
        client.connect();
        // create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();



        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stop application now...");
            logger.info("shutting down client from twitter...");
            client.stop();
            logger.info("closing producer...");
            producer.close();
            logger.info("done");
        }));

        // loop to send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg !=null) {
                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback(){
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("Something bad happened", e);
                        }
                    }
                });
            }
//            something(msg);
//            profit();
        }
        logger.info("End of app!");
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        //        System.out.println("hello world!");
        String bootstrapServers = "127.0.0.1:9092";
        // create Producer Properties
        Properties properties = new Properties();

        // Kafka require Properties
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        // create a kafka producer that is very safe. :)
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); //
        // kafka 2.0 >= 1.1 so we keep this as 5, 1 otherwsise.
        // Old version if use 5, you risk receiving message not in order.

        // create the producer

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }


    public Client createTwitterClient(BlockingQueue<String> msgQueue){

            /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
            Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
            StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
            // Optional: set up some followings and track terms
            List<Long> followings = Lists.newArrayList(1234L, 566788L);

            hosebirdEndpoint.followings(followings);
            hosebirdEndpoint.trackTerms(terms);

            // These secrets should be read from a config file
            Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

            ClientBuilder builder = new ClientBuilder()
                    .name("Hosebird-Client-01")                              // optional: mainly for the logs
                    .hosts(hosebirdHosts)
                    .authentication(hosebirdAuth)
                    .endpoint(hosebirdEndpoint)
                    .processor(new StringDelimitedProcessor(msgQueue));
           //                    .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

            Client hosebirdClient = builder.build();
            return hosebirdClient;


                }

}
