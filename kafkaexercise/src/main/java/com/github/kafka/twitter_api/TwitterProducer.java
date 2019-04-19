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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    String consumerKey = "6iR7IETh47U8DGrk5mfedvdYm";
    String consumerSecret = "m4jE2CxYqcxubeAdozTHKfaw6bS3t9uVJT8dVeHXPUcz6GCRhe";
    String token = "960017551940923392-YQFoCPOdpyCzJMzEElaPF5FiEY3RxVw";
    String secret = "FFWZDrriU5oVH8poVNai6cLaSKmBso3my8jXH40zjzOxF";
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
            if (msg !=null) logger.info(msg);
//            something(msg);
//            profit();
        }
        logger.info("End of app!");
    }


    public Client createTwitterClient(BlockingQueue<String> msgQueue){

            /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
            Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
            StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
            // Optional: set up some followings and track terms
            List<Long> followings = Lists.newArrayList(1234L, 566788L);
            List<String> terms = Lists.newArrayList("fastai","keras","tensorflow","pytorch");
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
