package no.safebase;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ElasticSearchConsumer {

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        RestHighLevelClient client = createClient();

        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

        // poll for new data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            int recordCount = records.count();

            logger.info("Received " + recordCount + " records");

            BulkRequest bulkRequest = new BulkRequest();

            for (ConsumerRecord<String, String> record : records) {

                // 2 strategies
                // kafka generic ID
//                String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                // twitter feed specific id
                String id = extraxtIdFromTweet(record.value());
                try {
                    // where we insert data into ElasticSearch
                    String jsonString = record.value();
                    IndexRequest indexRequest = new IndexRequest("twitter")
                            .id(id) // this is to make our consumer idempotent
                            .source(jsonString, XContentType.JSON);

                    bulkRequest.add(indexRequest); // we add to our bulk request (takes no time)
                } catch (NullPointerException e) {
                    logger.warn("skipping bad data: " + record.value());
                }
            }

            if (recordCount > 0) {
                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                logger.info("Committing offsets...");
                consumer.commitSync();
                logger.info("Offsets have been committed");
                try {
                    Thread.sleep(1000); // introduce a small delay
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

//        client.close();
    }

    private static JsonParser jsonParser = new JsonParser();

    private static String extraxtIdFromTweet(String tweetJson) {
        // gson library
        return jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

    private static KafkaConsumer<String, String> createConsumer(String topic) {
        String bootstrapServers = "localhost:9092";
        String groupId = "kafka-demo-elasticsearch";

        // create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singletonList(topic));

        return consumer;
    }

    private static RestHighLevelClient createClient() {

        String hostname = "kafka-cluster-6804910836.eu-west-1.bonsaisearch.net";
        String username = "7r2boogaaj";
        String password = "s01uo1yxqv";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        return new RestHighLevelClient(builder);
    }
}
