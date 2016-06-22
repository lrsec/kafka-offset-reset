package com.momo.live.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Created by linrui on 16/6/22.
 */
public class TopicConsumer {

	public static final Pattern COMMA_SPLIT_PATTERN = Pattern.compile("\\s*[,]+\\s*");

	public static void main(String[] args) {
		if (args.length < 3) {
			System.out.println("Use as: kafka-server-address group.id topics");
			System.exit(-1);
		}

		Properties props = new Properties();

		props.setProperty("bootstrap.servers", args[0]);
		props.setProperty("group.id", args[1]);
		props.put("enable.auto.commit", "false");
		props.put("auto.offset.reset", "earliest");
//		props.put("auto.offset.reset", "earliest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		String[] topics = COMMA_SPLIT_PATTERN.split(args[2]);

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Arrays.asList(topics));

		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(10);

			for (ConsumerRecord<String, String> record : records) {

				System.out.println(record.key() + " : " + record.value());

				consumer.commitSync();
			}
		}

	}
}
