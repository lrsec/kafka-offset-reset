package com.momo.live.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * Created by linrui on 16/6/22.
 */
public class OffsetReset {

	public static void main(String[] args) {
		if (args.length < 4) {
			System.out.println("Use as: kafka-server-address group.id topic partition-size");
			System.exit(-1);
		}

		System.out.println("还原 kafka 服务 " + args[0] + " group: " + args[1] + " topic: " + args[2] + " partition: " + args[3] + " 输入yes确认:");
		Scanner input = new Scanner(System.in);
		String inputString = input.nextLine();

		if ("yes".equalsIgnoreCase(inputString)) {

			Properties props = new Properties();
			props.setProperty("bootstrap.servers", args[0]);
			props.setProperty("group.id", args[1]);
			props.put("enable.auto.commit", "false");
			props.put("auto.offset.reset", "earliest");
			props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

			String topic = args[2];

			KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

			int partitionSize = Integer.parseInt(args[3]);
			List<TopicPartition> list = new ArrayList<TopicPartition>(partitionSize);

			for (int i=0; i<partitionSize; i++) {
				list.add(i, new TopicPartition(topic, i));
			}

			consumer.assign(list);

			Map<TopicPartition, OffsetAndMetadata> map = new HashMap<TopicPartition, OffsetAndMetadata>();
			for (int i = 0; i< partitionSize; i++) {
				TopicPartition tp = new TopicPartition(topic, i);


				System.out.println("Topic: " + topic + " Partition: " + i + " original offset: " + consumer.position(tp));

				consumer.seekToBeginning(tp);
				long offset = consumer.position(tp) - 1;
				if (offset < 0) {
					offset = 0;
				}

				System.out.println("Topic: " + topic + " Partition: " + i + " begin offset: " + offset);

				OffsetAndMetadata om = new OffsetAndMetadata(offset);

				map.put(tp, om);
			}

			consumer.commitSync(map);

		} else {
			System.out.println("quit");
			System.exit(-1);
		}
	}
}
