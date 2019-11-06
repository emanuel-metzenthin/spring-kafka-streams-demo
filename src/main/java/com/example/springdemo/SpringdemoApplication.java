package com.example.springdemo;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

interface Channels {
	String PURCHASE_OUT = "purchase_out";
	String PURCHASE_IN = "purchase_in";
	String SALES_OUT = "sales_out";

	String SALES_TABLE = "sales_table";

	@Output(PURCHASE_OUT)
	MessageChannel purchaseOut();

	@Input(PURCHASE_IN)
	KStream<String, Purchase> purchaseIn();

	@Output(SALES_OUT)
	KStream<String, Integer> salesOut();
}


@SpringBootApplication
@EnableBinding(Channels.class)
public class SpringdemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringdemoApplication.class, args);
	}

	@StreamListener(Channels.PURCHASE_IN)
	@SendTo(Channels.SALES_OUT)
	public KStream<String, Integer> process(KStream<String, Purchase> purchases) {
		KStream<String, Integer> aggregate = purchases
												.map((k, v) -> new KeyValue<>(v.getProductName(), v.getPrice()))
												.groupByKey()
												.reduce((oldVal, newVal) -> oldVal + newVal)
												.toStream();

		aggregate.foreach((k, v) -> System.out.println(k + ":  " + v));

		return aggregate;
	}

	@RestController
	public static class CountController {

		@Autowired
		private InteractiveQueryService queryService;

		@GetMapping("/sales")
		Map<String, Integer> sales() {
			Map<String, Integer> sales = new HashMap<>();

			ReadOnlyKeyValueStore<String, Integer> queryableStoreType =
					queryService.getQueryableStore(Channels.SALES_TABLE, QueryableStoreTypes.keyValueStore());

			KeyValueIterator<String, Integer> all = queryableStoreType.all();
			while (all.hasNext()) {
				KeyValue<String, Integer> value = all.next();
				sales.put(value.key, value.value);
			}
			return sales;
		}
	}

}