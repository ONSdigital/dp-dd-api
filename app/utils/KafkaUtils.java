package utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Utility methods for dealing with Kafka records.
 */
public final class KafkaUtils {
    private KafkaUtils() {
        // Utility class
    }

    /**
     * Convert a list of Kafka {@link ConsumerRecords} into a list of the values from those records.
     *
     * @param records the Kafka consumer records.
     * @return the values of the records in the same order.
     */
    public static List<String> recordValues(final ConsumerRecords<?, String> records) {
        if (records == null) {
            return Collections.emptyList();
        }
        final List<String> values = new ArrayList<>(records.count());
        for (ConsumerRecord<?, String> record : records) {
            values.add(record.value());
        }
        return values;
    }
}
