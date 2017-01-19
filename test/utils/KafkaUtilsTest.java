package utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.scalatest.testng.TestNGSuite;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;

public class KafkaUtilsTest extends TestNGSuite {

    @Test
    public void shouldReturnValuesCorrectly() {
        TopicPartition partition = new TopicPartition("test", 0);
        List<ConsumerRecord<String, String>> records = Arrays.asList(
                record("test 1"), record("test 2")
        );
        ConsumerRecords<String, String> consumerRecords = new ConsumerRecords<>(singletonMap(partition, records));

        List<String> result = KafkaUtils.recordValues(consumerRecords);
        assertThat(result).containsExactly("test 1", "test 2");
    }

    @Test
    public void shouldReturnEmptyListForNull() {
        assertThat(KafkaUtils.recordValues(null)).isEqualTo(Collections.emptyList());
    }

    private ConsumerRecord<String, String> record(String value) {
        return new ConsumerRecord<>("test", 0, 42, "test-key", value);
    }
}
