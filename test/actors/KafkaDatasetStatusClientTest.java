package actors;

import com.fasterxml.jackson.databind.ObjectMapper;
import models.DatasetStatus;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.mockito.Answers;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import services.DatasetStatusUpdater;
import utils.LambdaMatcher;

import java.util.UUID;

import static java.util.Collections.singletonList;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class KafkaDatasetStatusClientTest {


    public static final UUID DATASET_ID = UUID.randomUUID();
    public static final String STATUS_TOPIC = "statusTopic";
    public static final String DEAD_TOPIC = "deadTopic";

    @Mock
    private Consumer<String, String> consumerMock;
    @Mock
    private Producer<String, String> producerMock;
    @Mock
    private DatasetStatusUpdater updaterMock;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ConsumerRecords<String, String> recordsMock;
    private ConsumerRecord<String, String> consumerRecord;

    private ObjectMapper jsonMapper = new ObjectMapper();
    private DatasetStatus initialStatus;
    private String jsonString;

    private KafkaDatasetStatusClient testObj;

    @BeforeMethod
    public void setUp() throws Exception {
        initMocks(this);
        testObj = new KafkaDatasetStatusClient(consumerMock, producerMock, STATUS_TOPIC, DEAD_TOPIC);

        initialStatus = new DatasetStatus(System.currentTimeMillis(), 1, 0, DATASET_ID);
        jsonString = jsonMapper.writeValueAsString(initialStatus);

        when(consumerMock.poll(anyLong())).thenReturn(recordsMock);
        consumerRecord = new ConsumerRecord<String, String>(STATUS_TOPIC, 0, 0, "key", jsonString);
    }

    @Test
    public void shouldIgnoreEmptyMessages() throws Exception {
        // given an empty list of status messages

        // when processStatusMessages is invoked
        testObj.processStatusMessages(updaterMock);

        // then nothing should happen
        verify(updaterMock, never()).updateStatuses(anyListOf(DatasetStatus.class));
    }

    @Test
    public void shouldResendStatusMessage() throws Exception {
        // given a single status message
        when(recordsMock.iterator().hasNext()).thenReturn(true).thenReturn(false);
        when(recordsMock.iterator().next()).thenReturn(consumerRecord);
        // that represents a dataset still being processed
        DatasetStatus datasetStatus = new DatasetStatus(System.currentTimeMillis(), 1, 0, DATASET_ID);
        when(updaterMock.updateStatuses(singletonList(initialStatus))).thenReturn(singletonList(datasetStatus));

        // when processStatusMessages is invoked
        testObj.processStatusMessages(updaterMock);

        // then a status message is re-sent
        String datasetJson = jsonMapper.writeValueAsString(datasetStatus);

        verify(producerMock).send(LambdaMatcher.argThatMatches(record ->
                datasetJson.equals(record.value())
                        && STATUS_TOPIC.equals(record.topic())
        ));

    }

    @Test
    public void shouldIgnoreInvalidMessage() throws Exception {
        // given a valid status message following an invalid message
        ConsumerRecord<String, String> invalidRecord = new ConsumerRecord<String, String>(STATUS_TOPIC, 0, 0, "key", "invalid json");

        when(recordsMock.iterator().hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(recordsMock.iterator().next()).thenReturn(invalidRecord).thenReturn(consumerRecord);
        // that represents a dataset still being processed
        DatasetStatus datasetStatus = new DatasetStatus(System.currentTimeMillis(), 1, 0, DATASET_ID);
        when(updaterMock.updateStatuses(singletonList(initialStatus))).thenReturn(singletonList(datasetStatus));

        // when processStatusMessages is invoked
        testObj.processStatusMessages(updaterMock);

        // then a status message is re-sent
        String datasetJson = jsonMapper.writeValueAsString(datasetStatus);

        verify(producerMock).send(LambdaMatcher.argThatMatches(record ->
                datasetJson.equals(record.value())
                        && STATUS_TOPIC.equals(record.topic())
        ));

    }

    @Test
    public void shouldNotSendMessageForCompleteDataset() throws Exception {
        // given a single status message
        when(recordsMock.iterator().hasNext()).thenReturn(true).thenReturn(false);
        when(recordsMock.iterator().next()).thenReturn(consumerRecord);
        // that represents a dataset still being processed
        DatasetStatus datasetStatus = new DatasetStatus(System.currentTimeMillis(), 1, 1, DATASET_ID);
        when(updaterMock.updateStatuses(singletonList(initialStatus))).thenReturn(singletonList(datasetStatus));

        // when processStatusMessages is invoked
        testObj.processStatusMessages(updaterMock);

        // then no message
        verifyZeroInteractions(producerMock);
    }

    @Test
    public void shouldSendDeadDatasetMessage() throws Exception {
        // given a single status message
        when(recordsMock.iterator().hasNext()).thenReturn(true).thenReturn(false);
        when(recordsMock.iterator().next()).thenReturn(consumerRecord);
        // that represents a dataset that hasn't been updated in longer than the timeout
        DatasetStatus datasetStatus = new DatasetStatus(0, 1, 0, DATASET_ID);
        when(updaterMock.updateStatuses(singletonList(initialStatus))).thenReturn(singletonList(datasetStatus));

        // when processStatusMessages is invoked
        testObj.processStatusMessages(updaterMock);

        // then a dead dataset message is sent
        String datasetJson = jsonMapper.writeValueAsString(datasetStatus);

        verify(producerMock).send(LambdaMatcher.argThatMatches(record ->
                datasetJson.equals(record.value())
                        && DEAD_TOPIC.equals(record.topic())
        ));
    }

}