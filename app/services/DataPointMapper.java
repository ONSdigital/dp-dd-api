package services;

import au.com.bytecode.opencsv.CSVParser;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import exceptions.DataPointParseException;
import exceptions.DatapointMappingException;
import models.DataPointRecord;
import play.Logger;
import uk.co.onsdigital.discovery.model.DimensionalDataSet;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

/**
 * Service to receive messages from the {@link actors.KafkaActor}, process them and insert them into the database.
 */
public class DataPointMapper {

    private static final Logger.ALogger logger = Logger.of(DataPointMapper.class);

    private final ObjectMapper jsonMapper = new ObjectMapper();
    private final CSVParser csvParser = new CSVParser();
    private final EntityManager entityManager;
    private final InputCSVParser inputCSVParser;

    public DataPointMapper(InputCSVParser csvParser, EntityManager entityManager) {
        this.inputCSVParser = requireNonNull(csvParser);
        this.entityManager = requireNonNull(entityManager);
    }

    /**
     * Processes the given JSON records from the CSV Splitter and insert them into the database. All records in the
     * batch will be inserted in a single transaction. If an error occurs the entire batch will be aborted.
     *
     * @param jsonDataPoints the data points to process.
     */
    public void mapDataPoints(List<String> jsonDataPoints) throws DatapointMappingException {
        EntityTransaction tx = entityManager.getTransaction();
        tx.begin();
        try {

            for (String record : jsonDataPoints) {
                logger.debug("Processing data point: {}", record);

                final DataPointRecord dataPointRecord = parseDataPointRecord(record);
                mapDataPoint(dataPointRecord);
            }

            logger.debug("Committing transaction.");
            tx.commit();
            logger.debug("Finished processing {} data points", jsonDataPoints.size());
        } catch (Exception ex) {
            logger.error("Aborting transaction due to error: {}", ex, ex);
            tx.rollback();
            throw new DatapointMappingException(ex.getMessage());
        }
    }

    DimensionalDataSet findOrCreateDataset(UUID datasetId, String title) {
        DimensionalDataSet dimensionalDataSet = entityManager.find(DimensionalDataSet.class, datasetId);
        if (dimensionalDataSet == null) {
            dimensionalDataSet = new DimensionalDataSet(title, null);
            dimensionalDataSet.setDimensionalDataSetId(datasetId);
            entityManager.persist(dimensionalDataSet);
        }
        return dimensionalDataSet;
    }

    DataPointRecord parseDataPointRecord(String json) throws DataPointParseException, IOException {
        try {
            return jsonMapper.readValue(json, DataPointRecord.class);
        } catch (JsonMappingException | JsonParseException ex) {
            throw new DataPointParseException(ex);
        }
    }

    void mapDataPoint(final DataPointRecord dataPointRecord) throws IOException {
        final String[] rowDataArray = csvParser.parseLine(dataPointRecord.getRowData());
        logger.debug("rowDataArray: {}", (Object) rowDataArray);

        DimensionalDataSet dataSet = findOrCreateDataset(dataPointRecord.getDatasetID(), dataPointRecord.getFilename());
        inputCSVParser.parseRowdataDirectToTables(rowDataArray, dataSet);
    }

}
