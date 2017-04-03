package services;

import exceptions.DatapointMappingException;
import uk.co.onsdigital.discovery.model.DataSet;

import javax.persistence.*;
import java.util.UUID;

/**
 * Capable of parsing a single row of a csv file and importing it in to the database.
 */
public interface DatapointParser extends AutoCloseable {
    void parseRowdataDirectToTables(EntityManager em, String[] rowData, DataSet dds, UUID datapointId) throws DatapointMappingException;
    void close();
}
