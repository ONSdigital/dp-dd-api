package services;

import exceptions.DatapointMappingException;
import uk.co.onsdigital.discovery.model.DataSet;

import javax.persistence.*;
import java.sql.PreparedStatement;
import java.util.Map;
import java.util.UUID;

/**
 * Capable of parsing a single row of a csv file and importing it in to the database.
 */
public interface DatapointParser extends AutoCloseable {
    void parseRowdataDirectToTables(EntityManager em, String[] rowData, DataSet dds, UUID datapointId, Map<String, PreparedStatement> statements) throws DatapointMappingException;
    void close();
}
