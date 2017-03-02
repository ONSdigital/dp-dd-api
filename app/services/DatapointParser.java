package services;

import exceptions.DatapointMappingException;
import uk.co.onsdigital.discovery.model.DataSet;

import javax.persistence.*;

/**
 * Capable of parsing a single row of a csv file and importing it in to the database.
 */
public interface DatapointParser extends AutoCloseable {
    void parseRowdataDirectToTables(EntityManager em, String[] rowData, DataSet dds) throws DatapointMappingException;
    void close();
}
