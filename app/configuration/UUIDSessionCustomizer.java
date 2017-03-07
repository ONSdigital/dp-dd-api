package configuration;

//import org.hibernate.persistence.config.SessionCustomizer;
//import org.eclipse.persistence.descriptors.ClassDescriptor;
//import org.eclipse.persistence.internal.helper.DatabaseField;
//import org.eclipse.persistence.mappings.DatabaseMapping;
//import org.eclipse.persistence.mappings.ManyToOneMapping;
//import org.eclipse.persistence.sessions.Session;
import uk.co.onsdigital.discovery.model.DimensionValue;

import java.sql.Types;
import java.util.Vector;

/**
 * Temporary fix (until we get rid of eclipselink) to fix a bug whereby null values in a foreign key using uuid cause an error:
 * http://stackoverflow.com/questions/38504481/null-value-in-uuid-column
 *
 * Caused by: org.postgresql.util.PSQLException: ERROR: column "hierarchy_entry_id" is of type uuid but expression is of type character varying
 * Hint: You will need to rewrite or cast the expression.
 */
//public class UUIDSessionCustomizer implements SessionCustomizer {
//
//    @Override
//    public void customize(Session session) throws Exception {
//        ClassDescriptor classDescriptor = session.getClassDescriptor(DimensionValue.class);
//        DatabaseMapping mapping = classDescriptor.getMappingForAttributeName("hierarchyEntry");
//        if (mapping instanceof ManyToOneMapping) {
//            Vector<DatabaseField> foreignKeyFields = ((ManyToOneMapping) mapping).getForeignKeyFields();
//            if (!foreignKeyFields.isEmpty()) {
//                DatabaseField field = foreignKeyFields.elementAt(0);
//                field.setSqlType(Types.OTHER);
//            }
//        }
//    }
//}
