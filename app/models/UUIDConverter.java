package models;

import javax.persistence.AttributeConverter;
import javax.persistence.Converter;
import java.util.UUID;

/**
 * Trivial {@link AttributeConverter} for {@link UUID} data to encourage PostgreSQL to treat them as a real {@code uuid}
 * type rather than converting to a {@code bytea} (byte array). It may appear that this class does nothing, but it does
 * just the right amount of nothing to make a difference :-)
 */
@Converter(autoApply = true)
public class UUIDConverter implements AttributeConverter<UUID, UUID> {
    @Override
    public UUID convertToDatabaseColumn(UUID attribute) {
        return attribute;
    }

    @Override
    public UUID convertToEntityAttribute(UUID dbData) {
        return dbData;
    }
}
