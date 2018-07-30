package org.talend.components.netsuite.runtime.avro.converter;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.util.Date;
import java.util.GregorianCalendar;

import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

/**
 * Responsible for conversion of <code>XMLGregorianCalendar</code> from/to <code>date-time</code>.
 */
public class XMLGregorianCalendarToDateTimeConverter implements Converter<XMLGregorianCalendar, Object> {

    private DatatypeFactory datatypeFactory;

    public XMLGregorianCalendarToDateTimeConverter(DatatypeFactory datatypeFactory) {
        this.datatypeFactory = datatypeFactory;
    }

    @Override
    public Schema getSchema() {
        return LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    }

    @Override
    public Class<XMLGregorianCalendar> getDatumClass() {
        return XMLGregorianCalendar.class;
    }

    @Override
    public XMLGregorianCalendar convertToDatum(Object timestamp) {
        if (timestamp == null) {
            return null;
        }

        long timestampMillis;
        if (timestamp instanceof Long) {
            timestampMillis = ((Long) timestamp).longValue();
        } else if (timestamp instanceof Date) {
            timestampMillis = ((Date) timestamp).getTime();
        } else {
            throw new IllegalArgumentException("Unsupported Avro timestamp value: " + timestamp);
        }
        GregorianCalendar gc = new GregorianCalendar();
        gc.setTime(new Date(timestampMillis));
        return datatypeFactory.newXMLGregorianCalendar(gc);
    }

    @Override
    public Object convertToAvro(XMLGregorianCalendar xts) {
        if (xts == null) {
            return null;
        }

        LocalDateTime dateTime = LocalDateTime.now();
        try {
            dateTime.withYear(xts.getYear());
            dateTime.withMonth(xts.getMonth());
            dateTime.withDayOfMonth(xts.getDay());
            dateTime.withHour(xts.getHour());
            dateTime.withMinute(xts.getMinute());
            dateTime.withSecond(xts.getSecond());
            dateTime.with(ChronoField.MILLI_OF_SECOND, xts.getMillisecond());

            ZoneOffset tz = ZoneOffset.ofTotalSeconds(xts.getTimezone() * 60);
            return Long.valueOf(dateTime.toInstant(tz).toEpochMilli());
        } catch (IllegalArgumentException e) {
            throw new RuntimeException();
            // TODO: Fix exception
            // throw new ComponentException(e);
        }
    }
}
