package org.talend.components.netsuite.runtime.avro.converter;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Date;

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

        TemporalAccessor date = Instant.ofEpochMilli(timestampMillis);
        XMLGregorianCalendar xts = datatypeFactory.newXMLGregorianCalendar();
        xts.setYear(date.get(ChronoField.YEAR));
        xts.setMonth(date.get(ChronoField.MONTH_OF_YEAR));
        xts.setDay(date.get(ChronoField.DAY_OF_MONTH));
        xts.setHour(date.get(ChronoField.HOUR_OF_DAY));
        xts.setMinute(date.get(ChronoField.MINUTE_OF_DAY));
        xts.setSecond(date.get(ChronoField.SECOND_OF_DAY));
        xts.setMillisecond(date.get(ChronoField.MILLI_OF_SECOND));
        xts.setTimezone(ZoneOffset.from(date).getTotalSeconds() / 60);

        return xts;
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
