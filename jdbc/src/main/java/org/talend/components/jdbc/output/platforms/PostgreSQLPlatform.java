/*
 * Copyright (C) 2006-2024 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.jdbc.output.platforms;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import org.talend.components.jdbc.configuration.JdbcConfiguration;
import org.talend.components.jdbc.service.I18nMessage;

import lombok.extern.slf4j.Slf4j;

/**
 * syntax detail can be found at <a href=
 * "http://www.postgresqltutorial.com/postgresql-create-table/">http://www.postgresqltutorial.com/postgresql-create-table/</a>
 */
@Slf4j
public class PostgreSQLPlatform extends Platform {

    public static final String POSTGRESQL = "postgresql";

    public PostgreSQLPlatform(final I18nMessage i18n, final JdbcConfiguration.Driver driver) {
        super(i18n, driver);
    }

    @Override
    public String name() {
        return POSTGRESQL;
    }

    @Override
    protected String delimiterToken() {
        return "\"";
    }

    @Override
    protected String buildQuery(final Connection connection, final Table table, final boolean useOriginColumnName)
            throws SQLException {
        // keep the string builder for readability
        final StringBuilder sql = new StringBuilder("CREATE TABLE");
        sql.append(" ");
        sql.append("IF NOT EXISTS");
        sql.append(" ");
        if (table.getSchema() != null && !table.getSchema().isEmpty()) {
            sql.append(identifier(table.getSchema())).append(".");
        }
        sql.append(identifier(table.getName()));
        sql.append("(");
        sql.append(createColumns(table.getColumns(), useOriginColumnName));
        sql
                .append(createPKs(connection.getMetaData(), table.getName(),
                        table.getColumns().stream().filter(Column::isPrimaryKey).collect(Collectors.toList())));
        sql.append(")");
        // todo create index

        log.debug("### create table query ###");
        log.debug(sql.toString());
        return sql.toString();
    }

    @Override
    protected boolean isTableExistsCreationError(final Throwable e) {
        // name space creation issue in distributed exectution is not handled by "IF NOT EXISTS"
        // https://www.postgresql.org/message-id/CA%2BTgmoZAdYVtwBfp1FL2sMZbiHCWT4UPrzRLNnX1Nb30Ku3-gg%40mail.gmail.com
        return e instanceof SQLException && "23505".equals(((SQLException) e).getSQLState());
    }

    private String createColumns(final List<Column> columns, final boolean useOriginColumnName) {
        return columns.stream().map(e -> createColumn(e, useOriginColumnName)).collect(Collectors.joining(","));
    }

    private String createColumn(final Column column, final boolean useOriginColumnName) {
        log.debug("createColumn column: " + column);
        return identifier(useOriginColumnName ? column.getOriginalFieldName() : column.getName())//
                + " " + toDBType(column)//
                + " " + isRequired(column)//
        ;
    }

    private String toDBType(final Column column) {
        switch (column.getType()) {
        case STRING:
            return column.getSize() <= -1 ? "character varying" : "VARCHAR(" + column.getSize() + ")";
        case BOOLEAN:
            return "BOOLEAN";
        case DOUBLE:
            return "REAL";
        case FLOAT:
            return "FLOAT";
        case LONG:
            return "BIGINT";
        case INT:
            return "INT";
        case BYTES:
            return "BYTEA";
        case DATETIME:
            return "timestamp";
        case RECORD:
        case ARRAY:
        default:
            throw new IllegalStateException(
                    getI18n().errorUnsupportedType(column.getType().name(), column.getOriginalFieldName()));
        }
    }

}
