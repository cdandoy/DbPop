package org.dandoy.dbpop.database;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;

public abstract class ColumnType {
    public static ColumnType VARCHAR = new ColumnType() {
    };

    public static ColumnType INTEGER = new ColumnType() {
        @Override
        public void bind(PreparedStatement preparedStatement, int jdbcPos, String input) throws SQLException {
            if (input == null) {
                preparedStatement.setNull(jdbcPos, Types.INTEGER);
            } else {
                preparedStatement.setLong(jdbcPos, Long.parseLong(input));
            }
        }
    };

    public static ColumnType BIG_DECIMAL = new ColumnType() {
        @Override
        public void bind(PreparedStatement preparedStatement, int jdbcPos, String input) throws SQLException {
            if (input == null) {
                preparedStatement.setNull(jdbcPos, Types.DECIMAL);
            } else {
                preparedStatement.setBigDecimal(jdbcPos, new BigDecimal(input));
            }
        }
    };

    public static ColumnType TIMESTAMP = new ColumnType() {
        @Override
        public void bind(PreparedStatement preparedStatement, int jdbcPos, String input) throws SQLException {
            SimpleDateFormat format_10 = new SimpleDateFormat("yyyy-MM-dd");
            SimpleDateFormat format_19 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            SimpleDateFormat format_23 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            if (input == null) {
                preparedStatement.setNull(jdbcPos, Types.TIMESTAMP);
            } else {
                try {
                    Date date;
                    if (input.length() == 10) {
                        date = format_10.parse(input);
                    } else if (input.length() == 19) {
                        date = format_19.parse(input);
                    } else if (input.length() == 23) {
                        date = format_23.parse(input);
                    } else {
                        date = java.sql.Date.valueOf(input);
                    }
                    preparedStatement.setTimestamp(jdbcPos, new Timestamp(date.getTime()));
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    };

    public static ColumnType BINARY = new ColumnType() {
        @Override
        public void bind(PreparedStatement preparedStatement, int jdbcPos, String input) throws SQLException {
            if (input == null) {
                preparedStatement.setNull(jdbcPos, Types.BINARY);
            } else {
                byte[] bytes = Base64.getDecoder().decode(input);
                preparedStatement.setBytes(jdbcPos, bytes);
            }
        }
    };

    public void bind(PreparedStatement preparedStatement, int jdbcPos, String input) throws SQLException {
        preparedStatement.setString(jdbcPos, input);
    }

    public void bind(PreparedStatement preparedStatement, int jdbcPos, byte[] input) throws SQLException {
        preparedStatement.setBytes(jdbcPos, input);
    }

    public void bind(PreparedStatement preparedStatement, int jdbcPos, Object input) throws SQLException {
        preparedStatement.setObject(jdbcPos, input);
    }
}
