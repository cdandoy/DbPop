package org.dandoy.dbpop.database;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Evaluates an {{expression}}
 * It currently only supports date-time expressions.
 * Examples:
 * <pre>
 *     {{now}}
 *     {{yesterday}}
 *     {{now - 3 days}}
 * </pre>
 */
public class ExpressionParser {
    private static final Pattern PATTERN = Pattern.compile("\\{\\{(now|today|tomorrow|yesterday)( *([+-]) *(\\d) *(minute|minutes|hour|hours|day|days|month|months|year|years))?}}");

    public Object evaluate(String s) {
        if (!s.startsWith("{{") || !s.endsWith("}}")) return s;

        Matcher matcher = PATTERN.matcher(s);
        if (!matcher.matches()) throw new RuntimeException("Failed to parse \"" + s + "\"");

        String reference = matcher.group(1);
        LocalDateTime localDateTime = toLocalDateTime(reference).orElseThrow(() -> new RuntimeException("Failed to parse \"" + s + "\""));

        if (matcher.group(2) != null) {
            int dir = "+".equals(matcher.group(3)) ? 1 : -1;
            int nbr = Integer.parseInt(matcher.group(4));
            String unit = matcher.group(5);
            localDateTime = adjust(localDateTime, dir, nbr, unit);
        }
        return Date.from(
                localDateTime
                        .atZone(ZoneId.systemDefault())
                        .toInstant()
        );
    }

    private Optional<LocalDateTime> toLocalDateTime(String s) {
        switch (s) {
            case "yesterday":
                return Optional.of(LocalDateTime.now().minusDays(1));
            case "now":
            case "today":
                return Optional.of(LocalDateTime.now());
            case "tomorrow":
                return Optional.of(LocalDateTime.now().plusDays(1));
            default:
                return Optional.empty();
        }
    }

    private LocalDateTime adjust(LocalDateTime localDateTime, int dir, long nbr, String unit) {
        switch (unit) {
            case "minute":
            case "minutes":
                return localDateTime.plusMinutes(dir * nbr);
            case "hour":
            case "hours":
                return localDateTime.plusHours(dir * nbr);
            case "day":
            case "days":
                return localDateTime.plusDays(dir * nbr);
            case "month":
            case "months":
                return localDateTime.plusMonths(dir * nbr);
            case "year":
            case "years":
                return localDateTime.plusYears(dir * nbr);
            default:
                throw new RuntimeException("Unexpected unit " + unit);
        }
    }
}
