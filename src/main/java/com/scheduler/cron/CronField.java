package com.scheduler.cron;

import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.time.temporal.ChronoField;
import java.util.*;

public class CronField {

    private final FieldType fieldType;
    private final TreeSet<Integer> values;
    private final int min;
    private final int max;

    public enum FieldType {
        SECOND(0, 59, ChronoField.SECOND_OF_MINUTE),
        MINUTE(0, 59, ChronoField.MINUTE_OF_HOUR),
        HOUR(0, 23, ChronoField.HOUR_OF_DAY),
        DAY_OF_MONTH(1, 31, ChronoField.DAY_OF_MONTH),
        MONTH(1, 12, ChronoField.MONTH_OF_YEAR),
        DAY_OF_WEEK(0, 7, ChronoField.DAY_OF_WEEK);

        final int min;
        final int max;
        final ChronoField chronoField;

        FieldType(int min, int max, ChronoField chronoField) {
            this.min = min;
            this.max = max;
            this.chronoField = chronoField;
        }
    }

    public CronField(FieldType fieldType, TreeSet<Integer> values) {
        this.fieldType = fieldType;
        this.values = values;
        this.min = fieldType.min;
        this.max = fieldType.max;
    }

    public FieldType getFieldType() { return fieldType; }
    public TreeSet<Integer> getValues() { return values; }

    public static CronField parse(FieldType fieldType, String expression) {
        TreeSet<Integer> values = new TreeSet<>();
        for (String part : expression.split(",")) {
            values.addAll(parsePart(fieldType, part.trim()));
        }
        return new CronField(fieldType, values);
    }

    private static List<Integer> parsePart(FieldType fieldType, String part) {
        if (part.equals("*")) {
            return IntStream.rangeClosed(fieldType.min, fieldType.max).boxed().toList();
        }
        if (part.contains("/")) {
            return parseStep(fieldType, part);
        }
        if (part.contains("-")) {
            return parseRange(fieldType, part);
        }
        return List.of(parseValue(fieldType, part));
    }

    private static List<Integer> parseStep(FieldType fieldType, String part) {
        String[] parts = part.split("/");
        int start = parts[0].equals("*") ? fieldType.min : parseValue(fieldType, parts[0]);
        int step = Integer.parseInt(parts[1]);
        if (step <= 0) throw new IllegalArgumentException("Step must be positive: " + step);
        List<Integer> values = new ArrayList<>();
        for (int i = start; i <= fieldType.max; i += step) {
            values.add(i);
        }
        return values;
    }

    private static List<Integer> parseRange(FieldType fieldType, String part) {
        String[] parts = part.split("-");
        int start = parseValue(fieldType, parts[0]);
        int end = parseValue(fieldType, parts[1]);
        if (start > end) throw new IllegalArgumentException("Range start > end: " + part);
        return IntStream.rangeClosed(start, end).boxed().toList();
    }

    private static int parseValue(FieldType fieldType, String value) {
        if (value.equalsIgnoreCase("L") && fieldType == FieldType.DAY_OF_MONTH) {
            return fieldType.max;
        }
        if (value.equalsIgnoreCase("L") && fieldType == FieldType.DAY_OF_WEEK) {
            return 7;
        }
        if (value.equalsIgnoreCase("W") && fieldType == FieldType.DAY_OF_MONTH) {
            return -1;
        }
        int parsed = Integer.parseInt(value);
        if (parsed < fieldType.min || parsed > fieldType.max) {
            throw new IllegalArgumentException(
                    "Value %d out of range [%d, %d] for %s".formatted(parsed, fieldType.min, fieldType.max, fieldType)
            );
        }
        return parsed;
    }

    public boolean matches(int fieldValue) {
        if (fieldType == FieldType.DAY_OF_WEEK) {
            int cronDow = fieldValue == 0 ? 0 : (fieldValue % 7) + 1;
            return values.stream().anyMatch(v -> {
                int normalized = v == 0 ? 7 : v;
                return normalized == cronDow;
            });
        }
        return values.contains(fieldValue);
    }

    public int nextValue(int current) {
        Integer ceiling = values.ceiling(current);
        if (ceiling != null) return ceiling;
        return values.first();
    }

    public boolean hasChangedOnRollOver(int current) {
        return !values.contains(current);
    }
}
