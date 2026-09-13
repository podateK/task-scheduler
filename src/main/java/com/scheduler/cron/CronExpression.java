package com.scheduler.cron;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.EnumMap;
import java.util.Map;

public class CronExpression {

    private final String expression;
    private final Map<CronField.FieldType, CronField> fields;
    private final boolean hasSeconds;

    public CronExpression(String expression) {
        this.expression = expression;
        this.fields = new EnumMap<>(CronField.FieldType.class);
        String[] parts = expression.trim().split("\\s+");
        if (parts.length == 5) {
            this.hasSeconds = false;
            fields.put(CronField.FieldType.MINUTE, CronField.parse(CronField.FieldType.MINUTE, parts[0]));
            fields.put(CronField.FieldType.HOUR, CronField.parse(CronField.FieldType.HOUR, parts[1]));
            fields.put(CronField.FieldType.DAY_OF_MONTH, CronField.parse(CronField.FieldType.DAY_OF_MONTH, parts[2]));
            fields.put(CronField.FieldType.MONTH, CronField.parse(CronField.FieldType.MONTH, parts[3]));
            fields.put(CronField.FieldType.DAY_OF_WEEK, CronField.parse(CronField.FieldType.DAY_OF_WEEK, parts[4]));
        } else if (parts.length == 6) {
            this.hasSeconds = true;
            fields.put(CronField.FieldType.SECOND, CronField.parse(CronField.FieldType.SECOND, parts[0]));
            fields.put(CronField.FieldType.MINUTE, CronField.parse(CronField.FieldType.MINUTE, parts[1]));
            fields.put(CronField.FieldType.HOUR, CronField.parse(CronField.FieldType.HOUR, parts[2]));
            fields.put(CronField.FieldType.DAY_OF_MONTH, CronField.parse(CronField.FieldType.DAY_OF_MONTH, parts[3]));
            fields.put(CronField.FieldType.MONTH, CronField.parse(CronField.FieldType.MONTH, parts[4]));
            fields.put(CronField.FieldType.DAY_OF_WEEK, CronField.parse(CronField.FieldType.DAY_OF_WEEK, parts[5]));
        } else {
            throw new IllegalArgumentException("Invalid cron expression: " + expression);
        }
    }

    public String getExpression() { return expression; }

    public boolean matches(LocalDateTime dateTime) {
        return getFields().stream().allMatch(field -> {
            int value = getFieldValue(dateTime, field.getFieldType());
            return field.matches(value);
        });
    }

    public LocalDateTime nextFireTime(LocalDateTime after) {
        LocalDateTime next = after.plusSeconds(1);
        for (int i = 0; i < 366 * 24 * 60 * 60; i++) {
            if (matches(next)) return next;
            next = next.plusSeconds(1);
        }
        throw new IllegalStateException("No next fire time found for: " + expression);
    }

    public LocalDateTime nextFireTime(int count, LocalDateTime after) {
        LocalDateTime current = after;
        for (int i = 0; i < count; i++) {
            current = nextFireTime(current);
        }
        return current;
    }

    public boolean isSatisfiedBy(LocalDateTime dateTime) {
        return matches(dateTime);
    }

    private Iterable<CronField> getFields() {
        return fields.values();
    }

    private int getFieldValue(LocalDateTime dateTime, CronField.FieldType fieldType) {
        return switch (fieldType) {
            case SECOND -> dateTime.getSecond();
            case MINUTE -> dateTime.getMinute();
            case HOUR -> dateTime.getHour();
            case DAY_OF_MONTH -> dateTime.getDayOfMonth();
            case MONTH -> dateTime.getMonthValue();
            case DAY_OF_WEEK -> dateTime.getDayOfWeek().getValue();
        };
    }

    public static boolean isValid(String expression) {
        try {
            new CronExpression(expression);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public String toString() {
        return expression;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CronExpression other)) return false;
        return expression.equals(other.expression);
    }

    @Override
    public int hashCode() {
        return expression.hashCode();
    }
}
