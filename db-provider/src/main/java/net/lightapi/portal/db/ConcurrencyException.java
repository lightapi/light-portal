package net.lightapi.portal.db;

import java.sql.SQLException;

public class ConcurrencyException extends SQLException {
    public ConcurrencyException(String message) { super(message); }
    public ConcurrencyException(String message, Throwable cause) { super(message, cause); }
}
