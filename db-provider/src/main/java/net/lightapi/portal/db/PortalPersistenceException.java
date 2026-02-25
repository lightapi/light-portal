package net.lightapi.portal.db;

public class PortalPersistenceException extends RuntimeException {
    public PortalPersistenceException(String message) {
        super(message);
    }

    public PortalPersistenceException(String message, Throwable cause) {
        super(message, cause);
    }

    public PortalPersistenceException(Throwable cause) {
        super(cause);
    }
}
