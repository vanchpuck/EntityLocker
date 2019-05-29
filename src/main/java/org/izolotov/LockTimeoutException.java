package org.izolotov;

/**
 * Thrown when the thread could not acquire the lock within timeout
 */
public class LockTimeoutException extends Exception {

    public LockTimeoutException(String message) {
        super(message);
    }

}
