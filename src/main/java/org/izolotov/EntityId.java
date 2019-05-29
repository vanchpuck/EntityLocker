package org.izolotov;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Base entity ID class
 */
public class EntityId {

    private final int id;
    private final Lock lock;

    public EntityId(int id) {
        this.id = id;
        this.lock = new ReentrantLock();
    }

    public int getId() {
        return id;
    }

    public Lock lock() {
        return lock;
    }

}
