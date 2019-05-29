package org.izolotov;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * Utility class that provides synchronization mechanism similar to row-level DB locking.
 * @param <T> entity ID type
 */
public class EntityLocker<T extends EntityId> {

    public static final int DISABLE_LOCK_ESCALATION = -1;

    private final Lock globalLock;
    private final int globalLockThreshold;

    public EntityLocker() {
        this(DISABLE_LOCK_ESCALATION);
    }

    /**
     * @param lockEscalationThreshold if thread has locked more entities than this value,
     *                                its lock will be escalated to be a global lock.
     *                                If negative then the lock escalation will be disabled.
     */
    public EntityLocker(int lockEscalationThreshold) {
        this.globalLock = new ReentrantLock();
        this.globalLockThreshold = lockEscalationThreshold;
    }

    /**
     * Execute the protected code when all entity locks acquired
     * @param ids list of ids
     * @param protectedCode code that should have exclusive access to the entities
     * @param lockTimeout timeout to acquire the lock
     * @throws InterruptedException
     * @throws LockTimeoutException
     */
    public void execute(List<T> ids, Consumer<List<T>> protectedCode, long lockTimeout) throws InterruptedException, LockTimeoutException {
        executeWithrecursiveLocking(ids, protectedCode, lockTimeout, 0);
    }

    private void executeWithrecursiveLocking(List<T> ids, Consumer<List<T>> protectedCode, long lockTimeout, final int level) throws InterruptedException, LockTimeoutException {
        if (level < ids.size()) {
            if (level > globalLockThreshold && globalLockThreshold > 0) {
                executeWithGlobalLock(ids, protectedCode, lockTimeout);
            } else {
                EntityId entity = ids.get(level);
                // setting up a timeout is a simplest mechanism to avoid deadlocks
                // some advanced technics like retries could be used
                // but lets keep the sings simple this time
                if (entity.lock().tryLock(lockTimeout, TimeUnit.MILLISECONDS)) {
                    try {
                        executeWithrecursiveLocking(ids, protectedCode, lockTimeout, level + 1);
                    } finally {
                        entity.lock().unlock();
                    }
                } else {
                    throw new LockTimeoutException(String.format("Can't acquire the entity log within %d milliseconds", lockTimeout));
                }
            }
        } else {
            protectedCode.accept(ids);
        }
    }

    /**
     * Protected code will not be executed concurrently with any other protected code.
     * @param ids list of ids
     * @param protectedCode code that should have exclusive access to the entities
     * @param lockTimeout timeout to acquire the lock
     * @throws InterruptedException
     * @throws LockTimeoutException
     */
    public void executeWithGlobalLock(List<T> ids, Consumer<List<T>> protectedCode, long lockTimeout) throws InterruptedException, LockTimeoutException {
        if (globalLock.tryLock(lockTimeout, TimeUnit.MILLISECONDS)) {
            try {
                protectedCode.accept(ids);
            } finally {
                globalLock.unlock();
            }
        } else {
            throw new LockTimeoutException(String.format("Can't acquire the globalLock within %d milliseconds", lockTimeout));
        }
    }

}
