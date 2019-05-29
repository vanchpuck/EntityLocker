package org.izolotov;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;

/**
 * Just kind of smoke test kit.
 */
public class EntityLockerTest {

    private static final long TASK_EXECUTION_TIME = 2000L;

    private static class SpecificId extends EntityId {

        private final String extraData;

        SpecificId(int id, String data) {
            super(id);
            this.extraData = data;
        }

        public String getExtraData() {
            return extraData;
        }
    }

    private static class DummyConsumer<T extends EntityId> implements Consumer<List<T>> {

        @Override
        public void accept(List<T> t) {
            try {
                Thread.sleep(TASK_EXECUTION_TIME);
                System.out.println("Hello, world!");
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    public void entityLockTest() throws InterruptedException {
        SpecificId record1 = new SpecificId(1, "record_1");
        SpecificId record2 = new SpecificId(2, "record_2");
        SpecificId record3 = new SpecificId(3, "record_3");

        EntityLocker<SpecificId> locker = new EntityLocker<>();

        long startTime = System.currentTimeMillis();
        ExecutorService executor = Executors.newFixedThreadPool(10);
        Stream.of(record1, record2, record3).forEach(rec -> {
            executor.execute(() -> {
                try {
                    locker.execute(Arrays.asList(rec), new DummyConsumer<>(), 10000L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        });
        executor.shutdown();
        executor.awaitTermination(10000L, TimeUnit.MILLISECONDS);
        long elapsedTime = System.currentTimeMillis() - startTime;
        // there will be three parallel executions
        assertTrue(elapsedTime < 3 * TASK_EXECUTION_TIME);
    }

    @Test
    public void globalLockTest() throws InterruptedException {
        SpecificId record1 = new SpecificId(1, "record_1");
        SpecificId record2 = new SpecificId(2, "record_2");
        SpecificId record3 = new SpecificId(3, "record_3");

        EntityLocker<SpecificId> locker = new EntityLocker<>();

        long startTime = System.currentTimeMillis();
        ExecutorService executor = Executors.newFixedThreadPool(10);
        Stream.of(record1, record2, record3).forEach(id -> {
            executor.execute(() -> {
                try {
                    locker.executeWithGlobalLock(Arrays.asList(id), new DummyConsumer<>(), 10000L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        });
        executor.shutdown();
        executor.awaitTermination(10000L, TimeUnit.MILLISECONDS);
        long elapsedTime = System.currentTimeMillis() - startTime;
        // nothing will be executed in parallel
        assertTrue(elapsedTime > 3 * TASK_EXECUTION_TIME);

    }

    @Test
    public void reentrancyTest() throws InterruptedException {
        SpecificId record1 = new SpecificId(1, "record_1");

        EntityLocker<SpecificId> locker = new EntityLocker<>();

        long lockTimeout = 10000L;
        long startTime = System.currentTimeMillis();
        ExecutorService executor = Executors.newFixedThreadPool(10);
        executor.execute(() -> {
            try {
                locker.execute(Arrays.asList(record1, record1, record1), new DummyConsumer<>(), lockTimeout);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        executor.shutdown();
        executor.awaitTermination(10000L, TimeUnit.MILLISECONDS);
        long elapsedTime = System.currentTimeMillis() - startTime;
        // if the reentrany is not provided the execution will fail with lock timeout
        assertTrue(elapsedTime < lockTimeout);
    }

    @Test
    public void escalationTest() throws InterruptedException {
        SpecificId record1 = new SpecificId(1, "record_1");
        SpecificId record2 = new SpecificId(2, "record_2");
        SpecificId record3 = new SpecificId(3, "record_3");
        SpecificId record4 = new SpecificId(4, "record_4");
        SpecificId record5 = new SpecificId(5, "record_5");
        SpecificId record6 = new SpecificId(6, "record_6");
        SpecificId record7 = new SpecificId(7, "record_7");
        SpecificId record8 = new SpecificId(8, "record_8");
        SpecificId record9 = new SpecificId(9, "record_9");

        EntityLocker<SpecificId> locker = new EntityLocker<>(1);

        ExecutorService executor = Executors.newFixedThreadPool(10);
        long startTime = System.currentTimeMillis();
        Stream.of(
                Arrays.asList(record1, record2, record3),
                Arrays.asList(record4, record5, record6),
                Arrays.asList(record7, record8, record9)
        ).forEach(ids -> {
            executor.execute(() -> {
                try {
                    locker.execute(ids, new DummyConsumer<>(), 10000L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        });
        executor.shutdown();
        executor.awaitTermination(10000L, TimeUnit.MILLISECONDS);
        long elapsedTime = System.currentTimeMillis() - startTime;
        // at least two threads should wait for global lock, so nothing will be executed in parallel
        assertTrue(elapsedTime > TASK_EXECUTION_TIME * 3);
    }

    @Test(expected = LockTimeoutException.class)
    public void lockTimeoutTest() throws Throwable {
        SpecificId record1 = new SpecificId(1, "record_1");
        SpecificId record2 = new SpecificId(2, "record_2");

        EntityLocker<SpecificId> locker = new EntityLocker<>();

        ExecutorService executor = Executors.newFixedThreadPool(10);
        List<Future> futures = Stream.of(Arrays.asList(record1), Arrays.asList(record2)).map(ids -> {
            Future<Void> future = executor.submit((Callable<Void>) () -> {
                // lock timeout is less then the task execution time
                // so the second thread will not be able ro acquire the lock in time
                locker.executeWithGlobalLock(ids, new DummyConsumer<>(), TASK_EXECUTION_TIME / 2);
                return null;
            });
            return future;
        }).collect(Collectors.toList());
        executor.shutdown();

        for (Future<Void> future : futures) {
            try {
                future.get();
            } catch (ExecutionException e) {
                throw e.getCause();
            }
        }
    }

}
