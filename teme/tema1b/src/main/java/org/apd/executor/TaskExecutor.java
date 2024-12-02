package org.apd.executor;

import org.apd.storage.EntryResult;
import org.apd.storage.SharedDatabase;

import java.util.ArrayList;
import java.util.List;

/* DO NOT MODIFY THE METHODS SIGNATURES */
public class TaskExecutor {
    private final SharedDatabase sharedDatabase;

    public TaskExecutor(int storageSize, int blockSize, long readDuration, long writeDuration) {
        sharedDatabase = new SharedDatabase(storageSize, blockSize, readDuration, writeDuration);
    }

    public List<EntryResult> ExecuteWork(int numberOfThreads, List<StorageTask> tasks, LockType lockType) {
        /* IMPLEMENT HERE THE THREAD POOL, ASSIGN THE TASKS AND RETURN THE RESULTS */
        // ReaderPreffered

        ThreadPool threadPool = new ThreadPool(numberOfThreads, lockType, sharedDatabase);

//        System.out.println("Starting Pool\n");
        threadPool.start();
//        System.out.println("Pool started\n");

        for(StorageTask task : tasks) {
            threadPool.addTask(task);
        }

//        System.out.println("Added " + tasks.size() + " tasks to Pool\n");

        threadPool.isReceiving = false;

        while(threadPool.isUp()){}

        threadPool.close();

        return threadPool.results;

    }

    public List<EntryResult> ExecuteWorkSerial(List<StorageTask> tasks) {
        var results = tasks.stream().map(task -> {
            try {
                if (task.isWrite()) {
                    return sharedDatabase.addData(task.index(), task.data());
                } else {
                    return sharedDatabase.getData(task.index());
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).toList();

        return results.stream().toList();
    }
}
