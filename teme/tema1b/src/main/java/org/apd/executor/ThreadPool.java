package org.apd.executor;
import org.apd.storage.EntryResult;
import org.apd.storage.SharedDatabase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ThreadPool {
    public boolean isReceiving = true;
    List<WorkerThread> threads;
    private final BlockingQueue<StorageTask> taskQueue;
    List<EntryResult> results;
    private LockType lockType;
    private final SharedDatabase sharedDatabase;

    public class WorkerThread extends Thread{
        public static Integer readerNo = 0;
        public static Integer writerNo = 0;
        public static boolean someoneIsReading = false;
        public static boolean someoneIsWriting = false;

        public void run() {
            if (lockType == LockType.ReaderPreferred) {
                runReaderPreffered();
            }
        }

        public void runReaderPreffered() {
            while (isUp()) {
                try {
                    StorageTask task;
                    synchronized (taskQueue) {
                        task = taskQueue.take(); // Fetch the next task
                    }
                    System.out.println("Tasks remaining: " + taskQueue.size() + " from " + this.threadId() + " " + someoneIsReading);
                    try {
                        // Writer
                        if (task.isWrite()) {
                            if(someoneIsReading) {synchronized (WorkerThread.class){WorkerThread.class.wait();}}
                            synchronized (sharedDatabase) {
                                EntryResult newResult = sharedDatabase.addData(task.index(), task.data());
                                results.add(newResult);
                            }
                            // Reader
                        } else {
                            synchronized (ThreadPool.class) {
                                readerNo++;
                                if (readerNo == 1) {
                                    someoneIsReading = true;
                                }
                            }

                                EntryResult newResult = sharedDatabase.getData(task.index());
                                results.add(newResult);

                            synchronized (ThreadPool.class) {
                                readerNo--;
                                if (readerNo == 0) {
                                    someoneIsReading = false;
                                    synchronized (WorkerThread.class){WorkerThread.class.notifyAll();}
                                }
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    ThreadPool(int numberOfThreads, LockType lockType, SharedDatabase sharedDatabase){
        this.threads = new ArrayList<>();
        for (int i = 0; i < numberOfThreads; i++) {
            WorkerThread thread = new WorkerThread();
            this.threads.add(thread);
        }

        this.taskQueue = new LinkedBlockingQueue<>();
        this.results = new ArrayList<EntryResult>();
        this.lockType = lockType;
        this.sharedDatabase = sharedDatabase;
    }

    public void start(){
        for(Thread thread : this.threads) {
            thread.start();
        }
    }

    public void close(){
        for(Thread thread : this.threads){
//            thread.interrupt();
            try {
            thread.join();
            } catch (InterruptedException e) {
                System.err.println(Thread.currentThread().getName() + " was interrupted during sleep.");
            }
        }
    }

    public boolean isUp(){
        return !this.taskQueue.isEmpty() || this.isReceiving;
    }

    public void addTask(StorageTask task){
        taskQueue.add(task);
    }
}
