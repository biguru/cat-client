package com.dianping.cat;

import com.dianping.cat.util.Threads;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;

@Slf4j
public  final class CatThreadListener extends Threads.AbstractThreadListener {

    @Override
    public void onThreadGroupCreated(ThreadGroup group, String name) {
        log.info(String.format("Thread group(%s) created.", name));
    }

    @Override
    public void onThreadPoolCreated(ExecutorService pool, String name) {
        log.info(String.format("Thread pool(%s) created.", name));
    }

    @Override
    public void onThreadStarting(Thread thread, String name) {
        log.info(String.format("Starting thread(%s) ...", name));
    }

    @Override
    public void onThreadStopping(Thread thread, String name) {
        log.info(String.format("Stopping thread(%s).", name));
    }

    @Override
    public boolean onUncaughtException(Thread thread, Throwable e) {
        log.error(String.format("Uncaught exception thrown out of thread(%s)", thread.getName()), e);
        return true;
    }
}

