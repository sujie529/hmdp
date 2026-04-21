package com.hmdp.utils;

import org.springframework.stereotype.Component;


public interface ILock {

    /**
     * 尝试获取锁
     * @param timeoutSec 锁的过期时间
     * @return true获取成功;false获取失败
     */
    boolean tryLock(long timeoutSec);

    /**
     * 释放锁
     */
    void unlock();
}
