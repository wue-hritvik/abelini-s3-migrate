package com.abelini_s3_migrate.extra;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SkuStats {
    AtomicInteger imagesFetched = new AtomicInteger();

    AtomicInteger imagesToUpdate = new AtomicInteger();
    AtomicInteger updateProcessed = new AtomicInteger();
    AtomicInteger updateSuccess = new AtomicInteger();
    AtomicInteger updateFailed = new AtomicInteger();
    AtomicInteger updateTotalBatch = new AtomicInteger();
    AtomicInteger updateBatchProcessed = new AtomicInteger();
    AtomicInteger updateBatchSuccess = new AtomicInteger();
    AtomicInteger updateBatchFailed = new AtomicInteger();
    List<String> updateFailedBatchList = new CopyOnWriteArrayList<>();
    Map<String, String> failedUpdateMap = new ConcurrentHashMap<>();

    AtomicInteger imagesToCreate = new AtomicInteger();
    AtomicInteger createProcessed = new AtomicInteger();
    AtomicInteger createSuccess = new AtomicInteger();
    AtomicInteger createFailed = new AtomicInteger();
    AtomicInteger createTotalBatch = new AtomicInteger();
    AtomicInteger createBatchProcessed = new AtomicInteger();
    AtomicInteger createBatchSuccess = new AtomicInteger();
    AtomicInteger createBatchFailed = new AtomicInteger();
    List<String> createFailedBatchList = new CopyOnWriteArrayList<>();
    List<String> failedCreateList = new CopyOnWriteArrayList<>();
}
