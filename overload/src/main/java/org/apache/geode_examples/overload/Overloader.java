/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode_examples.overload;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;
import org.apache.geode_examples.overload.OverloadFun;

public class Overloader {
  private final Region region;
  private final Object[] data;
  private final String name;

  public Overloader(String name, Region region, Object[] data) {
    this.region = region;
    this.data = data;
    this.name = name;
  }

  public static void main(String[] args) {
    // connect to the locator using default port 10334
    ClientCache cache = new ClientCacheFactory().addPoolLocator("127.0.0.1", 10334)
        .setPdxSerializer(new ReflectionBasedAutoSerializer("org.apache.geode_examples.overload.*"))
        .set("log-level", "WARN").create();

    // create a local region that matches the server region
    Region region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("overload");

    OverloadFun myFun = new OverloadFun();
    FunctionService.registerFunction(myFun);



    int SIZE = new Integer(args[0]);
    Object[] data = createData(SIZE);

    Overloader insertoverloader = new Overloader("init", region, data);
    insertoverloader.insertValues();
    insertoverloader.readValues();


    int tasks = new Integer(args[1]);
    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(tasks);

    for (int i = 1; i <= tasks; i++) {
      Overloader overloader = new Overloader(new Integer(i).toString(), region, data);

      Overloader.overloaderTask task = overloader.new overloaderTask("Task " + i, overloader);
      System.out.println("Created: " + task.getName());

      executor.execute(task);
    }
    executor.shutdown();
    try {
      executor.awaitTermination(3600, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    cache.close();
  }

  private static Object[] createData(int size) {
    Object[] data = new Object[size];
    for (int i = 0; i < size; i++) {
      data[i] = createObject(i);
    }
    return data;
  }

  private static Object createObject(int i) {
    List<String> list = new ArrayList<String>(Collections.nCopies(i + 100, "0"));
    list = list.stream().map(x -> new Integer(i).toString()).collect(Collectors.toList());

    Object object = new DbObject(i, new Integer(i).toString(), list);
    return object;
  }

  String getName() {
    return name;
  }

  Set<String> getKeysOnServer() {
    return new HashSet<>(region.keySetOnServer());
  }

  void insertValues() {
    IntStream.range(0, data.length).forEach(index -> region.put(index, data[index]));
  }

  void readValues() {
    IntStream.range(0, data.length).mapToObj(index -> region.get(index))
        .forEach(System.out::println);
  }

  void executeQueries() {
    IntStream.range(0, data.length).forEach(x -> region.get(x));
  }

  void executeQuery(int key) {
    region.get(key);
  }

  void executePut(int x) {
    region.put(x, ((DbObject) region.get(x))
        .setName(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new Date())).incList());
  }

  void executeFunction(int x) {
    int key = x;
    Set filter = new HashSet();
    filter.add(key);
    Object args = new Integer(key).toString();
    Execution execution = FunctionService.onRegion(region).withFilter(filter).setArguments(args);
    ResultCollector<Integer, List> resultCollector = execution.execute(OverloadFun.ID);
    List result = (List) resultCollector.getResult();
  }

  void executePuts() {
    IntStream.range(0, data.length).forEach(x -> region.put(x, ((DbObject) region.get(x))
        .setName(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new Date())).incList()));
  }

  private void executeFunction() {
    for (int i = 0; i < data.length; i++) {
      int key = i;
      Set filter = new HashSet();
      filter.add(key);
      Object args = new Integer(key).toString();
      Execution execution = FunctionService.onRegion(region).withFilter(filter).setArguments(args);
      ResultCollector<Integer, List> resultCollector = execution.execute(OverloadFun.ID);
      List result = (List) resultCollector.getResult();
    }
  }

  public class overloaderTask implements Runnable {
    private Overloader overloader;
    private String name;

    public overloaderTask(String name, Overloader overloader) {
      this.overloader = overloader;
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public void run() {
      loopDbOperations(overloader);
    }
  }

  private void loopDbOperations(Overloader overloader) {
    long startTime = 0;
    long endTime = 0;

    for (int i = 0; i < 100000; i++) {

      if (i % 100 == 0) {
        // System.out.println("loopDbOperations : "+i);
        startTime = System.nanoTime();
      }

      try {
        // overloader.executeQueries();
        // overloader.executePuts();
        overloader.getKeysOnServer();
        // overloader.executeFunction();
        int key = i % data.length;
        // overloader.executeQuery(key);
        // overloader.executePut(key);
        // overloader.executeFunction(key);

      } catch (Exception e) {
        System.out.println(name + ": Exception in geode call");
      }

      try {
        Thread.sleep(20);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

      if (i % 100 == 0) {
        // overloader.readValues();
        endTime = System.nanoTime();

        // get difference of two nanoTime values
        long timeElapsed = endTime - startTime;

        // System.out.println("Execution time in nanoseconds : " + timeElapsed);
        System.out.println(
            overloader.getName() + ": Execution time in milliseconds : " + timeElapsed / 1000000);
      }
    }
  }
}
