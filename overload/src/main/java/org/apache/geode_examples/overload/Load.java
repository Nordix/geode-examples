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


/**
 * Generates Load on the CPU by keeping it busy for the given load percentage
 * 
 * @author Sriram
 */
public class Load {
  /**
   * Starts the Load Generation
   * 
   * @param args Command line arguments, ignored
   */
  public static void main(String[] args) {
    final int numCore = new Integer(args[0]).intValue();
    final int numThreadsPerCore = new Integer(args[1]).intValue();
    final double load = new Double(args[2]).doubleValue();
    final long duration = new Long(args[3]).longValue();
    Load loadInstance = new Load(numCore, numThreadsPerCore, load, duration);
    loadInstance.start();
  }

  private int numCore = 2;
  private int numThreadsPerCore = 2;
  private double load = 0.8;
  private long duration = 100;

  public Load() {}

  public Load(int numCore, int numThreadsPerCore, double load, long duration) {
    this.numCore = numCore;
    this.duration = duration * 1000;
    this.load = load;
    this.numThreadsPerCore = numThreadsPerCore;
  }

  public void start() {
    for (int thread = 0; thread < numCore * numThreadsPerCore; thread++) {
      new BusyThread("Thread" + thread, load, duration).start();
    }
  }

  /**
   * Thread that actually generates the given load
   * 
   * @author Sriram
   */
  private static class BusyThread extends Thread {
    private double load;
    private long duration;

    /**
     * Constructor which creates the thread
     * 
     * @param name Name of this thread
     * @param load Load % that this thread should generate
     * @param duration Duration that this thread should generate the load for
     */
    public BusyThread(String name, double load, long duration) {
      super(name);
      this.load = load;
      this.duration = duration;
    }

    /**
     * Generates the load when run
     */
    @Override
    public void run() {
      long startTime = System.currentTimeMillis();
      try {
        // Loop for the given duration
        while (System.currentTimeMillis() - startTime < duration) {
          // Every 100ms, sleep for the percentage of unladen time
          if (System.currentTimeMillis() % 100 == 0) {
            Thread.sleep((long) Math.floor((1 - load) * 100));
          }
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
