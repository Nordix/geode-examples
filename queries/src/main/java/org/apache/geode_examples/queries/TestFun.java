package org.apache.geode_examples.queries;
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


import java.util.Iterator;
import java.util.Set;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;


/**
 * Function to get timeout
 */
public class TestFun implements Function {
  public static final String ID = TestFun.class.getSimpleName();
  private static final Logger logger = LogService.getLogger();

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public void execute(FunctionContext context) {
    logger.info("Running " + this.getClass().getName());
    logger.info("EALBBUS");
    for (StackTraceElement elem : Thread.currentThread().getStackTrace()) {
      logger.info(elem.toString());
    }
    RegionFunctionContext regionContext = (RegionFunctionContext) context;
    Region region = regionContext.getDataSet();
    Set<Integer> keys = region.keySet();
    Iterator keysIterator = keys.iterator();
    Object data = null;
    while (keysIterator.hasNext()) {
      int k = (Integer) keysIterator.next();
      // logger.info("Key=" + k);
      // data = region.get(k);
    }
    context.getResultSender().lastResult(data);
  }

}
