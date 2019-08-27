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
package org.apache.geode_examples.queries;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;


public class Example {
  static long start, end;
  static String REGIONNAME = "example-region";
  static String QUERY1 = "SELECT DISTINCT * FROM /" + REGIONNAME;
  static String QUERY2 = "SELECT DISTINCT * FROM /" + REGIONNAME + " h WHERE h.hoursPerWeek < 40";
  static String QUERY3 = "SELECT DISTINCT * FROM /" + REGIONNAME + " x WHERE x.lastName=$1";

  static final int NUM_EMPLOYEES = 1000;
  static final int NUM_GETS = 100;
  static final int NUM_QUERIES = 1;

  static Region<Integer, EmployeeData> region;
  static ClientCache cache;

  public static void main(String[] args) throws NameResolutionException, TypeMismatchException,
      QueryInvocationTargetException, FunctionDomainException {
    // connect to the locator using default port 10334
    cache = new ClientCacheFactory().addPoolLocator("127.0.0.1", 10334).set("log-level", "WARN")
        .setPoolRetryAttempts(0)
        .setPoolReadTimeout( )
        .setPdxSerializer(
            new ReflectionBasedAutoSerializer("org.apache.geode_examples.queries.EmployeeData"))
        .create();

    // create a region on the server
    region = cache.<Integer, EmployeeData>createClientRegionFactory(ClientRegionShortcut.PROXY)
        .create(REGIONNAME);

    // create a set of employee data and put it into the region
    Map<Integer, EmployeeData> employees = createEmployeeData();
    for (Integer employeeKey : employees.keySet()) {
      region.put(employeeKey, employees.get(employeeKey));
    }

    runQueries();
    // for (int i = 0; i < 10000; i++) {
    // runFunction();
    // }

    region.removeAll(employees.keySet());
    cache.close();
  }

  private static void runFunction() {
    TestFun myFun = new TestFun();
    FunctionService.registerFunction(myFun);

    Execution execution = FunctionService.onRegion(region);
    ResultCollector<Integer, List> results = execution.execute(TestFun.ID);
    System.out.println("TestFun returned : " + results.getResult());
  }

  private static void runQueries() throws NameResolutionException, TypeMismatchException,
      QueryInvocationTargetException, FunctionDomainException {


    System.out.println("-------------- Starting get operations...");
    long times = 0L;
    for (int i = 0; i < NUM_GETS; i++) {
      start = System.currentTimeMillis();
      // region.keySetOnServer().forEach(key -> region.get(key));
      for (int k : region.keySetOnServer()) {
        System.out.println(region.get(k).toString());
      }
      end = System.currentTimeMillis();
      times += (end - start);
    }
    System.out.println("----- Average Get for each get (" + NUM_GETS + " times): "
        + (times / NUM_GETS) + " milliseconds");

    times = 0L;
    for (int i = 0; i < NUM_QUERIES; i++) {
      start = System.currentTimeMillis();
      // do a set of queries, printing the results of each query
      doQueries(cache);
      end = System.currentTimeMillis();
      times += (end - start);
    }
    System.out.println(
        "----- Queries (" + NUM_QUERIES + " times): " + (times / NUM_QUERIES) + " milliseconds");


  }



  public static Map<Integer, EmployeeData> createEmployeeData() {

    int emplNumber = 10000;

    // put data into the hashmap
    Map<Integer, EmployeeData> employees = new HashMap<Integer, EmployeeData>();
    for (int index = 0; index < NUM_EMPLOYEES; index++) {
      emplNumber = emplNumber + index;
      EmployeeData value = generateRandomEmployee();
      employees.put(emplNumber, value);
    }

    return employees;
  }

  private static EmployeeData generateRandomEmployee() {
    String[] firstNames =
        "Alex,Bertie,Kris,Dale,Frankie,Jamie,Morgan,Pat,Ricky,Taylor,Casey,Jessie,Ryan,Skyler"
            .split(",");
    String[] lastNames =
        "Able,Bell,Call,Driver,Forth,Jive,Minnow,Puts,Reliable,Tack,Catch,Jam,Redo,Skip".split(",");
    int index = (int) (Math.random() * firstNames.length);
    String email = firstNames[index] + "." + lastNames[index] + "@example.com";
    return new EmployeeData(firstNames[index], lastNames[index], 100, email, 50000, 40);
  }


  // Demonstrate querying using the API by doing 3 queries.
  public static void doQueries(ClientCache cache) throws NameResolutionException,
      TypeMismatchException, QueryInvocationTargetException, FunctionDomainException {
    QueryService queryService = cache.getQueryService();

    // Query for every entry in the region, and print query results.
    // System.out.println("\nExecuting query: " + QUERY1);
    SelectResults<EmployeeData> results =
        (SelectResults<EmployeeData>) queryService.newQuery(QUERY1).execute();
    // printSetOfEmployees(results);

    // Query for all part time employees, and print query results.
    // System.out.println("\nExecuting query: " + QUERY2);
    results = (SelectResults<EmployeeData>) queryService.newQuery(QUERY2).execute();
    // printSetOfEmployees(results);

    // Query for last name of Jive, and print the full name and employee number.
    // System.out.println("\nExecuting query: " + QUERY3);
    results =
        (SelectResults<EmployeeData>) queryService.newQuery(QUERY3).execute(new String[] {"Jive"});
    // for (EmployeeData eachEmployee : results) {
    // System.out.println(String.format("Employee %s %s has employee number %d",
    // eachEmployee.getFirstName(), eachEmployee.getLastName(), eachEmployee.getEmplNumber()));
    // }
  }

  private static void printSetOfEmployees(SelectResults<EmployeeData> results) {
    System.out.println("Query returned " + results.size() + " results.");
    for (EmployeeData eachEmployee : results) {
      System.out.println(String.format("Employee: %s", eachEmployee.toString()));
    }
  }
}
