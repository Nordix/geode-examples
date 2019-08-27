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

import java.util.List;
import java.util.stream.Collectors;

public class DbObject {

  public String getName() {
    return name;
  }

  public DbObject setName(String name) {
    this.name = name;
    return this;
  }

  public List<String> getList() {
    return list;
  }

  public DbObject setList(List<String> list) {
    this.list = list;
    return this;
  }

  public DbObject incList() {
    list = list.stream().map(x -> new Integer(new Integer(x) + 1).toString())
        .collect(Collectors.toList());
    return this;
  }

  public int getNumber() {
    return number;
  }

  private int number;
  private String name;
  private List<String> list;

  public DbObject(int number, String name, List<String> list) {
    this.number = number;
    this.name = name;
    this.list = list;
  }

  public DbObject() {

  }

  @Override
  public String toString() {
    return ("Name: " + name + ", number: " + number + ", list: " + list.toString());
  }
}
