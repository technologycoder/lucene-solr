package org.apache.solr.handler.component;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.lang.Integer;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedList;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.solr.common.cloud.Replica;

public class BBHostSet {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final String HOST_SET_DELIM = ";";
  static final String WEIGHT_HOST_DELIM = ":";
  static final String HOST_DELIM = ",";

  private final HashMap<String, Integer> hostWeights;
  private final int permutationSeed;
  private final int permutationMod;
  private final Random r;

  public BBHostSet(String replicaStrategy, int replicaPermutationSeed, int replicaPermutationMod, Random rand) {
    permutationSeed = replicaPermutationSeed;
    permutationMod = replicaPermutationMod;
    hostWeights = create_host_weight_map(replicaStrategy);
    r = rand;
  }

  private static HashMap<String, Integer> create_host_weight_map(String replicaStrategy) {
    HashMap<String, Integer> hostWeights = new HashMap<String, Integer>();
    String[] hostSetStrings = replicaStrategy.split(HOST_SET_DELIM);
    for (String hostSetString: hostSetStrings) {
      if (hostSetString.length() == 0) {
        log.warn("Empty weight:hosts element '{}'", hostSetString);
        continue;
      }
      String[] weightAndHosts = hostSetString.split(WEIGHT_HOST_DELIM);
      String hosts = null;
      Integer weightInt = 0;
      if (weightAndHosts.length == 2) {
        hosts = weightAndHosts[1];
        weightInt = Integer.valueOf(weightAndHosts[0]);
      } else {
        log.warn("Invalid weight:hosts '{}'", hostSetString);
        return null;
      }
      String[] hostArray = hosts.split(HOST_DELIM);
      for (String host: hostArray) {
        hostWeights.put(host, weightInt);
      }
    }
    return hostWeights;
  }

  static String node_name(Replica replica) {
    String[] hostAndPort = replica.getNodeName().split(":");
    if (hostAndPort.length > 0) {
      return hostAndPort[0];
    }
    return "";
  }

  class PickResult {
    int picked;
    int newPermutationMin;
    int newPermutationMax;
  }

  public boolean transform(List<Replica> replicas) {
    if (hostWeights == null) {
      return false;
    }

    // sort old list into canonical order.
    // go through list, look up weights, discarding offline.
    // iteratively pick next Replica.
    // return final Replica list.


    Collections.sort(replicas, new Comparator<Replica>() {
        @Override
        public int compare(final Replica lhs, final Replica rhs) {
          return String.CASE_INSENSITIVE_ORDER.compare(lhs.getNodeName(), rhs.getNodeName());
        }
      } );

    int permutationMin = 0;
    int permutationMax = permutationMod;

    LinkedList<Replica> replicaPosWeight = new LinkedList<Replica>();
    LinkedList<Replica> replicaZeroWeight = new LinkedList<Replica>();
    LinkedList<Replica> replicaNegWeight = new LinkedList<Replica>();

    LinkedList<Integer> weightsPos = new LinkedList<Integer>();
    int totalPosWeight = 0;

    for (Replica replica: replicas) {
      String node = node_name(replica);
      if (node.isEmpty()) continue;
      int weight = hostWeights.getOrDefault(node, 0);

      if (weight < 0) {
        replicaNegWeight.addLast(replica);
      } else if (weight > 0) {
        replicaPosWeight.addLast(replica);
        weightsPos.addLast(weight);
        totalPosWeight += weight;
      } else {
        replicaZeroWeight.addLast(replica);
      }
    }

    replicas.clear();

    // Pick in 4 phases:
    // (1) Weighted pick based on seed and mod.
    while (replicaPosWeight.size() > 0) {
      if (permutationMax <= permutationMin + 1) {
        // Our "mod" is only large enough to deterministically control the beginning of the ordered list.
        // We need to order the remaining hosts using "random".
        break;
      }
      PickResult pick = pickHost(weightsPos, totalPosWeight, permutationSeed, permutationMin, permutationMax);
      permutationMin = pick.newPermutationMin;
      permutationMax = pick.newPermutationMax;
      totalPosWeight -= weightsPos.remove(pick.picked);
      Replica picked = replicaPosWeight.remove(pick.picked);
      replicas.add(picked);
    }

    // Handle all remaining Replicas with positive weight that the previous loop wasn't able to finish.
    // (2) Weighted pick based on rand().0
    while (replicaPosWeight.size() > 0) {
      PickResult pick = pickHost(weightsPos, totalPosWeight, r.nextInt(totalPosWeight), 0, totalPosWeight);
      totalPosWeight -= weightsPos.remove(pick.picked);
      Replica picked = replicaPosWeight.remove(pick.picked);
      replicas.add(picked);
    }

    // (3) Unweighted pick based on rand().
    Collections.shuffle(replicaZeroWeight, r);
    replicas.addAll(replicaZeroWeight);

    // (4) Unweighted pick based on rand().
    Collections.shuffle(replicaNegWeight, r);
    replicas.addAll(replicaNegWeight);

    return true;
  }

  // lowSeed <= seed < highSeed.
  // Returns a number x, 0 <= x < weights.size()
  private PickResult pickHost(LinkedList<Integer> weights, int totalWeight, int seed, int lowSeed, int highSeed) {
    PickResult result = new PickResult();
    result.picked = 0;
    result.newPermutationMin = lowSeed;
    result.newPermutationMax = lowSeed;
    int weightSoFar = 0;
    for (int weight : weights) {
      weightSoFar += weight;
      result.newPermutationMin = result.newPermutationMax;
      if (result.picked == (weights.size() - 1)) {
        result.newPermutationMax = highSeed;
        break;
      }
      result.newPermutationMax = lowSeed + Math.round((weightSoFar /(float) totalWeight)*(highSeed - lowSeed));
      if (seed < result.newPermutationMax) {
        break;
      }
      result.picked = result.picked + 1;
    }
    return result;
  }
}
