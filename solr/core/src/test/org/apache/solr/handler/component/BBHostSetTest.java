package org.apache.solr.handler.component;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;

public class BBHostSetTest extends SolrTestCaseJ4 {
  private static Random r;

  private static Replica hostA;
  private static Replica hostB;
  private static Replica hostC;

  private static LinkedList<Replica> liveAB;
  private static LinkedList<Replica> liveBA;

  private static LinkedList<Replica> liveABC;
  private static LinkedList<Replica> liveACB;
  private static LinkedList<Replica> liveBAC;
  private static LinkedList<Replica> liveBCA;
  private static LinkedList<Replica> liveCAB;
  private static LinkedList<Replica> liveCBA;

  private static String signatureAB;
  private static String signatureBA;

  private static String signatureABC;
  private static String signatureACB;
  private static String signatureBAC;
  private static String signatureBCA;
  private static String signatureCAB;
  private static String signatureCBA;

  private static HashMap<String, Object> genPropMap(String node_name) {
    HashMap<String, Object>  pmap = new HashMap<String, Object>();
    pmap.put(ZkStateReader.NODE_NAME_PROP, node_name);
    return pmap;
  }
  private static String formReplicaOrderSig(List<Replica> replicas) {
    String replicaSig = "";
    String delim = "";
    for (Replica replica: replicas) {
      replicaSig += (delim + BBHostSet.node_name(replica));
      delim = ",";
    }
    return replicaSig;
  }


  private static LinkedList<Replica> copy(LinkedList<Replica> replicas) {
    return (LinkedList<Replica>) replicas.clone();
  }

  private int randomBetween(int low, int high) {
    // inclusive of both endpoints
    return (low + r.nextInt(1+(high-low)));
  }

  private int getOrDefault(HashMap<String, Integer> stats, String key, int defaultValue) {
    if (stats.containsKey(key)) {
      return stats.get(key);
    }
    return defaultValue;
  }
  
  @BeforeClass
  private static void testInit() {
    r = random();

    hostA = new Replica("hostA", genPropMap("hostA"));
    hostB = new Replica("hostB", genPropMap("hostB"));
    hostC = new Replica("hostC", genPropMap("hostC"));

    liveAB = new LinkedList<Replica>();
    liveAB.addLast(hostA);
    liveAB.addLast(hostB);

    liveBA = new LinkedList<Replica>();
    liveBA.addLast(hostB);
    liveBA.addLast(hostA);

    signatureAB = formReplicaOrderSig(liveAB);
    signatureBA = formReplicaOrderSig(liveBA);

    liveABC = new LinkedList<Replica>();
    liveABC.addLast(hostA);
    liveABC.addLast(hostB);
    liveABC.addLast(hostC);

    liveACB = new LinkedList<Replica>();
    liveACB.addLast(hostA);
    liveACB.addLast(hostC);
    liveACB.addLast(hostB);

    liveBAC = new LinkedList<Replica>();
    liveBAC.addLast(hostB);
    liveBAC.addLast(hostA);
    liveBAC.addLast(hostC);

    liveBCA = new LinkedList<Replica>();
    liveBCA.addLast(hostB);
    liveBCA.addLast(hostC);
    liveBCA.addLast(hostA);

    liveCAB = new LinkedList<Replica>();
    liveCAB.addLast(hostC);
    liveCAB.addLast(hostA);
    liveCAB.addLast(hostB);

    liveCBA = new LinkedList<Replica>();
    liveCBA.addLast(hostC);
    liveCBA.addLast(hostB);
    liveCBA.addLast(hostA);

    signatureABC = formReplicaOrderSig(liveABC);
    signatureACB = formReplicaOrderSig(liveACB);
    signatureBAC = formReplicaOrderSig(liveBAC);
    signatureBCA = formReplicaOrderSig(liveBCA);
    signatureCAB = formReplicaOrderSig(liveCAB);
    signatureCBA = formReplicaOrderSig(liveCBA);

  }

  @Test
  public void checkDelimiters() throws Exception {
    assertEquals("HOST_SET_DELIM", ";", BBHostSet.HOST_SET_DELIM);
    assertEquals("WEIGHT_HOST_DELIM", ":", BBHostSet.WEIGHT_HOST_DELIM);
    assertEquals("HOST_DELIM", ",", BBHostSet.HOST_DELIM);
  }

  @Test
  public void testTwoEqual() throws Exception {
    final int weight = randomBetween(10, 100);
    final String replicaStrategy = Integer.toString(weight) + ":hostA,hostB";

    final int permutationMod = randomBetween(5, 50)*2;

    LinkedList<Replica> live = null;

    for (int permutationSeed = 0; permutationSeed < permutationMod; permutationSeed++) {
      BBHostSet hostSet = new BBHostSet(replicaStrategy, permutationSeed, permutationMod, r);

      live = copy(liveAB);
      assertTrue("transform", hostSet.transform(live));
      if (permutationSeed < permutationMod/2) {
        assertEquals("initial AB; seed " + permutationSeed, signatureAB, formReplicaOrderSig(live));
      } else {
        assertEquals("initial AB; seed " + permutationSeed, signatureBA, formReplicaOrderSig(live));
      }

      live = copy(liveBA);
      assertTrue("transform", hostSet.transform(live));
      if (permutationSeed < permutationMod/2) {
        assertEquals("initial BA; seed " + permutationSeed, signatureAB, formReplicaOrderSig(live));
      } else {
        assertEquals("initial BA; seed " + permutationSeed, signatureBA, formReplicaOrderSig(live));
      }
    }
  }

  public void implTestABcThree(String replicaStrategy) {
    final int permutationMod = randomBetween(5, 50)*2;

    LinkedList<Replica> liveAll = copy(liveABC);
    Collections.shuffle(liveAll, r);

    LinkedList<Replica> live = null;

    for (int permutationSeed = 0; permutationSeed < permutationMod; permutationSeed++) {
      BBHostSet hostSet = new BBHostSet(replicaStrategy, permutationSeed, permutationMod, r);

      live = copy(liveAll);
      assertTrue("transform", hostSet.transform(live));
      if (permutationSeed < permutationMod/2) {
        assertEquals("seed " + permutationSeed, signatureABC, formReplicaOrderSig(live));
      } else {
        assertEquals("seed " + permutationSeed, signatureBAC, formReplicaOrderSig(live));
      }
    }
  }

  @Test
  public void testThreeW0() throws Exception {
    final int weight = randomBetween(10, 100);
    final String replicaStrategy = Integer.toString(weight) + ":hostA,hostB;0:hostC";
    implTestABcThree(replicaStrategy);
  }

  @Test
  public void testThreeUn() throws Exception {
    final int weight = randomBetween(10, 100);
    final String replicaStrategy = Integer.toString(weight) + ":hostA,hostB";
    implTestABcThree(replicaStrategy);
  }

  @Test
  public void testThreeNeg() throws Exception {
    final int weight = randomBetween(10, 100);
    final String replicaStrategy = Integer.toString(weight) + ":hostA,hostB;-1:hostC";
    implTestABcThree(replicaStrategy);
  }

  @Test
  public void testThreeWeighted() throws Exception {
    final String replicaStrategy = "50:hostA;25:hostB,hostC";

    LinkedList<Replica> liveAll = copy(liveABC);
    Collections.shuffle(liveAll, r);

    LinkedList<Replica> live = null;

    final int magnifier = (r.nextBoolean() ? 1 : 10);
    final int permutationMod = 12 * magnifier;
    for (int permutationSeed = 0; permutationSeed < permutationMod; permutationSeed++) {
      BBHostSet hostSet = new BBHostSet(replicaStrategy, permutationSeed, permutationMod, r);

      live = copy(liveAll);
      assertTrue("transform", hostSet.transform(live));
      switch (permutationSeed/magnifier) {
      // weights = { hostA=50 hostB=25 hostC=25 } gives totalWeight=100
      // half the cases get hostA first, and amongst those getting hostA first, the second choice equally divided between hostB and hostC
      case 0:
      case 1:
      case 2:
        assertEquals("seed " + permutationSeed, signatureABC, formReplicaOrderSig(live));
        break;
      case 3:
      case 4:
      case 5:
        assertEquals("seed " + permutationSeed, signatureACB, formReplicaOrderSig(live));
        break;
      // quarter of the cases get hostB first, and amongst those getting hostB first, the second choice is unequally divided (hostA has double weight of hostC).
      case 6:
      case 7:
        assertEquals("seed " + permutationSeed, signatureBAC, formReplicaOrderSig(live));
        break;
      case 8:
        assertEquals("seed " + permutationSeed, signatureBCA, formReplicaOrderSig(live));
        break;
      // quarter of the cases get hostC first, and amongst those getting hostC first, the second choice is unequally divided (hostA has double weight of hostB).
      case 9:
      case 10:
        assertEquals("seed " + permutationSeed, signatureCAB, formReplicaOrderSig(live));
        break;
      case 11:
        assertEquals("seed " + permutationSeed, signatureCBA, formReplicaOrderSig(live));
        break;
      }
    }
  }

  @Test
  public void testThreeUnknown() throws Exception {
    final String replicaStrategy = "";
    final int permutationSeed = 0;
    final int permutationMod = 12;
    BBHostSet hostSet = new BBHostSet(replicaStrategy, permutationSeed, permutationMod, r);

    LinkedList<Replica> liveAll = copy(liveABC);
    Collections.shuffle(liveAll, r);

    LinkedList<Replica> live = null;

    HashMap<String, Integer> stats = new HashMap<String, Integer>();
    final int n = permutationMod*100;
    for (int gen = 0; gen < n; gen++) {
      live = copy(liveAll);
      assertTrue("transform", hostSet.transform(live));
      String sig = formReplicaOrderSig(live);
      stats.put(sig, getOrDefault(stats, sig, 0) + 1);
    }

    final double sixthOfCasesMin = .7*(n/6);
    final double sixthOfCasesMax = 1.4*(n/6);
    log.info("stats = {}", stats);
    assertTrue("stats ABC", getOrDefault(stats, signatureABC, 0) > sixthOfCasesMin);
    assertTrue("stats ACB", getOrDefault(stats, signatureACB, 0) > sixthOfCasesMin);
    assertTrue("stats BAC", getOrDefault(stats, signatureBAC, 0) > sixthOfCasesMin);
    assertTrue("stats BCA", getOrDefault(stats, signatureBCA, 0) > sixthOfCasesMin);
    assertTrue("stats CAB", getOrDefault(stats, signatureCAB, 0) > sixthOfCasesMin);
    assertTrue("stats CBA", getOrDefault(stats, signatureCBA, 0) > sixthOfCasesMin);

    assertTrue("stats ABC", getOrDefault(stats, signatureABC, 0) < sixthOfCasesMax);
    assertTrue("stats ACB", getOrDefault(stats, signatureACB, 0) < sixthOfCasesMax);
    assertTrue("stats BAC", getOrDefault(stats, signatureBAC, 0) < sixthOfCasesMax);
    assertTrue("stats BCA", getOrDefault(stats, signatureBCA, 0) < sixthOfCasesMax);
    assertTrue("stats CAB", getOrDefault(stats, signatureCAB, 0) < sixthOfCasesMax);
    assertTrue("stats CBA", getOrDefault(stats, signatureCBA, 0) < sixthOfCasesMax);
  }

  @Test
  public void testThreeWeightedRandom() throws Exception {
    final String replicaStrategy = "50:hostA;25:hostB,hostC";
    final int permutationSeed = 0;
    final int permutationMod = 1;
    BBHostSet hostSet = new BBHostSet(replicaStrategy, permutationSeed, permutationMod, r);

    LinkedList<Replica> liveAll = copy(liveABC);
    Collections.shuffle(liveAll, r);

    LinkedList<Replica> live = null;

    HashMap<String, Integer> stats = new HashMap<String, Integer>();
    final int n = 12*100;

    for (int gen = 0; gen < n; gen++) {
      live = copy(liveAll);
      assertTrue("transform", hostSet.transform(live));
      String sig = formReplicaOrderSig(live);
      stats.put(sig, getOrDefault(stats, sig, 0) + 1);
    }
    final double twelfthOfCasesMin = .7*(n/12);
    final double twelfthOfCasesMax = 1.4*(n/12);
    log.info("stats = {}", stats);
    assertTrue("stats ABC", getOrDefault(stats, signatureABC, 0) > twelfthOfCasesMin*3);
    assertTrue("stats ACB", getOrDefault(stats, signatureACB, 0) > twelfthOfCasesMin*3);
    assertTrue("stats BAC", getOrDefault(stats, signatureBAC, 0) > twelfthOfCasesMin*2);
    assertTrue("stats BCA", getOrDefault(stats, signatureBCA, 0) > twelfthOfCasesMin*1);
    assertTrue("stats CAB", getOrDefault(stats, signatureCAB, 0) > twelfthOfCasesMin*2);
    assertTrue("stats CBA", getOrDefault(stats, signatureCBA, 0) > twelfthOfCasesMin*1);

    assertTrue("stats ABC", getOrDefault(stats, signatureABC, 0) < twelfthOfCasesMax*3);
    assertTrue("stats ACB", getOrDefault(stats, signatureACB, 0) < twelfthOfCasesMax*3);
    assertTrue("stats BAC", getOrDefault(stats, signatureBAC, 0) < twelfthOfCasesMax*2);
    assertTrue("stats BCA", getOrDefault(stats, signatureBCA, 0) < twelfthOfCasesMax*1);
    assertTrue("stats CAB", getOrDefault(stats, signatureCAB, 0) < twelfthOfCasesMax*2);
    assertTrue("stats CBA", getOrDefault(stats, signatureCBA, 0) < twelfthOfCasesMax*1);
  }

}
