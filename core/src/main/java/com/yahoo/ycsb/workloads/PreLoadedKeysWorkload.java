package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.UniformGenerator;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class PreLoadedKeysWorkload extends CoreWorkload {
  List<String> preLoadedKeys;
  UniformGenerator uniformGenerator;

  @Override
  public void init(Properties p) throws WorkloadException {
    super.init(p);
    String[] preloadedkeysArray = p.getProperty("preloadedkeys").split(",");
    preLoadedKeys = new ArrayList<>(preloadedkeysArray.length);
    System.out.println("Adding pre-loaded keys:");
    for (String k : preloadedkeysArray) {
      preLoadedKeys.add(k);
      System.out.println(k);
    }
    uniformGenerator = new UniformGenerator(preLoadedKeys);
  }

  @Override
  protected String buildKeyName(long keynum) {
    return uniformGenerator.nextValue();
  }
}
