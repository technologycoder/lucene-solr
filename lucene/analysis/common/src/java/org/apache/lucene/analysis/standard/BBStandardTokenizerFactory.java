package org.apache.lucene.analysis.standard;

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

import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.util.AttributeSource.AttributeFactory;

import java.io.Reader;
import java.util.Map;

/**
 * Custom factory for {@link BBStandardTokenizer}. 
 * Copied and modified from StandardTokenizerFactory
 */
public class BBStandardTokenizerFactory extends TokenizerFactory {
  
  private final int maxTokenLength;
  
  /** Creates a new StandardTokenizerFactory */
  public BBStandardTokenizerFactory(Map<String,String> args) {
    super(args);
    assureMatchVersion();
    maxTokenLength = getInt(args, "maxTokenLength", StandardAnalyzer.DEFAULT_MAX_TOKEN_LENGTH);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public BBStandardTokenizer create(AttributeFactory factory, Reader input) {
    BBStandardTokenizer tokenizer = new BBStandardTokenizer(luceneMatchVersion, factory, input); 
    tokenizer.setMaxTokenLength(maxTokenLength);
    return tokenizer;
  }
}
