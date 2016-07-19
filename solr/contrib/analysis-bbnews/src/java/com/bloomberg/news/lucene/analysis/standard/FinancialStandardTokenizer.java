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

package com.bloomberg.news.lucene.analysis.standard;

import java.io.Reader;

import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.util.Version;

public final class FinancialStandardTokenizer extends StandardTokenizer {

  /**
   * Creates a new instance of the {@link com.bloomberg.news.lucene.analysis.standard.FinancialStandardTokenizer}.  Attaches
   * the <code>input</code> to the newly created JFlex scanner.
   *
   * @param input The input reader
   *
   * See http://issues.apache.org/jira/browse/LUCENE-1068
   */
  public FinancialStandardTokenizer(Version matchVersion, Reader input) {
    super(matchVersion, input);
  }

  /**
   * Creates a new FinancialStandardTokenizer with a given {@link org.apache.lucene.util.AttributeSource.AttributeFactory} 
   */
  public FinancialStandardTokenizer(Version matchVersion, AttributeFactory factory, Reader input) {
    super(matchVersion, factory, input);
  }

  @Override
  protected void init(Version matchVersion) {
    this.scanner = new FinancialStandardTokenizerImpl(input);
  }

}
