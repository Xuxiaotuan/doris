// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.trees.expressions.literal;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StringLikeLiteralTest {
    @Test
    public void testStrToDouble() {
        // fix bug: maxStr.length = 4, maxStr.getBytes().length=12
        // when converting str to double, bytes length is used instead of string length
        String minStr = "商家+店长+场地+设备类型维度";
        String maxStr = "商家维度";
        double d1 = StringLikeLiteral.getDouble(minStr);
        double d2 = StringLikeLiteral.getDouble(maxStr);
        Assertions.assertTrue(d1 < d2);
    }

    @Test
    public void testUtf8() {
        System.setProperty("file.encoding", "ANSI_X3.4-1968");
        double d1 = StringLikeLiteral.getDouble("一般风险准备");
        Assertions.assertEquals(d1, 6.4379158486625512E16);
    }
}
