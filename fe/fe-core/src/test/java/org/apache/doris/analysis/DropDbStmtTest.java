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

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.MockedAuth;
import org.apache.doris.qe.ConnectContext;

import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DropDbStmtTest {
    Analyzer analyzer;

    @Mocked
    private AccessControllerManager accessManager;
    @Mocked
    private ConnectContext ctx;

    @Before
    public void setUp() {
        MockedAuth.mockedAccess(accessManager);
        MockedAuth.mockedConnectContext(ctx, "root", "192.168.1.1");
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
    }

    @Test
    public void testNormal() throws UserException, AnalysisException {
        DropDbStmt stmt = new DropDbStmt(false, new DbName("test", "test"), false);

        stmt.analyze(analyzer);
        Assert.assertEquals("test", stmt.getCtlName());
        Assert.assertEquals("test", stmt.getDbName());
        Assert.assertEquals("DROP DATABASE `test`", stmt.toString());
    }

    @Test
    public void testForce() throws UserException, AnalysisException {
        DropDbStmt stmt = new DropDbStmt(false, new DbName("test", "test"), true);

        stmt.analyze(analyzer);
        Assert.assertEquals("test", stmt.getCtlName());
        Assert.assertEquals("test", stmt.getDbName());
        Assert.assertEquals("DROP DATABASE `test` FORCE", stmt.toString());
    }

    @Test(expected = AnalysisException.class)
    public void testFailed() throws UserException, AnalysisException {
        DropDbStmt stmt = new DropDbStmt(false, new DbName("", ""), true);

        stmt.analyze(analyzer);
        Assert.fail("no exception");
    }

    @Test
    public void testNoPriv() {
        DropDbStmt stmt = new DropDbStmt(false, new DbName("", ""), true);
        try {
            stmt.analyze(AccessTestUtil.fetchBlockAnalyzer());
        } catch (AnalysisException e) {
            return;
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
        Assert.fail("no exception");
    }
}
