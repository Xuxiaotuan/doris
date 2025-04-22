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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.StorageVault;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.rpc.RpcException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Represents the command for SHOW CREATE VIEW.
 */
public class ShowCreateStorageVaultCommand extends ShowCommand {

    private final String name;

    public ShowCreateStorageVaultCommand(String name) {
        super(PlanType.SHOW_CREATE_STORAGE_VAULT_COMMAND);
        this.name = name;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowCreateStorageVaultCommand(this, context);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return StorageVault.CREATE_STORAGE_VAULT_META_DATA;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        List<List<String>> rows = new ArrayList<>();
        try {
            Cloud.GetObjStoreInfoResponse resp = MetaServiceProxy.getInstance()
                    .getObjStoreInfo(Cloud.GetObjStoreInfoRequest.newBuilder().build());
            List<Cloud.StorageVaultPB> storageVaults = resp.getStorageVaultList();
            for (Cloud.StorageVaultPB vault : storageVaults) {
                if (vault.getName().equals(name)) {
                    String createStmt = StorageVault.generateCreateStorageVaultStmt(vault);
                    rows.add(Arrays.asList(vault.getName(), createStmt));
                }
            }
        } catch (RpcException e) {
            throw new AnalysisException(e.getMessage());
        }

        return new ShowResultSet(getMetaData(), rows);
    }

}
