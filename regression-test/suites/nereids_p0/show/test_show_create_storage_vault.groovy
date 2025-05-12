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


suite("test_show_create_storage_vault", "query,arrow_flight_sql") {
    String vault_name = "test_show_create_vault_1"
    try {
        // Create storage vault with basic configuration
        sql """
            CREATE STORAGE VAULT IF NOT EXISTS ${vault_name}
            PROPERTIES (
                "type" = "s3",
                "AWS_ENDPOINT" = "s3.example.com",
                "AWS_ACCESS_KEY" = "access_key",
                "AWS_SECRET_KEY" = "secret_key",
                "AWS_REGION" = "us-east-1"
            )
        """
        
        // Verify show create storage vault command
        checkNereidsExecute("""SHOW CREATE STORAGE VAULT `${vault_name}`;""")
        qt_cmd("""SHOW CREATE STORAGE VAULT `${vault_name}`;""")
    } finally {
        // Cleanup storage vault
        try_sql("DROP STORAGE VAULT IF EXISTS `${vault_name}`")
    }
    // Case with HDFS configuration
    String vault_name_2 = "test_show_create_vault_2"
    try {
        // Create storage vault with HDFS configuration
        sql """
            CREATE STORAGE VAULT IF NOT EXISTS ${vault_name_2}
            PROPERTIES (
                "type" = "hdfs",
                "hdfs.nameservices" = "mycluster",
                "hdfs.uri" = "hdfs://mycluster:8020",
                "hdfs.username" = "doris"
            )
        """
        
        // Verify show create command
        checkNereidsExecute("""SHOW CREATE STORAGE VAULT `${vault_name_2}`;""")
        qt_cmd("""SHOW CREATE STORAGE VAULT `${vault_name_2}`;""")
    } finally {
        // Cleanup storage vault
        try_sql("DROP STORAGE VAULT IF EXISTS `${vault_name_2}`")
    }
}