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

suite("test_map") {
    sql "DROP TABLE IF EXISTS `test_map_table`"
    sql """
        create table `test_map_table` (
            `id` int,
            `k1` int,
            `value` map<text, text>
        ) distributed by hash(`k1`) buckets 1 properties("replication_num" = "1");
    """

    sql 'insert into `test_map_table` values (1, 1, {"key1": "value1"});'
    sql 'insert into `test_map_table` values (2, 1, {"key1_1": "value1_1"});'
    sql 'insert into `test_map_table` values (3, 2, {"key2": "value2", "key22": "value22"});'
    sql 'insert into `test_map_table` values (4, 2, {"key2_1": "value2_1", "key22_1": "value22_1"});'
    sql 'insert into `test_map_table` values (5, 2, {"key2_2": "value2_2", "key22_2": "value22_2"});'
    sql 'insert into `test_map_table` values (6, 3, {"key3": "value3", "key33": "value33", "key3333": "value333"});'
    sql 'insert into `test_map_table` values (7, 4, {"key4": "value4", "key44": "value44", "key444": "value444", "key4444": "value4444"});'

    sql "DROP TABLE IF EXISTS `test_map_table_right`"
    sql """
        create table `test_map_table_right` (
            `id` int,
            `value` int
        ) distributed by hash(`id`) buckets 1 properties("replication_num" = "1");
    """

    sql 'insert into `test_map_table_right` values(1, 1);'
    sql 'insert into `test_map_table_right` values(2, 1);'
    sql 'insert into `test_map_table_right` values(3, 2);'
    sql 'insert into `test_map_table_right` values(4, 2);'
    sql 'insert into `test_map_table_right` values(5, 3);'
    sql 'insert into `test_map_table_right` values(6, 3);'

    qt_sql """
        select * from test_map_table left join test_map_table_right on test_map_table.k1 = test_map_table_right.value order by 1,2,4,5;
    """

    sql "DROP TABLE IF EXISTS `task_map_agg_with_bitmap`"
    sql """
        CREATE TABLE `task_map_agg_with_bitmap` (
            `cache_key` varchar(65533) NOT NULL,
            `result_cnt` int NULL COMMENT '人群包人数'
        ) ENGINE = OLAP duplicate KEY(`cache_key`) COMMENT 'OLAP' DISTRIBUTED BY HASH(`cache_key`) BUCKETS 1 PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql 'insert into `task_map_agg_with_bitmap` values ("aa",null);'
    sql 'insert into `task_map_agg_with_bitmap` values ("bb",null);'
    sql 'insert into `task_map_agg_with_bitmap` values ("bb",1);'
    sql 'insert into `task_map_agg_with_bitmap` values ("bb",2);'
    sql 'insert into `task_map_agg_with_bitmap` values ("bb",3);'

    qt_sql2 """
        select bitmap_count(id_map['2024-11-03']) cnt,
            bitmap_contains(id_map['2024-11-03'], 1) c1,
            bitmap_contains(id_map['2024-11-03'], 2) c2,
            bitmap_contains(id_map['2024-11-03'], 3) c3
        from (
            select
                map_agg(tag_logymd, result) id_map
            from
                (
                    select
                        '2024-11-03' tag_logymd,
                        bitmap_agg(result_cnt) result
                    from
                        `task_map_agg_with_bitmap`
                ) t1
        ) t2;
    """
}
