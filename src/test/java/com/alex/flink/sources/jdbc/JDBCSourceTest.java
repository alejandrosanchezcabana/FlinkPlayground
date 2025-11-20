package com.alex.flink.sources.jdbc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class JDBCSourceTest {

    private static final String URL = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1";
    private static final String USER = "sa";
    private static final String PASSWORD = "";

    @Before
    public void setUp() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
                Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE test_table (id INT, name VARCHAR(255))");
            stmt.execute("INSERT INTO test_table VALUES (1, 'Alice')");
            stmt.execute("INSERT INTO test_table VALUES (2, 'Bob')");
        }
    }

    @After
    public void tearDown() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
                Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE test_table");
        }
    }

    @Test
    public void testJDBCSource() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        JDBCSource source = new JDBCSource(URL, USER, PASSWORD, "SELECT * FROM test_table");

        DataStream<List<Object>> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "JDBC Source");

        List<List<Object>> results = new ArrayList<>();
        try (var iterator = stream.executeAndCollect()) {
            while (iterator.hasNext()) {
                results.add(iterator.next());
            }
        }

        Assert.assertEquals(2, results.size());
        Assert.assertEquals(1, results.get(0).get(0));
        Assert.assertEquals("Alice", results.get(0).get(1));
        Assert.assertEquals(2, results.get(1).get(0));
        Assert.assertEquals("Bob", results.get(1).get(1));
    }
}
