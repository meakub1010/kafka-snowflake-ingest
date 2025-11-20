package com.bny.dw.kafkasnowflakeingest.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class SnowflakeService {
    private static final Logger log = LoggerFactory.getLogger(SnowflakeService.class);

    private final String url;
    private final Properties connProps;

    public SnowflakeService(
            @Value("${snowflake.url}") String url,
            @Value("${snowflake.user}") String user,
            @Value("${snowflake.password}") String password,
            @Value("${snowflake.warehouse}") String warehouse,
            @Value("${snowflake.database}") String database,
            @Value("${snowflake.schema}") String schema) {

        this.url = url;

        connProps = new Properties();
        connProps.put("user", user);
        connProps.put("password", password);
        connProps.put("warehouse", warehouse);
        connProps.put("database", database);
        connProps.put("schema", schema);

        connProps.put("GCP_IMDS_MODE", "OFF");
        connProps.put("AWS_IMDS_MODE", "OFF");
        connProps.put("AZURE_IMDS_MODE", "OFF");

        connProps.put("CLIENT_DISABLE_HOSTNAME_VERIFICATION", "true");
        connProps.put("ENABLE_OAUTH", "false");
    }

    public void insertTransaction(String tradeId, String trader, Double price, Integer quantity, String fund,
            String security, Timestamp tradeDate) {
        String sql = "INSERT INTO transactions_staging (id, trade_date, trader, fund, security, quantity, price) VALUES (?, ?, ?, ?, ?, ?, ?)";

        try (Connection connection = DriverManager.getConnection(url, connProps);
                PreparedStatement stmt = connection.prepareStatement(sql)) {

            stmt.setString(1, tradeId);
            stmt.setTimestamp(2, tradeDate);
            stmt.setString(3, trader);
            stmt.setString(4, fund);
            stmt.setString(5, security);
            stmt.setInt(6, quantity);
            stmt.setDouble(7, price);

            stmt.executeUpdate();
            log.info("Inserted transaction {}", tradeId);

        } catch (SQLException e) {
            log.error("Failed to insert transaction {}: {}", tradeId, e.getMessage(), e);
        }
    }
}
