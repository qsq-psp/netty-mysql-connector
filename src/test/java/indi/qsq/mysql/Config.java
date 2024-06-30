package indi.qsq.mysql;

import org.jetbrains.annotations.NotNull;

/**
 * Created on 2024/6/17.
 */
public class Config {

    @NotNull
    public static MysqlConnection.BeforeConfig nativeUser() {
        final MysqlConnection.BeforeConfig config = new MysqlConnection.BeforeConfig();
        config.userName = "connector-test-native";
        config.password = "4087d9d58e";
        config.defaultSchema = "connectortest";
        return config;
    }

    @NotNull
    public static MysqlConnection.BeforeConfig sha256User() {
        final MysqlConnection.BeforeConfig config = new MysqlConnection.BeforeConfig();
        config.userName = "connector-test-sha256";
        config.password = "577b202044";
        config.defaultSchema = "connectortest";
        return config;
    }

    @NotNull
    public static MysqlConnection.BeforeConfig cachingSha2User() {
        final MysqlConnection.BeforeConfig config = new MysqlConnection.BeforeConfig();
        config.userName = "connector-test-caching-sha2";
        config.password = "2c535b21ab";
        config.defaultSchema = "connectortest";
        return config;
    }
}
