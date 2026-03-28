package com.decentdb.jdbc;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Minimal {@link DataSource} implementation for frameworks that prefer a configured JDBC source.
 *
 * <p>This does not implement pooling. DecentDB uses a single-writer / many-readers process model,
 * so external pools should normally be configured with a maximum size of {@code 1} for write-heavy use.</p>
 */
public final class DecentDBDataSource implements DataSource {
    private String url;
    private boolean readOnly;
    private String mode = "openOrCreate";
    private int loginTimeout;
    private PrintWriter logWriter;

    public DecentDBDataSource() {
    }

    public DecentDBDataSource(String url) {
        this.url = url;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return openConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        if ((username != null && !username.isBlank()) || (password != null && !password.isBlank())) {
            throw new SQLFeatureNotSupportedException("DecentDB does not support username/password authentication", "0A000");
        }
        return openConnection();
    }

    private Connection openConnection() throws SQLException {
        if (url == null || url.isBlank()) {
            throw new SQLException("DecentDBDataSource.url must be configured", "08001");
        }
        Properties props = new Properties();
        props.setProperty("mode", mode);
        props.setProperty("readOnly", Boolean.toString(readOnly));
        DriverManager.setLoginTimeout(loginTimeout);
        return DriverManager.getConnection(url, props);
    }

    @Override
    public PrintWriter getLogWriter() {
        return logWriter;
    }

    @Override
    public void setLogWriter(PrintWriter out) {
        this.logWriter = out;
    }

    @Override
    public void setLoginTimeout(int seconds) {
        this.loginTimeout = seconds;
    }

    @Override
    public int getLoginTimeout() {
        return loginTimeout;
    }

    @Override
    public Logger getParentLogger() {
        return Logger.getLogger("com.decentdb.jdbc");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isAssignableFrom(getClass());
    }
}
