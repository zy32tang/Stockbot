package com.stockbot.jp.db;

import org.apache.logging.log4j.Logger;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Locale;

final class SqlLogProxy {
    private SqlLogProxy() {
    }

    static Connection wrapConnection(Connection delegate, Logger logger) {
        InvocationHandler handler = new ConnectionHandler(delegate, logger);
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class[]{Connection.class},
                handler
        );
    }

    private static final class ConnectionHandler implements InvocationHandler {
        private final Connection delegate;
        private final Logger logger;

        private ConnectionHandler(Connection delegate, Logger logger) {
            this.delegate = delegate;
            this.logger = logger;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String name = method.getName();
            try {
                Object out = method.invoke(delegate, args);
                if ("prepareStatement".equals(name)
                        && args != null
                        && args.length > 0
                        && args[0] instanceof String
                        && out instanceof PreparedStatement) {
                    return wrapPreparedStatement((PreparedStatement) out, (String) args[0], logger);
                }
                if ("createStatement".equals(name) && out instanceof Statement) {
                    return wrapStatement((Statement) out, logger);
                }
                return out;
            } catch (InvocationTargetException e) {
                throw e.getTargetException();
            }
        }
    }

    private static PreparedStatement wrapPreparedStatement(PreparedStatement delegate, String sql, Logger logger) {
        InvocationHandler handler = new PreparedStatementHandler(delegate, sql, logger);
        return (PreparedStatement) Proxy.newProxyInstance(
                PreparedStatement.class.getClassLoader(),
                new Class[]{PreparedStatement.class},
                handler
        );
    }

    private static Statement wrapStatement(Statement delegate, Logger logger) {
        InvocationHandler handler = new StatementHandler(delegate, logger);
        return (Statement) Proxy.newProxyInstance(
                Statement.class.getClassLoader(),
                new Class[]{Statement.class},
                handler
        );
    }

    private abstract static class AbstractSqlHandler implements InvocationHandler {
        final Logger logger;

        AbstractSqlHandler(Logger logger) {
            this.logger = logger;
        }

        boolean isExecuteMethod(String method) {
            return "execute".equals(method)
                    || "executeQuery".equals(method)
                    || "executeUpdate".equals(method)
                    || "executeLargeUpdate".equals(method)
                    || "executeBatch".equals(method)
                    || "executeLargeBatch".equals(method);
        }

        String normalizeSql(String sql) {
            if (sql == null) {
                return "";
            }
            String normalized = sql.replaceAll("\\s+", " ").trim();
            if (normalized.length() <= 800) {
                return normalized;
            }
            return normalized.substring(0, 800) + "...";
        }

        String resultSummary(Object result) {
            if (result == null) {
                return "";
            }
            if (result instanceof Integer) {
                return " rows=" + result;
            }
            if (result instanceof Long) {
                return " rows=" + result;
            }
            if (result instanceof int[]) {
                return " batch_size=" + ((int[]) result).length;
            }
            if (result instanceof long[]) {
                return " batch_size=" + ((long[]) result).length;
            }
            if (result instanceof Boolean) {
                return " has_result_set=" + result;
            }
            return "";
        }

        void logSuccess(String method, String sql, long elapsedNanos, Object result) {
            if (!logger.isInfoEnabled()) {
                return;
            }
            logger.info(
                    "SQL ok method={} elapsed_ms={}{} sql={}",
                    method,
                    String.format(Locale.US, "%.3f", elapsedNanos / 1_000_000.0),
                    resultSummary(result),
                    normalizeSql(sql)
            );
        }

        void logFailure(String method, String sql, long elapsedNanos, Throwable error) {
            logger.warn(
                    "SQL fail method={} elapsed_ms={} err={} sql={}",
                    method,
                    String.format(Locale.US, "%.3f", elapsedNanos / 1_000_000.0),
                    error == null ? "" : error.getMessage(),
                    normalizeSql(sql)
            );
        }
    }

    private static final class PreparedStatementHandler extends AbstractSqlHandler {
        private final PreparedStatement delegate;
        private final String sql;
        private int pendingBatchCount;

        private PreparedStatementHandler(PreparedStatement delegate, String sql, Logger logger) {
            super(logger);
            this.delegate = delegate;
            this.sql = sql == null ? "" : sql;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String name = method.getName();
            if ("addBatch".equals(name) && (args == null || args.length == 0)) {
                pendingBatchCount++;
            }
            if (!isExecuteMethod(name)) {
                try {
                    return method.invoke(delegate, args);
                } catch (InvocationTargetException e) {
                    throw e.getTargetException();
                }
            }

            long started = System.nanoTime();
            try {
                Object out = method.invoke(delegate, args);
                long elapsed = System.nanoTime() - started;
                String resolvedSql = sql;
                if ("executeBatch".equals(name) || "executeLargeBatch".equals(name)) {
                    resolvedSql = sql + " [batched_statements=" + pendingBatchCount + "]";
                    pendingBatchCount = 0;
                }
                logSuccess(name, resolvedSql, elapsed, out);
                return out;
            } catch (InvocationTargetException e) {
                long elapsed = System.nanoTime() - started;
                Throwable target = e.getTargetException();
                logFailure(name, sql, elapsed, target);
                throw target;
            }
        }
    }

    private static final class StatementHandler extends AbstractSqlHandler {
        private final Statement delegate;

        private StatementHandler(Statement delegate, Logger logger) {
            super(logger);
            this.delegate = delegate;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String name = method.getName();
            if (!isExecuteMethod(name)) {
                try {
                    return method.invoke(delegate, args);
                } catch (InvocationTargetException e) {
                    throw e.getTargetException();
                }
            }

            String sql = "";
            if (args != null && args.length > 0 && args[0] instanceof String) {
                sql = (String) args[0];
            }

            long started = System.nanoTime();
            try {
                Object out = method.invoke(delegate, args);
                long elapsed = System.nanoTime() - started;
                logSuccess(name, sql, elapsed, out);
                return out;
            } catch (InvocationTargetException e) {
                long elapsed = System.nanoTime() - started;
                Throwable target = e.getTargetException();
                logFailure(name, sql, elapsed, target);
                throw target;
            }
        }
    }
}
