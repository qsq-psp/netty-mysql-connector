package indi.qsq.mysql;

/**
 * Created on 2024/5/22.
 * Close immediately, non-blocking, you can call it in event loop.
 * No future is returned.
 */
public interface MysqlImmediateCloseable extends AutoCloseable {

    @Override
    void close() throws MysqlException;
}
