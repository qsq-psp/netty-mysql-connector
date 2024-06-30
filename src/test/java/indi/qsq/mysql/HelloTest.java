package indi.qsq.mysql;

import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created on 2024/6/25.
 */
public class HelloTest {

    @Test
    public void test() throws Exception {
        final NioEventLoopGroup group = new NioEventLoopGroup(1);
        final MysqlConnection connection = MysqlConnection.create(group, Config.cachingSha2User()).sync().get();
        try (ResultSet resultSet = connection.query("select \"hello mysql\";").sync().get()) {
            assertEquals(1, resultSet.header.columnCount());
            assertEquals(1, resultSet.rows.size());
            assertEquals("hello mysql", resultSet.rows.get(0).getString(resultSet.header.getByIndex(0)));
        } finally {
            connection.close().await();
            group.shutdownGracefully().await();
        }
    }
}
