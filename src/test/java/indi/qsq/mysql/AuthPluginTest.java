package indi.qsq.mysql;

import io.netty.channel.ChannelFuture;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.Future;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.*;

/**
 * Created on 2024/6/26.
 */
public class AuthPluginTest {

    static NioEventLoopGroup group;

    @BeforeClass
    public static void openGroup() {
        group = new NioEventLoopGroup(1); // one selector can register many channels, so one is enough
    }

    @AfterClass
    public static void closeGroup() throws Exception {
        group.shutdownGracefully().await();
        group = null;
    }

    public void test(MysqlConnection.BeforeConfig user) throws Exception {
        final MysqlConnection connection = MysqlConnection.create(group, user).sync().get();
        try {
            assertNotNull(connection.ping().sync().get());
        } finally {
            connection.close();
        }
    }

    @Test
    public void testNativeUser() throws Exception {
        test(Config.nativeUser());
    }

    @Test
    public void testSha256User() throws Exception {
        test(Config.sha256User());
    }

    @Test
    public void testCachingSha2User() throws Exception {
        test(Config.cachingSha2User());
    }

    static final int COUNT_PER_USER = 2;

    @Test
    public void testParallel() throws Exception {
        final ArrayList<Future<MysqlConnection>> connectFutures = new ArrayList<>(3 * COUNT_PER_USER);
        for (int i = 0; i < COUNT_PER_USER; i++) {
            connectFutures.add(MysqlConnection.create(group, Config.nativeUser()));
            connectFutures.add(MysqlConnection.create(group, Config.sha256User()));
            connectFutures.add(MysqlConnection.create(group, Config.cachingSha2User()));
        }
        try {
            for (Future<MysqlConnection> future : connectFutures) {
                future.await();
                assertTrue(future.isSuccess());
            }
            ArrayList<Future<ResponsePacket.OK>> pingFutures = new ArrayList<>(3 * COUNT_PER_USER);
            for (Future<MysqlConnection> future : connectFutures) {
                pingFutures.add(future.get().ping());
            }
            for (Future<ResponsePacket.OK> future : pingFutures) {
                future.await();
                assertTrue(future.isSuccess());
            }
        } finally {
            ArrayList<ChannelFuture> closeFutures = new ArrayList<>(3 * COUNT_PER_USER);
            for (Future<MysqlConnection> future : connectFutures) {
                if (future.isSuccess()) {
                    closeFutures.add(future.get().close());
                }
            }
            for (ChannelFuture future : closeFutures) {
                future.await();
                assertTrue(future.isSuccess());
            }
        }
    }

    @Test
    public void testSslNativeUser() throws Exception {
        final MysqlConnection.BeforeConfig user = Config.nativeUser();
        user.enableSSL();
        test(user);
    }

    @Test
    public void testSslSha256User() throws Exception {
        final MysqlConnection.BeforeConfig user = Config.sha256User();
        user.enableSSL();
        test(user);
    }

    @Test
    public void testSslCachingSha2User() throws Exception {
        final MysqlConnection.BeforeConfig user = Config.cachingSha2User();
        user.enableSSL(); // javax.net.ssl.SSLHandshakeException: PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path to requested target
        test(user);
    }
}
