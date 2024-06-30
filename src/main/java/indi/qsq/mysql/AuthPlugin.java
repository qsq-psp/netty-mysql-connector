package indi.qsq.mysql;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.Base64;

/**
 * Created on 2024/5/30.
 * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_authentication_methods.html
 */
public abstract class AuthPlugin extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuthPlugin.class);

    static final byte[] ZERO_BYTE = new byte[0];

    static final byte[] ONE_BYTE = new byte[1];

    /**
     * Created on 2024/5/31.
     */
    @FunctionalInterface
    public interface NameHolder {

        String getAuthPluginName();
    }

    /**
     * Created on 2024/5/31.
     */
    @FunctionalInterface
    public interface DataHolder {

        byte[] getAuthData();
    }

    public static AuthPlugin forName(@NotNull MysqlConnection connection, String pluginName) throws MysqlException {
        if (pluginName == null) {
            throw new MysqlException();
        }
        switch (pluginName) {
            case ClearPassword.NAME:
                return new ClearPassword(connection);
            case "mysql_old_password":
                return new OldPassword(connection);
            case "mysql_native_password":
                return new NativePassword(connection);
            case "sha256_password":
                return new SHA256Password(connection);
            case "caching_sha2_password":
                return new CachingSHA2Password(connection);
            default:
                throw new MysqlException();
        }
    }

    @NotNull
    public static byte[] concatBytes(@NotNull byte[] a, @NotNull byte[] b) {
        final int aLength = a.length;
        final int bLength = b.length;
        final byte[] c = new byte[aLength + bLength];
        System.arraycopy(a, 0, c, 0, aLength);
        System.arraycopy(b, 0, c, aLength, bLength);
        return c;
    }

    @NotNull
    @SuppressWarnings("SameParameterValue")
    public static byte[] sliceBytes(@NotNull byte[] src, int offset, int minLength) {
        final int srcLength = src.length;
        final int dstLength = Math.max(minLength, srcLength - offset);
        final byte[] dst = new byte[dstLength];
        if (offset < srcLength) {
            System.arraycopy(src, offset, dst, 0, srcLength - offset);
        }
        return dst;
    }

    @NotNull
    static byte[] xor(@NotNull byte[] a, @NotNull byte[] b) {
        final int length = Math.min(a.length, b.length);
        final byte[] c = new byte[length];
        for (int i = 0; i < length; i++) {
            c[i] = (byte) (a[i] ^ b[i]);
        }
        return c;
    }

    @NotNull
    static byte[] repeat(@NotNull byte[] src, int len) {
        final int mod = src.length;
        if (mod == len) {
            return src;
        }
        final byte[] dst = new byte[len];
        for (int i = 0; i < len; i++) {
            dst[i] = src[i % mod];
        }
        return dst;
    }

    void clear(@NotNull RequestPacket.AuthNextFactor packet) {
        packet.authData = concatBytes(
                connection.config.password.getBytes(StandardCharsets.UTF_8),
                ONE_BYTE
        );
    }

    /**
     * @return true if uses empty password fast path
     */
    boolean scramble(@NotNull RequestPacket.AuthNextFactor packet, @NotNull String messageDigestAlgorithm) throws NoSuchAlgorithmException {
        final String password = connection.config.password;
        if (password.isEmpty()) { // empty password fast path
            packet.authData = ZERO_BYTE;
            return true;
        }
        final MessageDigest messageDigest = MessageDigest.getInstance(messageDigestAlgorithm);
        final byte[] passwordHash1 = messageDigest.digest(password.getBytes(StandardCharsets.UTF_8));
        messageDigest.reset();
        final byte[] passwordHash2 = messageDigest.digest(passwordHash1);
        messageDigest.reset();
        messageDigest.update(packet.authData);
        messageDigest.update(passwordHash2);
        packet.authData = xor(passwordHash1, messageDigest.digest());
        return false;
    }

    @NotNull
    static RSAPublicKey decodePublicKey(@NotNull String publicKeyString) throws Exception {
        final int fromIndex = publicKeyString.indexOf('\n') + 1;
        final int toIndex = publicKeyString.indexOf("-----END PUBLIC KEY-----") - 1;
        publicKeyString = publicKeyString.substring(fromIndex, toIndex);
        final X509EncodedKeySpec keySpec = new X509EncodedKeySpec(Base64.getMimeDecoder().decode(publicKeyString));
        final KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        return (RSAPublicKey) keyFactory.generatePublic(keySpec);
    }

    @NotNull
    byte[] encryptWithRSAPublicKey(@NotNull byte[] seed, @NotNull RSAPublicKey publicKey, @NotNull String transformation) throws Exception {
        final byte[] passwordBytes = concatBytes(
                connection.config.password.getBytes(StandardCharsets.UTF_8),
                ONE_BYTE
        );
        final Cipher cipher = Cipher.getInstance(transformation);
        cipher.init(Cipher.ENCRYPT_MODE, publicKey);
        return cipher.doFinal(xor(passwordBytes, repeat(seed, passwordBytes.length)));
    }

    @NotNull
    final MysqlConnection connection;

    public AuthPlugin(@NotNull MysqlConnection connection) {
        super();
        this.connection = connection;
    }

    @Override
    public void channelRead(@NotNull ChannelHandlerContext context, Object message) throws Exception {
        if (message instanceof DataHolder) {
            LOGGER.info("channelRead DataHolder {}", message);
            RequestPacket.AuthNextFactor packet = new RequestPacket.AuthNextFactor();
            packet.authData = ((DataHolder) message).getAuthData();
            boolean finished = handle(context, packet);
            LOGGER.info("channelRead packet {} {}", packet, finished);
            if (packet.pluginName != null && packet.authData != null) {
                context.writeAndFlush(packet);
            }
            if (finished) {
                context.pipeline().remove(this);
            }
        }
        context.fireChannelRead(message);
    }

    /**
     * @param packet both input and output data
     * @return true if is last sent packet, auth success if server then send OK back
     */
    protected abstract boolean handle(@NotNull ChannelHandlerContext context, @NotNull RequestPacket.AuthNextFactor packet) throws Exception;

    /**
     * Created on 2024/5/30.
     */
    public static class ClearPassword extends AuthPlugin {

        static final String NAME = "mysql_clear_password";

        public ClearPassword(@NotNull MysqlConnection connection) {
            super(connection);
        }

        @Override
        protected boolean handle(@NotNull ChannelHandlerContext context, @NotNull RequestPacket.AuthNextFactor packet) {
            LOGGER.info("ClearPassword.handle");
            packet.pluginName = NAME;
            clear(packet);
            return true;
        }
    }

    /**
     * Created on 2024/5/30.
     */
    public static class OldPassword extends AuthPlugin {

        public OldPassword(@NotNull MysqlConnection connection) {
            super(connection);
        }

        @Override
        protected boolean handle(@NotNull ChannelHandlerContext context, @NotNull RequestPacket.AuthNextFactor packet) {
            LOGGER.info("OldPassword.handle");
            packet.pluginName = "mysql_old_password";
            final String password = connection.config.password;
            if (password.isEmpty()) { // empty password fast path
                packet.authData = ZERO_BYTE;
                return true;
            }
            return true;
        }
    }

    /**
     * Created on 2024/5/30.
     */
    public static class NativePassword extends AuthPlugin {

        public NativePassword(@NotNull MysqlConnection connection) {
            super(connection);
        }

        /**
         * SHA1( password ) XOR SHA1( "20-bytes random data from server" CONCAT SHA1( SHA1( password ) ) )
         */
        @Override
        protected boolean handle(@NotNull ChannelHandlerContext context, @NotNull RequestPacket.AuthNextFactor packet) throws Exception {
            LOGGER.info("NativePassword.handle");
            packet.pluginName = "mysql_native_password";
            scramble(packet, "SHA-1");
            return true;
        }
    }

    /**
     * Created on 2024/5/30.
     * No documentation available
     */
    public static class SHA256Password extends AuthPlugin {

        static final byte PUBLIC_KEY_RETRIEVAL_CODE = 1;

        byte[] seed; // public key retrieval state represented in its nullish

        public SHA256Password(@NotNull MysqlConnection connection) {
            super(connection);
        }

        @Override
        protected boolean handle(@NotNull ChannelHandlerContext context, @NotNull RequestPacket.AuthNextFactor packet) throws Exception {
            LOGGER.info("SHA256Password.handle");
            packet.pluginName = "sha256_password";
            final String password = connection.config.password;
            if (password.isEmpty()) { // empty password fast path
                packet.authData = ZERO_BYTE;
                return true;
            }
            if (connection.hasCapability(CapabilitiesFlags.SSL)) {
                clear(packet);
                return true;
            }
            if (!connection.hasPreference(PreferenceFlags.ALLOW_PUBLIC_KEY_RETRIEVAL)) {
                throw new MysqlException();
            }
            if (seed == null) {
                seed = packet.authData;
                packet.authData = new byte[] {PUBLIC_KEY_RETRIEVAL_CODE};
                return false;
            } else {
                packet.authData = encryptWithRSAPublicKey(
                        decodePublicKey(new String(packet.authData, StandardCharsets.UTF_8))
                );
                return true;
            }
        }

        @NotNull
        byte[] encryptWithRSAPublicKey(@NotNull RSAPublicKey publicKey) throws Exception {
            return encryptWithRSAPublicKey(seed, publicKey, "RSA/ECB/OAEPWithSHA-1AndMGF1Padding");
        }
    }

    /**
     * Created on 2024/6/1.
     * https://dev.mysql.com/doc/dev/mysql-server/latest/page_caching_sha2_authentication_exchanges.html#sect_caching_sha2_info
     * Cached version of {@link SHA256Password}, has states, and public key retrieval code is different
     */
    public static class CachingSHA2Password extends AuthPlugin {

        static final byte PUBLIC_KEY_RETRIEVAL_CODE = 2;

        static final int[] MINIMUM_VERSION = new int[] {8, 0, 5}; // 8.0.5

        /**
         * Created on 2024/6/2.
         */
        enum Stage {

            SEND_SCRAMBLE,
            SELECT_PATH,
            REQUEST_PUBLIC_KEY,
            COMPLETE;

            private static final long serialVersionUID = 0x689DAEDF605DE822L;
        }

        Stage stage = Stage.SEND_SCRAMBLE;

        byte[] seed;

        public CachingSHA2Password(@NotNull MysqlConnection connection) {
            super(connection);
        }

        @Override
        protected boolean handle(@NotNull ChannelHandlerContext context, @NotNull RequestPacket.AuthNextFactor packet) throws Exception {
            LOGGER.info("CachingSHA2Password.handle {}", stage);
            packet.pluginName = "caching_sha2_password";
            switch (stage) {
                case SEND_SCRAMBLE:
                    seed = packet.authData;
                    if (scramble(packet, "SHA-256")) {
                        stage = Stage.COMPLETE;
                    } else {
                        stage = Stage.SELECT_PATH;
                    }
                    break;
                case SELECT_PATH:
                    selectPath(context, packet);
                    break;
                case REQUEST_PUBLIC_KEY:
                    packet.authData = encryptWithRSAPublicKey(
                            decodePublicKey(new String(packet.authData, StandardCharsets.UTF_8))
                    );
                    stage = Stage.COMPLETE;
                    break;
                default:
                    throw new MysqlException();
            }
            return stage == Stage.COMPLETE;
        }

        protected void selectPath(@NotNull ChannelHandlerContext context, @NotNull RequestPacket.AuthNextFactor packet) throws Exception {
            final int fastAuthResult = packet.authData[0];
            if (fastAuthResult == 3) {
                stage = Stage.COMPLETE;
            } else if (fastAuthResult == 4) {
                if (connection.hasCapability(CapabilitiesFlags.SSL)) {
                    clear(packet);
                    stage = Stage.COMPLETE;
                } else {
                    RSAPublicKey publicKey = connection.config.publicKey;
                    if (publicKey != null) {
                        packet.authData = encryptWithRSAPublicKey(publicKey);
                        stage = Stage.COMPLETE;
                    }
                    if (connection.hasPreference(PreferenceFlags.ALLOW_PUBLIC_KEY_RETRIEVAL)) {
                        packet.authData = new byte[] {PUBLIC_KEY_RETRIEVAL_CODE};
                        stage = Stage.REQUEST_PUBLIC_KEY;
                    } else {
                        throw new MysqlException();
                    }
                }
            } else {
                throw new MysqlException();
            }
        }

        @NotNull
        byte[] encryptWithRSAPublicKey(@NotNull RSAPublicKey publicKey) throws Exception {
            if (Arrays.compare(connection.serverVersion, MINIMUM_VERSION) >= 0) {
                LOGGER.info("encryptWithRSAPublicKey newer");
                return encryptWithRSAPublicKey(seed, publicKey, "RSA/ECB/OAEPWithSHA-1AndMGF1Padding"); // newer version, same as SHA256Password
            } else {
                LOGGER.info("encryptWithRSAPublicKey older");
                return encryptWithRSAPublicKey(seed, publicKey, "RSA/ECB/PKCS1Padding"); // older version
            }
        }
    }
}
