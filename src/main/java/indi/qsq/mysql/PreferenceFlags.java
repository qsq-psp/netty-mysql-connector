package indi.qsq.mysql;

/**
 * Created on 2024/5/28.
 * Client local preference, the server do not know
 */
@SuppressWarnings("PointlessBitwiseExpression")
public interface PreferenceFlags {

    int ALLOW_NAN = 1 << 0;

    int ALLOW_INFINITY = 1 << 1;

    int AUTO_READ = 1 << 16;

    int FAST_READ = 1 << 17;

    int ALLOW_PUBLIC_KEY_RETRIEVAL = 1 << 18;

    int ACCESS_THREAD_CHECK = 1 << 19;

    int LAZY_DECODE_TEXT = 1 << 20;

    int LAZY_DECODE_BINARY = 1 << 21;
}
