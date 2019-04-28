package cn.oge.kdm.rtdb.vzdb.common;

import org.apache.commons.codec.binary.Base64;

public class Base64Util {

    /**
     * 二进制数据编码为BASE64字符串
     *
     * @param bytes 要压缩的byte数组
     * @return String
     */
    public static String encode(final byte[] bytes) {
        return new String(Base64.encodeBase64(bytes));
    }
}
