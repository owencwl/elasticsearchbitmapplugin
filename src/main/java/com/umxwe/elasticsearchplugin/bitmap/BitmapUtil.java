package com.umxwe.elasticsearchplugin.bitmap;

import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class BitmapUtil {

    /**
     * 编码函数
     * 将字符串转为long类型，通用计算逻辑：编写可逆Hash函数,且long唯一，能够实现 string <==> long 之间的转换；
     * 此功能目前将车牌号：京d2NN30 按照每个字符的acsii码进行转换，形成long类型
     *
     * @param value
     * @return
     */
    public static String stringToAscii(String value) {
        StringBuffer sbu = new StringBuffer();
        char[] chars = value.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            sbu.append((int) chars[i]);
        }
        return sbu.toString();
    }

    /**
     * 解码函数
     * 将long类型转为string类型;
     * 有待实现具体逻辑
     *
     * @param value
     * @return
     */
    public static String asciiToString(long value) {
        return "";
    }

    /**
     * 反序列化
     *
     * @param arr
     * @return
     */
    public static Roaring64Bitmap deserializeBitmap(byte[] arr) {
        Roaring64Bitmap roaringBitmap = new Roaring64Bitmap();
        try {
            roaringBitmap.deserialize(ByteBuffer.wrap(arr));
        } catch (IOException e) {
            System.out.println(e);
        }
        return roaringBitmap;
    }

    /**
     * 序列化
     *
     * @param roaringBitmap
     * @return
     */
    public static byte[] serializeBitmap(Roaring64Bitmap roaringBitmap) {
        try {
            long sizeInBytesL = roaringBitmap.serializedSizeInBytes();
            if (sizeInBytesL >= Integer.MAX_VALUE) {
                throw new UnsupportedOperationException();
            }
            int sizeInBytesInt = (int) sizeInBytesL;
            ByteBuffer byteBuffer = ByteBuffer.allocate(sizeInBytesInt).order(ByteOrder.LITTLE_ENDIAN);
            roaringBitmap.serialize(byteBuffer);
            return byteBuffer.array();
        } catch (Exception e) {
            System.out.println(e);
        }
        return null;
    }

}
