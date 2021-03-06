package com.umxwe.elasticsearchplugin.bitmap;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class BitmapUtil {


    public static void main(String[] args) throws InterruptedException {
        /**
         * 问题：
         * 此代码在bitmap0.9.0和jdk1.8._251版本中，存在bug（数据越界异常）: Exception in thread "main" java.lang.ArrayIndexOutOfBoundsException: 74
         *
         * 解决方案：
         * 在jdk1.8下，bitmap0.9.3以上版本已经修复了此bug。
         */
        Random random = new Random();
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        for (long i = 0; i < 50000; i++) {
//            System.out.println(random.nextLong());
            bitmap.add(random.nextInt());
        }
        Random random2= new Random();
        Roaring64Bitmap bitmap2 = new Roaring64Bitmap();
        for (long i = 1; i < 100000; i++) {
            bitmap2.add( random2.nextInt());
        }
        //bit and
//        bitmap.and(bitmap2);
//
//        System.out.println(bitmap);
//        System.out.println(bitmap.getLongCardinality());

    }
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
