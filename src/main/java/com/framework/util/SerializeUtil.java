/**
 * 
 */
package com.framework.util;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**   
 *    
 * 项目名称：framework-common   
 * 类名称：CodecHandlerUtil   
 * 类描述：各种java对象序列化和反序列化工具
 * 创建人：Administrator   
 * 创建时间：2015年7月16日 下午3:56:15   
 * 修改人：Administrator   
 * 修改时间：2015年7月16日 下午3:56:15   
 * 修改备注：   
 * @version    
 *    
 */
public class SerializeUtil {

	@SuppressWarnings("unchecked")
    public static final <T extends Serializable> T decode(byte[] bytes) {
		if(bytes ==null || bytes.length==0){
			return null;
		}
		
        T t = null;
        Exception thrown = null;
        try {
            ObjectInputStream oin = new ObjectInputStream(new ByteArrayInputStream(bytes));
            t = (T) oin.readObject();
        } catch (IOException e) {
            e.printStackTrace();
            thrown = e;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            thrown = e;
        } catch (ClassCastException e) {
            e.printStackTrace();
            thrown = e;
        } finally {
            if (null != thrown)
                throw new RuntimeException("Error decoding byte[] data to instantiate java object - "
                        + "data at key may not have been of this type or even an object", thrown);
        }
        return t;
    }
    
    public static final <T extends Serializable> byte[] encode(T obj) {
        byte[] bytes = null;
    	if(obj==null){
    		return bytes;
    	}

        try {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(bout);
            out.writeObject(obj);
            bytes = bout.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error serializing object" + obj + " => " + e);
        }
        return bytes;
    }

    public static byte[] encode(String str) {
        return str.getBytes();
    }
    
    public static byte[] encode(Number value) {
        return encode(String.valueOf(value));
    }

    public static int toInt(byte[] data) {
        return Integer.parseInt(toStr(data));
    }
    
    public static long toLong(byte[] data) {
        return Long.parseLong(toStr(data));
    }

    public static String toStr(byte[] data) {
        return new String(data);
    }
    
    public static final List<Long> toLong(List<byte[]> bytearray){
        List<Long> list = new ArrayList<Long>(bytearray.size());
        for(byte[] b : bytearray) {
            if (b == null) {
                list.add(null);
            } else {
                list.add(toLong(b));
            }
        }
        return list;
    }
   public static final List<String> toString(List<byte[]> bytearray){
        List<String> list = new ArrayList<String>(bytearray.size());
        for(byte[] b : bytearray) {
            if (b == null) {
                list.add(null);
            } else {
                list.add(toStr(b));
            }
        }
        return list;
    }
   public static final List<String> toString(byte[]... byteslist){
       List<String> list = new ArrayList<String>(byteslist.length);
       for(byte[] b : byteslist) {
           if (b == null) {
               list.add(null);
           } else {
               list.add(toStr(b));
           }
       }
       return list;
   }
    
    public static final byte[][] toBytes(List<String> strList){
        byte[][] bytesArray = new byte[strList.size()][];
        for (int i = 0; i < bytesArray.length; i++) {
        	bytesArray[i] = encode(strList.get(i));
        }
        return bytesArray;
    }
    
    public static void main(String[] args){
    	String name="test";
    	byte[] bytes = name.getBytes();
    	byte[] bytess = SerializeUtil.encode(name);
    }
}
