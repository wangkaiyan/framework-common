/**
 * copyright (c) weibo.com
 * created by weijia at Nov 9, 2013 1:03:18 AM
 */
package com.framework.util;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.RuntimeMXBean;


public class JVMUtil {
    private static final RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
    private static final MemoryMXBean memory = ManagementFactory.getMemoryMXBean();

    public static String getJVMPID() {
        String pid = getJVMName().split("@")[0];
        return pid;
    }

    public static String getJVMName() {
        String name = runtime.getName();
        return name;
    }

    public static String getJVMPort() {
        String port = System.getProperty("console.port");
        String hostname = IPAddrUtil.getHostname();
        if (port == null || port.trim().length() == 0) {
            port = "880";
        }
        return hostname + ":" + port;
    }

//    public static String getJVMMemory() {
//        long usedHeap = memory.getHeapMemoryUsage().getUsed() / Constants.KILO;
//        long maxHeap = memory.getHeapMemoryUsage().getMax() / Constants.MEGA;
//        long usedNonHeap = memory.getNonHeapMemoryUsage().getUsed() / Constants.KILO;
//        long maxNonHeap = memory.getNonHeapMemoryUsage().getMax() / Constants.MEGA;
//        return String.valueOf(
//                usedHeap) + Constants.UNIT_KILO + Constants.SLASH + maxHeap +
//                Constants.UNIT_MEGA + Constants.COLON + usedNonHeap +
//                Constants.UNIT_KILO + Constants.SLASH + maxNonHeap +
//                Constants.UNIT_MEGA;
//    }

}
