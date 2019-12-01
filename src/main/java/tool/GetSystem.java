package tool;

import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

/**
 * @author :jinkai
 * @date :Created in 2019/10/23 17:19
 * @description:
 * @modified By:
 * @version:
 */

public class GetSystem {
    private static final Logger LOG = LoggerFactory.getLogger(GetSystem.class);

    public final static String WIN_OS = "WINDOWS";
    public final static String MAC_OS = "MAC";
    public final static String LINUX_OS = "LINUX";
    public final static String OTHER_OS = "OTHER";

    public static String getOS() {
        if (SystemUtils.IS_OS_WINDOWS){
            return WIN_OS;
        }
        if (SystemUtils.IS_OS_MAC || SystemUtils.IS_OS_MAC_OSX){
            return MAC_OS;
        }
        if (SystemUtils.IS_OS_UNIX){
            return LINUX_OS;
        }
        return OTHER_OS;
    }

    // 获取mac地址
    public static String getMacAddress() {
        try {
            Enumeration<NetworkInterface> allNetInterfaces = NetworkInterface.getNetworkInterfaces();
            byte[] mac = null;
            while (allNetInterfaces.hasMoreElements()) {
                NetworkInterface netInterface = (NetworkInterface) allNetInterfaces.nextElement();
                if (netInterface.isLoopback() || netInterface.isVirtual() || netInterface.isPointToPoint() || !netInterface.isUp()) {
                    continue;
                } else {
                    mac = netInterface.getHardwareAddress();
                    if (mac != null) {
                        StringBuilder sb = new StringBuilder();
                        for (int i = 0; i < mac.length; i++) {
                            sb.append(String.format("%02X%s", mac[i], (i < mac.length - 1) ? "-" : ""));
                        }
                        if (sb.length() > 0) {
                            return sb.toString();
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("MAC地址获取失败", e);
        }
        return "";
    }

    // 获取ip地址
    public static String getIpAddress() {
        try {
            Enumeration<NetworkInterface> allNetInterfaces = NetworkInterface.getNetworkInterfaces();
            InetAddress ip = null;
            while (allNetInterfaces.hasMoreElements()) {
                NetworkInterface netInterface = (NetworkInterface) allNetInterfaces.nextElement();
                if (netInterface.isLoopback() || netInterface.isVirtual() || netInterface.isPointToPoint() || !netInterface.isUp()) {
                    continue;
                } else {
                    Enumeration<InetAddress> addresses = netInterface.getInetAddresses();
                    while (addresses.hasMoreElements()) {
                        ip = addresses.nextElement();
                        if (ip != null && ip instanceof Inet4Address) {
                            return ip.getHostAddress();
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("IP地址获取失败", e);
        }
        return "";
    }
}
