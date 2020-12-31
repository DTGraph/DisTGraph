package chaos;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * o 给视频加水印
 *
 * @author admin
 *
 */
public class FFMPEG {
    public static String dealString(String str) {
        Matcher m = java.util.regex.Pattern.compile("^frame=.*").matcher(str);
        String msg = "";
        while (m.find()) {
            msg = m.group();
        }
        return msg;
    }

    /**
     * 如果是数字就是成功的时间(秒数)
     *
     * @param str
     * @return
     */
    public static boolean isNumeric(String str) {
        Pattern pattern = Pattern.compile("[0-9]*");
        Matcher isNum = pattern.matcher(str);
        if (!isNum.matches()) {
            return false;
        }
        return true;
    }

    /**
     * 如果返回不是null的值就是成功(值为转换用时单位:秒)
     *
     * @param instr
     * @return
     */
    public static String returnSecond(String instr) {
        String returnValue = null;
        if (null != instr) {
            String[] a = instr.split("\\.");
            String[] b = a[0].split(":");
            int returnNumber = 0;
            if (null != instr && b[0].length() != 0) {
                returnNumber = Integer.valueOf(b[0]) * 60 * 60 + Integer.valueOf(b[1]) * 60 + Integer.valueOf(b[2]);
                returnValue = String.valueOf(returnNumber);
            } else {
                returnValue = null;
            }
        }
        return returnValue;
    }

    /**
     * 获取视频格式(转码前的格式和转码后的格式都可以调用)
     *
     * @param outputPath
     * @return
     */
    public static String returnVideoFormat(String outputPath) {
        return outputPath.substring(outputPath.lastIndexOf(".") + 1);
    }

    /**
     * @ HashMap<String,String> dto 参数传递对象<br>
     * dto中包含的参数<br>
     * (必填)1.ffmpeg_path:ffmpeg执行文件地址,如 d:\\ffmpeg\\ffmpeg.exe
     * Linux下直接调用ffmpeg命令(当然你事先已经有这个程序了)<br>
     * (必填)2.input_path:原视频路径<br>
     * (必填)3.video_converted_path:转换后视频输出路径<br>
     * (可选)4.screen_size:视频尺寸 长度乘宽度 乘号用英文小写"x"如 512x480<br>
     * (可选)5.logo:水印地址(其实在ffmpeg中有一个专门的watermark参数,logo跟它有何不同,我还没看,不过对我来说效果一样
     * 貌似需要png图片才行)<br>
     * (可选,如果填写必须有logo才行,默认为0)6.xaxis:水印logo的横坐标(只有logo参数为一个正确路径才行) 比如0<br>
     * (可选,如果填写必须有logo才行,默认为0)6.yaxis:水印logo的纵坐标(只有logo参数为一个正确路径才行) 比如0<br>
     * (可选)vb:视频比特率,传入一个数值,单位在程序里面拼接了k (可选)ab:音频比特率,传入一个数值,单位在程序里面拼接了k
     */
    public String videoTransfer(HashMap<String, String> dto) {
        // String ffmpeg_path,String input_path,String video_converted_path,String
        // logo,String screen_size,String xaxis,String yaxis,String vb,String ab
        List<String> cmd = new ArrayList<String>();
        cmd.add(dto.get("ffmpeg_path"));
        cmd.add("-y");
        cmd.add("-i");
        cmd.add(dto.get("input_path"));

        if (null != dto.get("screen_size")) {
            cmd.add("-s");
            cmd.add(dto.get("screen_size"));
        }
        if (null != dto.get("logo")) {
            String logo = dto.get("logo");
            cmd.add("-vf");
            String xaxis = dto.get("xaxis");
            String yaxis = dto.get("yaxis");
            xaxis = xaxis != null && !xaxis.equals("") ? xaxis : "0";
            yaxis = yaxis != null && !yaxis.equals("") ? yaxis : "0";

            String logoString = "movie=" + logo + "[logo],[in][logo]overlay=x=" + xaxis + ":y=" + yaxis + "[out]";
            cmd.add(logoString);
        }
        cmd.add("-strict");
        cmd.add("-2");
        if (null != dto.get("vb") && !dto.get("vb").equals("")) {
            cmd.add("-vb");
            cmd.add(dto.get("vb") + "k");
        }
        if (null != dto.get("ab") && !dto.get("ab").equals("")) {
            cmd.add("-ab");
            cmd.add(dto.get("ab") + "k");
        }
        cmd.add("-q:v");
        cmd.add("4");
        cmd.add(dto.get("video_converted_path"));
        String converted_time = CmdExecuter.exec(cmd);
        return returnSecond(converted_time);// 获取转换时间
    }

    public String videoCut(HashMap<String, String> dto) {
        // String ffmpeg_path,String input_path,String video_converted_path,String
        // logo,String screen_size,String xaxis,String yaxis,String vb,String ab
        List<String> cmd = new ArrayList<String>();
        cmd.add(dto.get("ffmpeg_path"));
        cmd.add("-i");
        cmd.add(dto.get("input_path"));
        cmd.add("-vcodec");
        cmd.add("copy");
        cmd.add("-acodec");
        cmd.add("copy");
        cmd.add("-ss");
        cmd.add(dto.get("start-time"));
        cmd.add("-to");
        cmd.add(dto.get("end-time"));
        cmd.add(dto.get("video_converted_path"));
        cmd.add("-y");
        String converted_time = CmdExecuter.exec(cmd);
        return returnSecond(converted_time);// 获取转换时间
    }

    public static void main(String[] arg) {
        for(int i = 2; i <= 42; i++){
            HashMap<String, String> dto = new HashMap<String, String>();
            String firstpath = "D:\\self\\test\\copy5\\" + i + "temp.mp4";
            String secpath = "D:\\self\\test\\copy5\\" + i + ".mp4";
            dto.put("ffmpeg_path",
                    "D:\\software\\ffmpeg\\bin\\ffmpeg.exe");// 必填
            dto.put("input_path", "D:\\self\\test\\5\\" + i + ".mp4");// 必填
            dto.put("video_converted_path", firstpath);// 必填
            dto.put("logo", "C\\\\:/Users/71051/Pictures/q1.jpg");
            dto.put("xaxis", "810");
            dto.put("yaxis", "10");
            FFMPEG f = new FFMPEG();
            String secondsString = f.videoTransfer(dto);

            dto.put("input_path", firstpath);// 必填
            dto.put("video_converted_path", secpath);// 必填
            dto.put("start-time", "00:00:00");
            dto.put("end-time", "00:45:00");
            f.videoCut(dto);

            File file = new File(firstpath);
            file.delete();
            System.out.println(i + "   转换共用:" + secondsString + "秒");
        }

    }

}

