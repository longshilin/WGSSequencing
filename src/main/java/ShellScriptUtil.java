import java.io.File;

/**
 * shell脚本工具包
 */
public class ShellScriptUtil {

    /**
     * Usage: scriptUtil  -shellScript  [-outputFile]  [-logFile]
     * 执行shellPath指定的脚本，shellScript参数是必须指定的
     * 另外两个是可选的，当不指定时默认输出方式
     * 其中shell脚本中的输出信息保存在outputFile,错误日志信息保存在logFile
     *
     * @param path 指定多个路径
     */
    public static void callProcess(String... path) {
        File outputFile;
        File logFile;
        Process process;

        String shellScript = path[0];
        String chmod = "chmod u+x " + shellScript;
        try {
            // 为shell脚本增加可执行权限
            Runtime.getRuntime().exec(chmod).waitFor();
        } catch (Exception e) {
            e.printStackTrace();
        }

        ProcessBuilder pb = new ProcessBuilder("./" + shellScript);
        pb.inheritIO();

        if (path.length == 3) {
            outputFile = new File(path[1]);
            // shell脚本执行结果输出
            pb.redirectOutput(outputFile);
            // shell脚本错误日志输出
            logFile = new File(path[2]);
            pb.redirectError(logFile);
        }
        if (path.length == 2) {
            // shell脚本错误日志输出
            logFile = new File(path[1]);
            pb.redirectError(logFile);
        }
        try {
            process = pb.start();
            process.waitFor();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
