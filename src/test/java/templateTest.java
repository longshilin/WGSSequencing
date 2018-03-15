import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * 测试模板类
 */
public class templateTest {
    public static void template(String... path) throws Exception {
        TemplateEngine.init();
        Map<String,String> templateMap = new HashMap<>();
        templateMap.put("test","123");
        String templateFileName = path[0];
        String scriptFileName = path[1];
        String logFileName = path[2];
        File scriptFile = TemplateEngine.createDynamicContentAsFile(templateFileName, templateMap, scriptFileName);

        if (scriptFile != null) {
            ShellScriptUtil.callProcess(scriptFileName, logFileName);
        }
    }
    public static void main(String[] args) throws Exception {
        templateTest.template("test","./test.sh","./test.log");
    }
}
