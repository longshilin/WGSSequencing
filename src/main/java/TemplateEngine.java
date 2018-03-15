import freemarker.template.Configuration;
import freemarker.template.DefaultObjectWrapper;
import freemarker.template.Template;

import java.io.*;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import freemarker.template.TemplateException;
import org.apache.log4j.Logger;

/**
 * 这个类使用FreeMarker(http://freemarker.apache.org)
 * FreeMarker是一个模板引擎，这是一个根据模板生成文本输出的通用工具
 * (从shell脚本到自动生成的源代码都是文本输出)
 */
public class TemplateEngine {

    // 日志记录
    private static Logger theLogger = Logger.getLogger(TemplateEngine.class.getName());

    // 通常在整个应用生命周期中只执行一次
    private static Configuration TEMPLATE_CONFIGURATION = null;
    private static AtomicBoolean initialized = new AtomicBoolean(false);

    // freemarker templates Path
    protected static String TEMPLATE_DIRECTORY = "./templates";

    public static void init() throws Exception {
        if (initialized.get()) {
            //
            return;
        }
        initConfiguration();
        initialized.compareAndSet(false, true);
    }

    static {
        if (!initialized.get()) {
            try {
                init();
            } catch (Exception e) {
                theLogger.error("TemplateEnigine init failed at initialization.", e);
            }
        }
    }

    //
    private static void initConfiguration() throws Exception {
        TEMPLATE_CONFIGURATION = new Configuration();
        TEMPLATE_CONFIGURATION.setDirectoryForTemplateLoading(new File(TEMPLATE_DIRECTORY));
        TEMPLATE_CONFIGURATION.setObjectWrapper(new DefaultObjectWrapper());
        TEMPLATE_CONFIGURATION.setWhitespaceStripping(true);
        //
        TEMPLATE_CONFIGURATION.setClassicCompatible(true);
    }

    /**
     * 通过模板和keyValuePairs动态创建shell脚本
     * @param templateFileName         是一个模板文件名，如：script.sh.template，其模板目录已在configuration中指定
     * @param keyValuePairs        存储数据模型的<K,V>Map
     * @param outputScriptFilePath 脚本文件全路径名
     * @return 可执行脚本
     * @throws Exception
     */
    public static File createDynamicContentAsFile(String templateFileName, Map<String, String> keyValuePairs, String outputScriptFilePath) {
        if ((templateFileName == null) || (templateFileName.length() == 0)) {
            return null;
        }

        Writer writer = null;
        File outputFile = null;
        try {
            Template template = TEMPLATE_CONFIGURATION.getTemplate(templateFileName);
            // 合并数据模型和模板，生成shell脚本
            outputFile = new File(outputScriptFilePath);
            writer = new BufferedWriter(new FileWriter(outputFile));
            template.process(keyValuePairs, writer);
            writer.flush();
        } catch (IOException e) {
            theLogger.error("创建文件失败...", e);
        } catch (TemplateException e) {
            theLogger.error("freeMarker动态创建shell脚本失败...", e);
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return outputFile;
    }

}
