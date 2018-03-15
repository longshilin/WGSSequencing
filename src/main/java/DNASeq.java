import java.io.File;
import java.util.HashMap;
import java.util.Map;


public class DNASeq {

    // Execute shell logs local directory
    private static String LOG_DIRECTORY = "~/dnaseq/shell/logs";
    // freemarker dynamic generate shell scripts local directory
    protected static String SCRIPT_DIRECTORY = "~/freemarker/scripts";

    /*
     **********************************
     ********   步骤一 比对   ***********
     **********************************
     */

    /**
     * @param value      ${SampleName}_${fileNumber}\t${SampleName}_${fileNumber}_R1.fastq ${SampleName}_${fileNumber}_R2.fastq
     * @param runName    本次分析的名称
     * @throws Exception
     */
    public static void comparisonMapper(String value, String runName) {

        String delimiter = "\\s+"; // 匹配任何多个空白字符，包括空格、制表符、换行符等。
        String[] strArray = value.split(delimiter);
        HashMap<String, String> templateMap = new HashMap<>();
        templateMap.put("input_file_1", strArray[1]);
        templateMap.put("input_file_2", strArray[2]);
        templateMap.put("run_name", runName);

        String templateFileName = "comparison_mapper.template";
        String scriptFileName = SCRIPT_DIRECTORY + "/comparison_mapper_" + templateMap.get("run_name") + "_" + templateMap.get("analysis_id") + ".sh";
        String logFileName = LOG_DIRECTORY + "/comparison_mapper_" + templateMap.get("run_name") + "_" + templateMap.get("analysis_id") + ".log";

        // 从模板创建具体脚本
        File scriptFile = TemplateEngine.createDynamicContentAsFile(templateFileName, templateMap, scriptFileName);

        if (scriptFile != null) {
            ShellScriptUtil.callProcess(scriptFileName, logFileName);
        }


    }

    /**
     * 对于每个染色体，将已分区的染色体的小文件进行合并成一个大的bam文件
     * @param value chrID;run_name
     */
    public static void comparisonReducer(String value) throws Exception {
        TemplateEngine.init();
        String[] tokens = value.split(";");
        String charID = tokens[0];
        String run_name = tokens[1];
        HashMap<String, String> templateMap = new HashMap<>();
        templateMap.put("chr_id", charID);
        templateMap.put("analysis_id", run_name);

        String templateFileName = "comparison_reducer.template";
        String scriptFileName = SCRIPT_DIRECTORY + "/comparison_reducer_merge_" + templateMap.get("chr_id") + "_" + templateMap.get("analysis_id") + ".sh";
        String logFileName = LOG_DIRECTORY + "/comparison_reducer_" + templateMap.get("chr_id") + "_" + templateMap.get("analysis_id") + ".log";
        File scriptFile = TemplateEngine.createDynamicContentAsFile(templateFileName, templateMap, scriptFileName);

        if (scriptFile != null) {
            ShellScriptUtil.callProcess(scriptFileName, logFileName);
        }
    }

    /*
     **********************************
     ******** 步骤二 再校正   ***********
     **********************************
     */

    /**
     * 提取出文件位置所包含的模板信息
     *
     * 再校正
     *
     * @param value
     * @throws Exception
     */
    public static void recalibrationMapper(String value) throws Exception {
        TemplateEngine.init();
        // 文件列表格式为 chrID;run_name
        String delimiter = "\\s+";
        String[] strArray = value.split(delimiter);
        HashMap<String, String> templateMap = new HashMap<>();
        templateMap.put("chr_id", strArray[0]);
        templateMap.put("run_name", strArray[1]);

        String templateName = "recalibration_mapper.template";
        String scriptFileName = SCRIPT_DIRECTORY + "/recalibration_mapper_" + templateMap.get("key") + "_" + templateMap.get("analysis_id") + ".sh";
        String logFileName = LOG_DIRECTORY + "/recalibration_mapper_" + templateMap.get("key") + "_" + templateMap.get("analysis_id") + ".log";
        File scriptFile = TemplateEngine.createDynamicContentAsFile(templateName, templateMap, scriptFileName);

        if (scriptFile != null) {
            ShellScriptUtil.callProcess(scriptFileName, logFileName);
        }

    }


/*
    public static void recalibrationReducer(String analysisId) throws Exception {
        TemplateEngine.init();

        HashMap<String, String> templateMap = new HashMap<>();
        templateMap.put("key", "-");
        templateMap.put("analysis_id", analysisId);

        // 从一个模板文件创建具体的脚本
        String templateFileName = TemplateEngine.TEMPLATE_DIRECTORY + "recalibration_reducer.template";
        String scriptFileName = SCRIPT_DIRECTORY + "/recalibration_reducer_" + templateMap.get("analysis_id") + ".sh";
        String logFileName = LOG_DIRECTORY + "/recalibration_reducer_" + templateMap.get("analysis_id") + ".log";
        File scriptFile = TemplateEngine.createDynamicContentAsFile(templateFileName, templateMap, scriptFileName);
        if (scriptFile != null) {
            ShellScriptUtil.callProcess(scriptFileName, logFileName);
        }

    }
*/
    /*
     *********************************
     ******** 步骤三 变异检测 ***********
     **********************************
     */

    /**
     * @param value chrID;run_name\tjobID
     */
    public static void theVariantDetectionMapper(String value) throws Exception {

        TemplateEngine.init();
        // 文件列表格式为 chrID;run_name
        String delimiter = "\\s+";
        String[] strArray = value.split(delimiter);
        HashMap<String, String> templateMap = new HashMap<>();
        templateMap.put("chr_id", strArray[0]);
        templateMap.put("run_name", strArray[1]);

        String templateName = "variant_detection_mapper.template";
        String scriptFileName = "/dnaseq/variant_detection_mapper_" + templateMap.get("analysis_id") + "_" + templateMap.get("key") + ".sh";
        String logFileName = "/dnaseq/variant_detection_mapper_" + templateMap.get("analysis_id") + "_" + templateMap.get("key") + ".log";
        File scriptFile = TemplateEngine.createDynamicContentAsFile(templateName, templateMap, scriptFileName);
        if (scriptFile != null) {
            ShellScriptUtil.callProcess(scriptFileName, logFileName);
        }

    }

/*
    public static void theVariantDetectionReducer(String analysisId) throws Exception {
        TemplateEngine.init();
        HashMap<String, String> templateMap = new HashMap<>();
        templateMap.put("key", "-");
        templateMap.put("analysis_id", analysisId);
        // 从一个模板文件创建具体的脚本
        String scriptFileName = "/dnaseq/variant_detection_reducer_" + templateMap.get("analysis_id") + ".sh";
        String logFileName = "/dnaseq/variant_detection_recuder_" + templateMap.get("analysis_id") + ".log";

        File scriptFile = TemplateEngine.createDynamicContentAsFile("variant_detection_reducer.template", templateMap, scriptFileName);
        if (scriptFile != null) {
            ShellScriptUtil.callProcess(scriptFileName, logFileName);
        }

    }
*/

}
