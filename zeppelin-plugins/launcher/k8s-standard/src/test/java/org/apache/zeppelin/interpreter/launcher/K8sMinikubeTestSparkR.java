package org.apache.zeppelin.interpreter.launcher;

import org.apache.zeppelin.interpreter.*;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class K8sMinikubeTestSparkR {
    private static MiniZeppelin zeppelin;
    private static InterpreterFactory interpreterFactory;
    private static InterpreterSettingManager interpreterSettingManager;

    @BeforeClass
    public static void setUp() throws IOException {
        zeppelin = new MiniZeppelin();
        zeppelin.start(K8sMinikubeTestBasic.class);
        interpreterFactory = zeppelin.getInterpreterFactory();
        interpreterSettingManager = zeppelin.getInterpreterSettingManager();
    }


    @AfterClass
    public static void tearDown() throws IOException {
        if (zeppelin != null) {
            zeppelin.stop();
        }
    }

    @Test
    public void testK8sStartSparkRSuccessful() throws IOException, InterruptedException, XmlPullParserException, InterpreterException {
        // given
        InterpreterSetting interpreterSetting = interpreterSettingManager.getInterpreterSettingByName("spark");
        interpreterSetting.setProperty("zeppelin.k8s.interpreter.container.image", "local/zeppelin");
        interpreterSetting.setProperty("ZEPPELIN_CONF_DIR", "/opt/zeppelin/conf");
        interpreterSetting.setProperty("ZEPPELIN_HOME", "/opt/zeppelin");
        interpreterSetting.setProperty("zeppelin.k8s.interpreter.container.imagePullPolicy", "Never");

        interpreterSetting.setProperty("zeppelin.k8s.spark.container.imagePullPolicy", "Never");
        interpreterSetting.setProperty("zeppelin.k8s.spark.container.image", "local/spark-r:latest");
        interpreterSetting.setProperty("SPARK_HOME", "/spark");
        interpreterSetting.setProperty("spark.master", "k8s://https://kubernetes.default.svc");
        interpreterSetting.setProperty("zeppelin.spark.enableSupportedVersionCheck", "false");

        interpreterSetting.setProperty("PYSPARK_PYTHON", "python3");

        interpreterSetting.setProperty("spark.kubernetes.container.image", "local/spark-r:latest");
        interpreterSetting.setProperty("spark.kubernetes.container.image.pullPolicy", "Never");
        interpreterSetting.setProperty("SPARK_PRINT_LAUNCH_COMMAND", "true");

        interpreterSetting.setProperty("zeppelin.spark.useHiveContext", "false");
        interpreterSetting.setProperty("zeppelin.pyspark.useIPython", "false");
        interpreterSetting.setProperty("spark.driver.memory", "1g");

        interpreterSetting.setProperty("spark.driver.cores", "500m");
        interpreterSetting.setProperty("spark.kubernetes.driver.request.cores", "500m");

        interpreterSetting.setProperty("spark.executor.memory", "1g");
        interpreterSetting.setProperty("spark.executor.instances", "1");

        interpreterSetting.setProperty("spark.kubernetes.executor.request.cores","500m");

        interpreterSetting.setProperty("zeppelin.spark.scala.color", "false");
        interpreterSetting.setProperty("zeppelin.spark.deprecatedMsg.show", "false");

        // test SparkRInterpreter
        Interpreter sparkrInterpreter = interpreterFactory.getInterpreter("spark.r", new ExecutionContext("user1", "note1", "test"));
        InterpreterContext context = new InterpreterContext.Builder().setNoteId("note1").setParagraphId("paragraph_1").build();

        InterpreterResult interpreterResult = sparkrInterpreter.interpret("foo <-TRUE\nprint(foo)", context);
        assertEquals(interpreterResult.toString(), InterpreterResult.Code.SUCCESS, interpreterResult.code());
        assertTrue(interpreterResult.toString(), interpreterResult.message().get(0).getData().contains("TRUE"));
    }
}
