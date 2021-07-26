package org.apache.zeppelin.interpreter.launcher;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;

import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.apache.zeppelin.interpreter.*;

import org.codehaus.plexus.util.xml.pull.XmlPullParserException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.*;


public class K8sRemoteInterpreterProcessMinikubeTest {
    private static MiniZeppelin zeppelin;
    private static InterpreterFactory interpreterFactory;
    private static InterpreterSettingManager interpreterSettingManager;

    @BeforeClass
    public static void setUp() throws IOException {
        zeppelin = new MiniZeppelin();
        zeppelin.start(K8sRemoteInterpreterProcessMinikubeTest.class);
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
    public void testK8sStartSuccessful() throws InterpreterException {
        // given
        InterpreterSetting interpreterSetting = interpreterSettingManager.getInterpreterSettingByName("sh");
        interpreterSetting.setProperty("zeppelin.k8s.interpreter.container.image", "local/zeppelin");
        interpreterSetting.setProperty("ZEPPELIN_CONF_DIR", "/opt/zeppelin/conf");
        interpreterSetting.setProperty("ZEPPELIN_HOME", "/opt/zeppelin");
        interpreterSetting.setProperty("zeppelin.k8s.interpreter.container.imagePullPolicy", "Never");

        // test shell interpreter
        Interpreter interpreter = interpreterFactory.getInterpreter("sh", new ExecutionContext("user1", "note1", "test"));

        InterpreterContext context = new InterpreterContext.Builder().setNoteId("note1").setParagraphId("paragraph_1").build();
        InterpreterResult interpreterResult = interpreter.interpret("pwd", context);
        assertEquals(interpreterResult.toString(), InterpreterResult.Code.SUCCESS, interpreterResult.code());
    }



    @Test
    public void testK8sStartSparkSuccessful() throws IOException, XmlPullParserException, InterpreterException {
        // given
        InterpreterSetting interpreterSetting = interpreterSettingManager.getInterpreterSettingByName("spark");
        interpreterSetting.setProperty("zeppelin.k8s.interpreter.container.image", "local/zeppelin");
        interpreterSetting.setProperty("ZEPPELIN_CONF_DIR", "/opt/zeppelin/conf");
        interpreterSetting.setProperty("ZEPPELIN_HOME", "/opt/zeppelin");
        interpreterSetting.setProperty("zeppelin.k8s.interpreter.container.imagePullPolicy", "Never");

        interpreterSetting.setProperty("zeppelin.k8s.spark.container.imagePullPolicy", "Never");
        interpreterSetting.setProperty("zeppelin.k8s.spark.container.image", "local/spark");
        interpreterSetting.setProperty("SPARK_HOME", "/spark");
        interpreterSetting.setProperty("spark.master", "k8s://https://kubernetes.default.svc");
        interpreterSetting.setProperty("zeppelin.spark.enableSupportedVersionCheck", "false");

        interpreterSetting.setProperty("spark.kubernetes.container.image.pullPolicy", "Never");
        interpreterSetting.setProperty("SPARK_PRINT_LAUNCH_COMMAND", "true");

        interpreterSetting.setProperty("zeppelin.spark.useHiveContext", "false");
        interpreterSetting.setProperty("zeppelin.pyspark.useIPython", "false");
        interpreterSetting.setProperty("spark.driver.memory", "1g");

        interpreterSetting.setProperty("spark.driver.cores", "500m");
        interpreterSetting.setProperty("spark.kubernetes.driver.request.cores", "500m");

        //interpreterSetting.setProperty("spark.executor.cores", "1");
        interpreterSetting.setProperty("spark.executor.memory", "1g");
        interpreterSetting.setProperty("spark.executor.instances", "1");

        interpreterSetting.setProperty("spark.kubernetes.executor.request.cores","500m");

        interpreterSetting.setProperty("zeppelin.spark.scala.color", "false");
        interpreterSetting.setProperty("zeppelin.spark.deprecatedMsg.show", "false");

        MavenXpp3Reader reader = new MavenXpp3Reader();
        Model model = reader.read(new FileReader("pom.xml"));
        interpreterSetting.setProperty("spark.jars", new File("target/zeppelin-interpreter-integration-" + model.getVersion() + ".jar").getAbsolutePath());
        interpreterSetting.setProperty("spark.jars.packages", "com.maxmind.geoip2:geoip2:2.5.0");

        // test spark interpreter
        Interpreter interpreter = interpreterFactory.getInterpreter("spark.spark", new ExecutionContext("user1", "note1", "test"));

        InterpreterContext context = new InterpreterContext.Builder().setNoteId("note1").setParagraphId("paragraph_1").build();

        InterpreterResult interpreterResult = interpreter.interpret("sc.range(1,10).sum()", context);
        assertEquals(interpreterResult.toString(), InterpreterResult.Code.SUCCESS, interpreterResult.code());
        assertTrue(interpreterResult.toString(), interpreterResult.message().get(0).getData().contains("45"));


        // test jars & packages can be loaded correctly
        interpreterResult = interpreter.interpret("import org.apache.zeppelin.interpreter.integration.DummyClass\n" +
                "import com.maxmind.geoip2._", context);
        assertEquals(interpreterResult.toString(), InterpreterResult.Code.SUCCESS, interpreterResult.code());

        // test PySparkInterpreter
        Interpreter pySparkInterpreter = interpreterFactory.getInterpreter("spark.pyspark", new ExecutionContext("user1", "note1", "test"));
        interpreterResult = pySparkInterpreter.interpret("sqlContext.createDataFrame([(1,'a'),(2,'b')], ['id','name']).registerTempTable('test')", context);
        assertEquals(interpreterResult.toString(), InterpreterResult.Code.SUCCESS, interpreterResult.code());

        // test IPySparkInterpreter
        Interpreter ipySparkInterpreter = interpreterFactory.getInterpreter("spark.ipyspark", new ExecutionContext("user1", "note1", "test"));
        interpreterResult = ipySparkInterpreter.interpret("sqlContext.table('test').show()", context);
        assertEquals(interpreterResult.toString(), InterpreterResult.Code.SUCCESS, interpreterResult.code());

        // test SparkSQLInterpreter
        Interpreter sqlInterpreter = interpreterFactory.getInterpreter("spark.sql", new ExecutionContext("user1", "note1", "test"));
        interpreterResult = sqlInterpreter.interpret("select count(1) as c from test", context);
        assertEquals(interpreterResult.toString(), InterpreterResult.Code.SUCCESS, interpreterResult.code());
        assertEquals(interpreterResult.toString(), InterpreterResult.Type.TABLE, interpreterResult.message().get(0).getType());
        assertEquals(interpreterResult.toString(), "c\n2\n", interpreterResult.message().get(0).getData());
    }


/*
    @Test
    public void testK8sStartFailed() {
        // given
        KubernetesClient client = new DefaultKubernetesClient();
        Properties properties = new Properties();
        Map<String, String> envs = new HashMap<>();
        envs.put("SERVICE_DOMAIN", "mydomain");
        envs.put("ZEPPELIN_HOME", "/opt/zeppelin/");
        URL url = Thread.currentThread().getContextClassLoader()
                .getResource("k8s-specs/interpreter-spec-ci-minikube.yaml");
        File file = new File(url.getPath());

        K8sRemoteInterpreterProcess intp = new K8sRemoteInterpreterProcess(
                client,
                "default",
                file,
                "local/zeppelin",
                "shared_process",
                "spark",
                "myspark",
                properties,
                envs,
                "zeppelin.server.service",
                12320,
                false,
                "spark-container:1.0",
                3000,
                10,
                false,
                true);



        ExecutorService service = Executors.newFixedThreadPool(1);
        service.submit(new PodStatusChecker(client, intp.getNamespace(), intp.getPodName(), intp));

        // should throw an IOException
        try {
            intp.start("TestUser");
            fail("We excepting an IOException");
        } catch (IOException e) {
            assertNotNull(e);
            // Check that the Pod is deleted
            assertNull(
                    server.getClient().pods().inNamespace(intp.getNamespace()).withName(intp.getPodName())
                            .get());
        }
    }
*/

}
