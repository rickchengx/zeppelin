package org.apache.zeppelin.interpreter.launcher;

import org.apache.zeppelin.interpreter.*;

import org.codehaus.plexus.util.xml.pull.XmlPullParserException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;


import static org.junit.Assert.*;


public class K8sMinikubeTestBasic {
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
    public void testK8sStartShellSuccessful() throws InterpreterException {
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
    public void testK8sStartPythonSuccessful() throws InterpreterException {
        // given
        InterpreterSetting interpreterSetting = interpreterSettingManager.getInterpreterSettingByName("python");
        interpreterSetting.setProperty("zeppelin.k8s.interpreter.container.image", "local/zeppelin");
        interpreterSetting.setProperty("ZEPPELIN_CONF_DIR", "/opt/zeppelin/conf");
        interpreterSetting.setProperty("ZEPPELIN_HOME", "/opt/zeppelin");
        interpreterSetting.setProperty("zeppelin.k8s.interpreter.container.imagePullPolicy", "Never");
        interpreterSetting.setProperty("zeppelin.python", "python3");

        // test shell interpreter
        Interpreter interpreter = interpreterFactory.getInterpreter("python", new ExecutionContext("user1", "note1", "test"));

        InterpreterContext context = new InterpreterContext.Builder().setNoteId("note1").setParagraphId("paragraph_1").build();
        InterpreterResult interpreterResult = interpreter.interpret("foo = True\nprint(foo)", context);
        assertEquals(interpreterResult.toString(), InterpreterResult.Code.SUCCESS, interpreterResult.code());
        assertTrue(interpreterResult.toString(), interpreterResult.message().get(0).getData().contains("True"));
    }

}
