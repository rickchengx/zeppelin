package org.apache.zeppelin.interpreter.launcher;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;

import org.apache.zeppelin.interpreter.remote.RemoteInterpreterManagedProcess;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class K8sRemoteInterpreterProcessMinikubeTest {

    @Test
    public void testPredefinedPortNumbers() {
        // given
        KubernetesClient client = new DefaultKubernetesClient();
        Properties properties = new Properties();
        Map<String, String> envs = new HashMap<>();

        K8sRemoteInterpreterProcess intp = new K8sRemoteInterpreterProcess(
                client,
                "default",
                new File(".skip"),
                "interpreter-container:1.0",
                "shared_process",
                "sh",
                "shell",
                properties,
                envs,
                "zeppelin.server.hostname",
                12320,
                false,
                "spark-container:1.0",
                10,
                10,
                false,
                false);


        // following values are hardcoded in k8s/interpreter/100-interpreter.yaml.
        // when change those values, update the yaml file as well.
        assertEquals("12321:12321", intp.getInterpreterPortRange());
        assertEquals(22321, intp.getSparkDriverPort());
        assertEquals(22322, intp.getSparkBlockmanagerPort());
    }

    @Test
    public void testK8sStartSuccessful() throws IOException {
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
                "sh",
                "shell",
                properties,
                envs,
                "zeppelin.server.service",
                12320,
                false,
                "spark-container:1.0",
                100000,
                10,
                false,
                false);

        ExecutorService service = Executors.newFixedThreadPool(1);
        service.submit(new PodStatusChecker(client, intp.getNamespace(), intp.getPodName(), intp));

        intp.start("TestUser");
        // then
        assertEquals("Running", intp.getPodPhase());
    }

    class PodStatusChecker implements Runnable {

        private final KubernetesClient client;
        private final String namespace;
        private final String podName;
        private final RemoteInterpreterManagedProcess process;

        public PodStatusChecker(
                KubernetesClient client,
                String namespace,
                String podName,
                RemoteInterpreterManagedProcess process) {
            this.client = client;
            this.namespace = namespace;
            this.podName = podName;
            this.process = process;
        }

        @Override
        public void run() {
            try {
                Instant timeoutTime = Instant.now().plusSeconds(10);
                while (timeoutTime.isAfter(Instant.now())) {
                    Pod pod = client.pods().inNamespace(namespace).withName(podName).get();
                    if (pod != null) {
                        TimeUnit.SECONDS.sleep(1);
                        String phase = pod.getStatus().getPhase();
                        System.out.println(phase);
                        if (phase.equals("Running")) {
                            process.processStarted(12320, "testing");
                        }
                    } else {
                        TimeUnit.MILLISECONDS.sleep(100);
                    }
                }
            } catch (InterruptedException e) {
                // Do nothing
            }
        }
    }
}
