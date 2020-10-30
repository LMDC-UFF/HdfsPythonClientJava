package br.uff.lmdc.HadoopPythonServer;

import lombok.extern.log4j.Log4j2;
import lombok.val;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import py4j.GatewayServer;

@Log4j2
@EnableScheduling
@SpringBootApplication
public class HadoopPythonServerApplication {

    public static void main(String[] args) {
        val ctx = SpringApplication.run(HadoopPythonServerApplication.class, args);
        val service = ctx.getBean(HadoopPythonService.class);
        GatewayServer gatewayServer = new GatewayServer(service);
        gatewayServer.start();
        log.info("GatewayServer started");
    }

}
