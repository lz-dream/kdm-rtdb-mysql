package cn.oge.kdm.rtdb.vzdb;

import java.io.IOException;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

import cn.oge.kdm.rtdb.autoconfigure.EnableKdmRtdbServer;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

@EnableSwagger2
@EnableDiscoveryClient
@EnableKdmRtdbServer
@SpringBootApplication
@MapperScan("cn.oge.kdm.rtdb.vzdb.mapper")
public class KdmRtdbVzdbBlockServerApplication {

	public static void main(String[] args) throws IOException {
		SpringApplication.run(KdmRtdbVzdbBlockServerApplication.class, args);
	}
}
