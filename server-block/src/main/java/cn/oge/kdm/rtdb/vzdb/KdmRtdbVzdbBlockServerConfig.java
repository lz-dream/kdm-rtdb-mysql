package cn.oge.kdm.rtdb.vzdb;

import cn.oge.kdm.rtdb.vzdb.adapter.VzdbBlockReadAdapter;
import cn.oge.kdm.rtdb.vzdb.mapper.EMRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KdmRtdbVzdbBlockServerConfig {

    @Bean
    public VzdbBlockReadAdapter readService(EMRepository em) {
        return new VzdbBlockReadAdapter(em);
    }

}