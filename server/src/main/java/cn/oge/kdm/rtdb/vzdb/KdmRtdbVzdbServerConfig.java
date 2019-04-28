package cn.oge.kdm.rtdb.vzdb;

import cn.oge.kdm.rtdb.vzdb.adapter.VzdbPointReadAdapter;
import cn.oge.kdm.rtdb.vzdb.mapper.EMRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KdmRtdbVzdbServerConfig {

    @Bean
    public VzdbPointReadAdapter readService(EMRepository emRepository) {
        return new VzdbPointReadAdapter(emRepository);
    }
}