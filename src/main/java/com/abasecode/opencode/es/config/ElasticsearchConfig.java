package com.abasecode.opencode.es.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author Jon
 * e-mail: ijonso123@gmail.com
 * url: <a href="https://jon.wiki">Jon's blog</a>
 * url: <a href="https://github.com/abasecode">project github</a>
 * url: <a href="https://abasecode.com">AbaseCode.com</a>
 */
@Configuration
@Component
public class ElasticsearchConfig {

    @Bean
    @ConfigurationProperties(prefix = "app.elasticsearch")
    public EsConfig esConfig() {
        return new EsConfig();
    }

    @Setter
    @Getter
    public static class EsConfig {
        private List<String> uris;

        public List<String> getUris() {
            return uris;
        }

        public String getPassword() {
            return password;
        }

        public String getUsername() {
            return username;
        }

        private String password;
        private String username;
    }
}
