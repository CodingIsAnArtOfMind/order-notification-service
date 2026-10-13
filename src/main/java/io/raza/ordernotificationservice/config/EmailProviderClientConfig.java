package io.raza.ordernotificationservice.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

@Configuration
public class EmailProviderClientConfig {

    @Bean
    public RestClient emailProviderRestClient(
            @Value("${email.provider.base-url}")
            String baseUrl) {

        return RestClient.builder()
                .baseUrl(baseUrl)
                .build();
    }
}