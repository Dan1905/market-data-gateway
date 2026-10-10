package com.mdg.gateway.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * The root path leads somewhere.
 *
 * <p>This service has no home page, and should not grow one. But someone who opens its
 * address in a browser deserves better than "404": they are sent to the event contract,
 * which is the one document that explains what the gateway is for.
 */
@Configuration(proxyBeanMethods = false)
public class WebConfig implements WebMvcConfigurer {

    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
        registry.addRedirectViewController("/", "/asyncapi.html");
    }
}
