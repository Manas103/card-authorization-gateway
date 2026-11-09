package com.manas.cardauth.health;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * The React console (console/, a standalone Vite app, not bundled into this
 * jar, see README's "Building and running") runs on its own dev-server port
 * and calls {@code /api/health/**} cross-origin. Permissive by design and
 * scoped to localhost only: this is a local demo console, not a deployed
 * multi-origin product.
 */
@Configuration
public class HealthConsoleCorsConfig implements WebMvcConfigurer {

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/api/health/**")
                .allowedOriginPatterns("http://localhost:*")
                .allowedMethods("GET");
    }
}
