package com.scheduler.api;

import com.scheduler.SchedulerConfig;
import com.scheduler.TaskScheduler;
import io.javalin.Javalin;
import io.javalin.json.JavalinJackson;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchedulerApi implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SchedulerApi.class);

    private final Javalin app;
    private final TaskScheduler scheduler;

    public SchedulerApi(TaskScheduler scheduler, SchedulerConfig config) {
        this.scheduler = scheduler;

        ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

        this.app = Javalin.create(javalinConfig -> {
            javalinConfig.jsonMapper(new JavalinJackson(objectMapper, true));
            javalinConfig.showJavalinBanner = false;
        });

        TaskController taskController = new TaskController();
        taskController.registerRoutes(app, scheduler);

        registerGlobalExceptionHandlers();
        registerLifecycleHooks();

        log.info("SchedulerApi configured on port {}", config.getApiPort());
    }

    public void start(int port) {
        app.start(port);
        log.info("SchedulerApi started on port {}", port);
    }

    public void stop() {
        app.stop();
        log.info("SchedulerApi stopped");
    }

    public Javalin getApp() {
        return app;
    }

    private void registerGlobalExceptionHandlers() {
        app.exception(Exception.class, (e, ctx) -> {
            log.error("Unhandled exception on {} {}", ctx.method(), ctx.path(), e);
            ctx.status(500).json(java.util.Map.of(
                    "error", "Internal server error",
                    "message", e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName()
            ));
        });

        app.exception(IllegalArgumentException.class, (e, ctx) -> {
            ctx.status(400).json(java.util.Map.of("error", e.getMessage()));
        });
    }

    private void registerLifecycleHooks() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook triggered, stopping API...");
            stop();
        }));
    }

    @Override
    public void close() {
        stop();
    }
}
