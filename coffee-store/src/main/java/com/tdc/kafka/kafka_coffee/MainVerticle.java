package com.tdc.kafka.kafka_coffee;

import io.reactivex.Completable;
import io.vertx.core.DeploymentOptions;
import io.vertx.reactivex.core.AbstractVerticle;

public class MainVerticle extends AbstractVerticle {
    @Override
    public Completable rxStart() {
        DeploymentOptions mainConfig = new DeploymentOptions();
        mainConfig.setConfig(config());
        return vertx.rxDeployVerticle(new RepositoryVerticle(), mainConfig)
                .ignoreElement()
                .andThen(vertx.rxDeployVerticle(new EventStreamVerticle(), mainConfig))
                .ignoreElement()
                .andThen(vertx.rxDeployVerticle(new UserInputEventStream(), mainConfig))
                .ignoreElement();
    }
}
