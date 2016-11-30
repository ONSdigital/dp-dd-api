package config;

import actors.KafkaActorSingleton;
import play.Application;
import play.GlobalSettings;

public class Global extends GlobalSettings {

    // todo make this work with the recommended Play 2.5 dependency inject static initialisation method

    @Override
    public void onStart(Application app) {

        System.out.println("\n\n\n###########  Starting Kafka polling in Global class  #############\n\n\n");

        super.onStart(app);
        KafkaActorSingleton.createActorToPollKafka();
    }
}