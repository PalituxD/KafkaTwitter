package org.palituxd.twitterkafkapoc.services;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;

public class StoreService {

    private static StoreService INSTANCE;
    private MongoDatabase twitterDatabase;

    private StoreService(){
        connect();
    }

    public synchronized static StoreService getInstance(){
        if(INSTANCE==null){
            INSTANCE = new StoreService();
        }
        return INSTANCE;
    }

    private void connect(){
        MongoClientURI uri = new MongoClientURI(
                "MongoURL");
        MongoClient mongoClient = new MongoClient(uri);
        twitterDatabase = mongoClient.getDatabase("TwitterData");
    }

    public MongoDatabase getTwitterDatabase() {
        return twitterDatabase;
    }
}
