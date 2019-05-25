package org.palituxd.twitterkafkapoc.services;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.palituxd.twitterkafkapoc.BDConstants;
import org.palituxd.twitterkafkapoc.TwitterConstants;

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
        MongoClientURI uri = new MongoClientURI(BDConstants.URL_CONNECTION);
        MongoClient mongoClient = new MongoClient(uri);
        twitterDatabase = mongoClient.getDatabase(BDConstants.DATABASE);
    }

    public MongoDatabase getConnection() {
        return twitterDatabase;
    }

    public void insertOne(Document document){
        MongoCollection<Document> coll = this.getConnection().getCollection(BDConstants.DOCUMENT);
        coll.insertOne(document);
    }
}
