package org.palituxd.twitterkafkapoc.services;

import com.ibm.cloud.sdk.core.service.security.IamOptions;
import com.ibm.watson.discovery.v1.Discovery;
import com.ibm.watson.natural_language_understanding.v1.NaturalLanguageUnderstanding;
import com.ibm.watson.natural_language_understanding.v1.model.AnalysisResults;
import com.ibm.watson.natural_language_understanding.v1.model.AnalyzeOptions;
import com.ibm.watson.natural_language_understanding.v1.model.Features;
import com.ibm.watson.natural_language_understanding.v1.model.SentimentOptions;
import org.palituxd.twitterkafkapoc.IBMConstants;

import java.util.ArrayList;
import java.util.List;

public class SentimentAnalyzerClient {

    public static void main(String args[]) {
        IamOptions options = new IamOptions.Builder()
                .apiKey(IBMConstants.KEY)
                .build();

        Discovery service = new Discovery("2017-11-07", options);

        NaturalLanguageUnderstanding naturalLanguageUnderstanding = new NaturalLanguageUnderstanding("2018-11-16", options);
        naturalLanguageUnderstanding.setEndPoint(IBMConstants.URL);

        String url = "";

        List<String> targets = new ArrayList<>();
        //targets.add("pablo");

        SentimentOptions sentiment = new SentimentOptions.Builder()
                //.targets(targets)
                .build();

        Features features = new Features.Builder()
                .sentiment(sentiment)
                .build();

        AnalyzeOptions parameters = new AnalyzeOptions.Builder()
                .text("El congreso peruano es pésimo, no se preocupa por las leyes que nos beneficien")
                .features(features)
                .build();

        AnalysisResults response = naturalLanguageUnderstanding
                .analyze(parameters)
                .execute()
                .getResult();
        System.out.println(response);
    }
}
