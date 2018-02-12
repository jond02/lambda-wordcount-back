package com.jdann.aws.wc;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.jdann.aws.wc.dto.Word;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WordCountConsumerHandler implements RequestHandler<String, String> {

    private static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
    private static DynamoDBMapper mapper = new DynamoDBMapper(client);
    private static DynamoDB dynamoDB = new DynamoDB(client);
    private static final int LIMIT = 10;
    private static final String DYNAMODB_TABLE_NAME = "word_totals";
    private static final String WORD_COUNT_QUEUE = "https://sqs.us-west-2.amazonaws.com/736338261372/word-count";
    private final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();

    public String handleRequest(String string, Context context) {

        List<Message> messages = sqs.receiveMessage(WORD_COUNT_QUEUE).getMessages();

        if (!messages.isEmpty()) {
            Message message = messages.get(0);
            findTopWords(message.getBody());
            sqs.deleteMessage(WORD_COUNT_QUEUE, message.getReceiptHandle());
        }
        return "Done";
    }

    private void findTopWords(String address) {

        if (address == null || address.trim().length() == 0) {
            return;
        }

        //check if already in database
        List<Word> words = findAllWithHashKey(address);
        if (words.isEmpty()) {

            //need to process content for the first time
            String content = fetchContent(address);
            words = processContent(content, address);

            System.out.println("saving " + words);
            //save in database
            save(words);
        }
    }

    private List<Word> findAllWithHashKey(String hashKey) {

        Map<String, AttributeValue> attributeValues = new HashMap<>();
        attributeValues.put(":val", new AttributeValue().withS(hashKey));

        ScanRequest scanRequest = new ScanRequest()
                .withTableName(DYNAMODB_TABLE_NAME)
                .withFilterExpression("address = :val")
                .withExpressionAttributeValues(attributeValues);

        ScanResult result = client.scan(scanRequest);
        List<Word> words = new ArrayList<>();

        //marshall the results into WordTotals
        result.getItems().forEach(item -> {

            Word word = new Word();
            item.forEach((k, v) -> {
                switch (k) {
                    case "address"   : word.setAddress(v.getS()); break;
                    case "word"      : word.setWord(v.getS()); break;
                    case "total"     : word.setTotal(Long.valueOf(v.getN())); break;
                }
            });
            words.add(word);
        });
        return words;
    }

    private void save(Word word) {
        mapper.save(word);
    }

    private void save(List<Word> words) {
        words.forEach(this::save);
    }

    private List<Word> processContent(String content, String address) {

        List<Word> words = new ArrayList<>();

        if (content == null) {
            return words;
        }

        //split the text by sequence of non-alphanumeric characters and get totals
        String[] wordSequence = content.split("[^\\w']");

        //get totals
        for (String word : wordSequence) {

            if (word.trim().length() == 0) {
                continue;
            }
            Word current = new Word(address, word);
            int i = words.indexOf(current);

            if (i > -1) {
                words.get(i).inc();
            } else {
                words.add(current);
            }
        }
        words.sort(Word.byTotal);

        //return entries up to the set limit
        return words.subList(0, words.size() > LIMIT ? LIMIT : words.size());
    }

    private String fetchContent(String address) {

        if (address == null) {
            return null;
        }

        URL url;
        InputStream is = null;
        BufferedReader br;
        String line;

        try {
            url = new URL(address);
            is = url.openStream();
            br = new BufferedReader(new InputStreamReader(is));
            StringBuilder builder = new StringBuilder();

            while ((line = br.readLine()) != null) {
                builder.append(line);
            }
            return builder.toString();

        } catch (IOException ignore) {
        } finally {
            try {
                if (is != null) {
                    is.close();
                }
            } catch (IOException ignore) {
            }
        }
        return null;
    }

}
