package util;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import models.UserModel;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DynamoDBUtil {

    /**
     * DB接続情報はオンコディングしない
     * application.yml 等を参照するようにすること
     */
    private final String region = "ap-northeast-1";
    private final String endpointUrl = "http://localhost:8000";
    private final String accessKey = "dummy";
    private final String secretKey = "dummykey";

    private AmazonDynamoDB amazonDynamoDB;
    public DynamoDBMapper dynamoDBMapper;

    public DynamoDBUtil() {
        AWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
        AWSCredentialsProvider awsCredentialsProvider = new AWSStaticCredentialsProvider(awsCredentials);
        AwsClientBuilder.EndpointConfiguration endpointConfiguration =
                new AwsClientBuilder.EndpointConfiguration(endpointUrl, region);
        amazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(awsCredentialsProvider)
                .withEndpointConfiguration(endpointConfiguration).build();
        dynamoDBMapper = new DynamoDBMapper(amazonDynamoDB, DynamoDBMapperConfig.DEFAULT);
        assertNotNull(amazonDynamoDB);
        assertNotNull(dynamoDBMapper);
    }

    /**
     * テーブル作成
     */
    public void createTable() {
        CreateTableRequest createTableRequest = dynamoDBMapper.generateCreateTableRequest(UserModel.class)
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L));
        assertTrue(TableUtils.createTableIfNotExists(amazonDynamoDB, createTableRequest));
    }

    /**
     * テーブル削除
     */
    public void deleteTable() {
        DeleteTableRequest deleteTableRequest = dynamoDBMapper.generateDeleteTableRequest(UserModel.class);
        assertTrue(TableUtils.deleteTableIfExists(amazonDynamoDB, deleteTableRequest));
    }

    /**
     * シードデータをロードする。
     * UserModelのみ利用可能
     */
    public void seedDataLoaderForUserModel(String seedFile) {
        Gson gson = new Gson();
        Type modelType = new TypeToken<ArrayList<UserModel>>(){}.getType();
        List<UserModel> modelList = null;
        try {
            modelList = gson.fromJson(new FileReader(seedFile), modelType);
        } catch(FileNotFoundException e) {
            System.out.println(e.getMessage());
        }
        dynamoDBMapper.batchSave(modelList);
    }

    /**
     * シードデータをロードする。
     */
    public <T> void seedDataLoaderForAllModel(String seedFile, Class<T> classType) {
        Gson gson = new Gson();
        Type collectionType = TypeToken.getParameterized(List.class, classType).getType();
        List<T> modelList = null;
        try {
            modelList = gson.fromJson(new FileReader(seedFile), collectionType);
        } catch(FileNotFoundException e) {
            System.out.println(e.getMessage());
        }
        dynamoDBMapper.batchSave(modelList);
    }

    /**
     * シードデータをロードする。
     */
    public <T> void seedDataLoaderForAllModelV2(String seedFile, Class<T[]> classes) {
        Gson gson = new Gson();
        T[] models = null;
        try {
            models = gson.fromJson(new FileReader(seedFile), classes);
        } catch(FileNotFoundException e) {
            System.out.println(e.getMessage());
        }
        dynamoDBMapper.batchSave(models);
    }
}
