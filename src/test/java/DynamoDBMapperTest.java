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
import models.UserModel;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class DynamoDBMapperTest {

    private static AmazonDynamoDB amazonDynamoDB;
    private static DynamoDBMapper dynamoDBMapper;

    /**
     * DB接続情報はオンコディングしない
     * application.yml 等を参照するようにすること
     */
    private static final String region = "ap-northeast-1";
    private static final String endpointUrl = "http://localhost:8000";
    private static final String accessKey = "dummy";
    private static final String secretKey = "dummykey";

    /**
     * 全テスト実行前に1回のみ実行される
     */
    @BeforeAll
    static void setDynamoDB() {
        AWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
        AWSCredentialsProvider awsCredentialsProvider = new AWSStaticCredentialsProvider(awsCredentials);
        AwsClientBuilder.EndpointConfiguration endpointConfiguration =
                new AwsClientBuilder.EndpointConfiguration(endpointUrl, region);
        amazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(awsCredentialsProvider)
                .withEndpointConfiguration(endpointConfiguration).build();
        dynamoDBMapper = new DynamoDBMapper(amazonDynamoDB, DynamoDBMapperConfig.DEFAULT);
    }

    /**
     * テーブル作成
     * 各テストの実行前に実行される
     */
    @BeforeEach
    void createTable() {
        CreateTableRequest createTableRequest = dynamoDBMapper.generateCreateTableRequest(UserModel.class)
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L));
        assertTrue(TableUtils.createTableIfNotExists(amazonDynamoDB, createTableRequest));
    }

    /**
     * テーブル削除
     * 各テストの実行後に実行される
     */
    @AfterEach
    void deleteTable() {
        DeleteTableRequest deleteTableRequest = dynamoDBMapper.generateDeleteTableRequest(UserModel.class);
        assertTrue(TableUtils.deleteTableIfExists(amazonDynamoDB, deleteTableRequest));
    }

    /**
     * アイテム登録＆アイテム取得
     */
    @Test
    void putItem() {
        // アイテム設定
        UserModel userModel = new UserModel();
        userModel.setId("yama2020");
        userModel.setGender("man");
        userModel.setName("yamada");
        userModel.setAge(10);
        // アイテム登録
        dynamoDBMapper.save(userModel);
        // 登録したアイテムを取得
        UserModel getUserModel = dynamoDBMapper.load(UserModel.class, userModel.getId(), userModel.getGender());
        // log
        System.out.println("putItem result : " + getUserModel.getId());
        System.out.println("putItem result : " + getUserModel.getGender());
        System.out.println("putItem result : " + getUserModel.getName());
        System.out.println("putItem result : " + getUserModel.getAge());
        // 結果確認
        assertEquals(userModel.getId(), getUserModel.getId());
    }

    /**
     * アイテム更新
     */
    @Test
    void updItem() {
        // アイテム設定
        UserModel userModel = new UserModel();
        userModel.setId("yama2020");
        userModel.setGender("man");
        userModel.setName("yamada");
        userModel.setAge(10);
        // アイテム登録
        dynamoDBMapper.save(userModel);

        // 更新用アイテム設定
        UserModel updUserModel = new UserModel();
        updUserModel.setId("yama2020");
        updUserModel.setGender("man");
        updUserModel.setName("nakamura");
        updUserModel.setAge(15);
        // アイテム登録
        dynamoDBMapper.save(updUserModel);
        // 登録したアイテムを取得
        UserModel getUserModel = dynamoDBMapper.load(UserModel.class, userModel.getId(), userModel.getGender());
        // log
        System.out.println("updItem result : " + getUserModel.getId());
        System.out.println("updItem result : " + getUserModel.getGender());
        System.out.println("updItem result : " + getUserModel.getName());
        System.out.println("updItem result : " + getUserModel.getAge());
        // 結果確認
        assertEquals(updUserModel.getAge(), getUserModel.getAge());
    }

    /**
     * アイテム削除
     */
    @Test
    void deleteItem() {
        // アイテム設定
        UserModel userModel = new UserModel();
        userModel.setId("yama2020");
        userModel.setGender("man");
        userModel.setName("yamada");
        userModel.setAge(10);
        // アイテム登録
        dynamoDBMapper.save(userModel);
        assertNotNull(dynamoDBMapper.load(UserModel.class, userModel.getId(), userModel.getGender()));

        // アイテム削除
        dynamoDBMapper.delete(userModel);
        assertNull(dynamoDBMapper.load(UserModel.class, userModel.getId(), userModel.getGender()));
    }

    /**
     * アイテム登録（複数件）、アイテム取得（複数件）
     */
    @Test
    void putItems() {
        // アイテム設定
        UserModel userModel = new UserModel();
        userModel.setId("yama2020");
        userModel.setGender("man");
        userModel.setName("yamada");
        userModel.setAge(10);
        UserModel userModel2 = new UserModel();
        userModel2.setId("mori1900");
        userModel2.setGender("man");
        userModel2.setName("morita");
        userModel2.setAge(20);
        List<UserModel> userModelList = new ArrayList<>();
        userModelList.add(userModel);
        userModelList.add(userModel2);

        // アイテム登録
        dynamoDBMapper.batchSave(userModelList);
        // アイテム登録
        //dynamoDBMapper.batchSave(Arrays.asList(userModel, userModel2));

        // 取得するアイテムの条件設定
        UserModel resultUserModel = new UserModel();
        resultUserModel.setId(userModel.getId());
        resultUserModel.setGender(userModel.getGender());
        UserModel resultUserModel2 = new UserModel();
        resultUserModel2.setId(userModel2.getId());
        resultUserModel2.setGender(userModel2.getGender());
        ArrayList<Object> resultUserModelList = new ArrayList<Object>();
        resultUserModelList.add(resultUserModel);
        resultUserModelList.add(resultUserModel2);

        // アイテム取得
        Map<String, List<Object>> items = dynamoDBMapper.batchLoad(resultUserModelList);
        assertEquals(userModelList, items.get("UserModel"));
    }

    /**
     * アイテム削除（複数件）
     */
    @Test
    void delItems() {
        // アイテム設定
        UserModel userModel = new UserModel();
        userModel.setId("yama2020");
        userModel.setGender("man");
        UserModel userModel2 = new UserModel();
        userModel2.setId("mori1900");
        userModel2.setGender("man");
        // アイテム登録
        dynamoDBMapper.batchSave(Arrays.asList(userModel, userModel2));
        // 取得するアイテム設定
        UserModel deleteUserModel = new UserModel();
        deleteUserModel.setId(userModel.getId());
        deleteUserModel.setGender(userModel.getGender());
        UserModel deleteUserModel2 = new UserModel();
        deleteUserModel2.setId(userModel2.getId());
        deleteUserModel2.setGender(userModel2.getGender());
        ArrayList<Object> deleteUserModelList = new ArrayList<Object>();
        deleteUserModelList.add(deleteUserModel);
        deleteUserModelList.add(deleteUserModel2);
        // 結果確認
        assertNotNull(dynamoDBMapper.batchLoad(deleteUserModelList).get("UserModel"));

        // アイテム削除
        dynamoDBMapper.batchDelete(deleteUserModelList);
        // アイテム削除
        //dynamoDBMapper.batchDelete(Arrays.asList(deleteUserModel, deleteUserModel2));

        // 結果確認
        Map<String, List<Object>> resultItems = dynamoDBMapper.batchLoad(deleteUserModelList);
        assertTrue(resultItems.get("UserModel").size() == 0);
    }
}
