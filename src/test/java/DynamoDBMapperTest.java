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

import static org.junit.jupiter.api.Assertions.*;

public class DynamoDBMapperTest {

    private static AmazonDynamoDB amazonDynamoDB;
    private static DynamoDBMapper dynamoDBMapper;

    /**
     * 全テスト実行前に1回のみ実行される
     */
    @BeforeAll
    static void setDynamoDB() {
        AWSCredentials awsCredentials = new BasicAWSCredentials("dummy", "dummykey");
        AWSCredentialsProvider awsCredentialsProvider = new AWSStaticCredentialsProvider(awsCredentials);
        AwsClientBuilder.EndpointConfiguration endpointConfiguration =
                new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "ap-northeast-1");
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
     * アイテム登録
     */
    @Test
    void putItem() {
        // データ設定
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
     * アイテム取得
     */
    @Test
    @Disabled
    void getItem() {
        // putItem()の下記の登録したアイテムを取得するところで確認できるのでスルーする
        // UserModel getUserModel = dynamoDBMapper.load(UserModel.class, userModel.getId(), userModel.getGender());
    }

    /**
     * アイテム更新
     */
    @Test
    void updItem() {
        // データ設定
        UserModel userModel = new UserModel();
        userModel.setId("yama2020");
        userModel.setGender("man");
        userModel.setName("yamada");
        userModel.setAge(10);
        // アイテム登録
        dynamoDBMapper.save(userModel);

        // 更新用データ設定
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
        // データ設定
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
}
