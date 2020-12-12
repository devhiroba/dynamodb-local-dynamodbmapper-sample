import models.UserModel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import util.DynamoDBUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DynamoDBLoadSeedDataTest {

    private DynamoDBUtil dynamoDBUtil;

    public DynamoDBLoadSeedDataTest() {
        this.dynamoDBUtil = new DynamoDBUtil();
    }

    /**
     * テーブル作成
     */
    @BeforeEach
    void beforeEach(){
        dynamoDBUtil.createTable();
    }

    /**
     * テーブル削除
     */
    @AfterEach
    void afterEach() {
        dynamoDBUtil.deleteTable();
    }

    /**
     * シードデータを登録する
     * UserModelのみ登録可能
     */
    @Test
    void seedDataLoaderForUserModelTest() {
        /**
         * アイテム登録
         */
        dynamoDBUtil.seedDataLoaderForUserModel("src/test/resources/seed-data-usermodel.json");
        /**
         * アイテム取得
         */
        UserModel userModel = new UserModel();
        userModel.setId("yama1010");
        userModel.setGender("man");
        UserModel userModel2 = new UserModel();
        userModel2.setId("mori9910");
        userModel2.setGender("man");
        List<UserModel> userModelList = new ArrayList<>();
        userModelList.add(userModel);
        userModelList.add(userModel2);
        Map<String, List<Object>> items = dynamoDBUtil.dynamoDBMapper.batchLoad(userModelList);
        System.out.println(items.get("UserModel"));
    }

    /**
     * シードデータを登録する
     */
    @Test
    void seedDataLoaderForAllModelTest() {
        dynamoDBUtil.<UserModel>seedDataLoaderForAllModel("src/test/resources/seed-data-v1.json", UserModel.class);
    }

    /**
     * シードデータを登録する
     */
    @Test
    void seedDataLoaderForAllModelV2Test() {
        dynamoDBUtil.<UserModel>seedDataLoaderForAllModelV2("src/test/resources/seed-data-v2.json", UserModel[].class);
    }
}
