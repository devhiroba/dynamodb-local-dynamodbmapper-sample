package models;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import lombok.Data;

@Data
@DynamoDBTable(tableName = "UserModel")
public class UserModel {
    /**
     * Partition key
     */
    @DynamoDBHashKey(attributeName = "id")
    private String id;
    /**
     * Range key
     */
    @DynamoDBRangeKey(attributeName = "gender")
    private String gender;

    @DynamoDBAttribute(attributeName = "name")
    private String name;

    @DynamoDBAttribute(attributeName = "age")
    private Integer age;
}
