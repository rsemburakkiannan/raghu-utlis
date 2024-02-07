/*
Local dynamoDb connect
<dependency>
    <groupId>com.amazonaws</groupId>
    <artifactId>aws-java-sdk-dynamodb</artifactId>
    <version>1.12.163</version> <!-- Or the latest version -->
</dependency>
*/

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.model.*;

public class DynamoDBLocalSample {

    public static void main(String[] args) {
        // Configure the DynamoDB endpoint to point to DynamoDB Local
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2"))
                .build();
        DynamoDB dynamoDB = new DynamoDB(client);

        // Define table name and schema
        String tableName = "TestTable";

        // Check if the table exists
        boolean tableExists = false;
        try {
            DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(tableName);
            client.describeTable(describeTableRequest);
            tableExists = true;
        } catch (ResourceNotFoundException e) {
            // Table does not exist
        }

        if (!tableExists) {
            // Table doesn't exist, create it
            CreateTableRequest createTableRequest = new CreateTableRequest()
                    .withTableName(tableName)
                    .withKeySchema(new KeySchemaElement().withAttributeName("id").withKeyType(KeyType.HASH))
                    .withAttributeDefinitions(new AttributeDefinition().withAttributeName("id").withAttributeType(ScalarAttributeType.S))
                    .withProvisionedThroughput(new ProvisionedThroughput(5L, 5L));

            Table table = dynamoDB.createTable(createTableRequest);
            try {
                table.waitForActive();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // Put an item into the table
        Table table = dynamoDB.getTable(tableName);
        PutItemSpec putItemSpec = new PutItemSpec()
                .withItem(new Item().withString("id", "1").withString("name", "John Doe").withNumber("age", 30));
        table.putItem(putItemSpec);

        // Query the item
        GetItemSpec getItemSpec = new GetItemSpec().withPrimaryKey("id", "1");
        Item item = table.getItem(getItemSpec);
        System.out.println("Item retrieved: " + item.toJSON());
    }
}

