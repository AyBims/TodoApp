import * as AWS from 'aws-sdk'
import * as AWSXRay from 'aws-xray-sdk'
import { DocumentClient } from 'aws-sdk/clients/dynamodb'
import { createLogger } from '../utils/logger'
import { TodoItem } from '../models/TodoItem'
// import { TodoUpdate } from '../models/TodoUpdate';
import { UpdateTodoRequest } from "../requests/UpdateTodoRequest";

const XAWS = AWSXRay.captureAWS(AWS)

const logger = createLogger('TodosAccess')
const todoDBIndex = process.env.TODOS_CREATED_AT_INDEX;

// TODO: Implement the dataLayer logic
export class TodoAccess {
    constructor(
      private readonly docClient: DocumentClient = createDynamoDBClient(),
      private readonly todosTable = process.env.TODOS_TABLE
    ) {}
  
    async getAllUserTodos(userId: string): Promise<TodoItem[]> {
      logger.info("Getting all user todos");
  
      const result = await this.docClient
        .query({
          TableName: this.todosTable,
          // IndexName : userId,
          KeyConditionExpression: "userId = :userId",
          ExpressionAttributeValues: {
            ":userId": userId,
          },
        })
        .promise();
  
      const items = result.Items;
      return items as TodoItem[];
    }
  
    async getTodoById(todoId: string): Promise<TodoItem> {
      logger.info("Getting todo by Id");
  
      const result = await this.docClient
        .query({
          TableName: this.todosTable,
          IndexName: todoDBIndex,
          KeyConditionExpression: "todoId = :todoId",
          ExpressionAttributeValues: {
            ":todoId": todoId,
          },
        })
        .promise();
  
      const items = result.Items;
  
      if (items.length !== 0) {
        return items[0] as TodoItem;
      }
    }
  
    async createTodo(todo: TodoItem): Promise<TodoItem> {
      logger.info("Creating todo item for user");
      await this.docClient
        .put({
          TableName: this.todosTable,
          Item: todo,
        })
        .promise();
  
      return todo;
    }
  
    async getTodoUsingTodoIdAndUserId(todoId: string, userId: string) {
      logger.info("Getting todo item for user: ", { userId: userId });
  
      const result = await this.docClient
        .get({
          TableName: this.todosTable,
          Key: { todoId: todoId, userId: userId },
        })
        .promise();
  
      return !!result.Item;
    }
  
    async updateTodo(
      todoId: string,
      userId: string,
      updatedTodo: UpdateTodoRequest
    ) {
      logger.info("Updating todo item", { todoId });
  
      await this.docClient
        .update({
          TableName: this.todosTable,
          Key: { todoId: todoId, userId: userId },
          UpdateExpression:
            "set #name = :name, #dueDate = :dueDate, #done = :done",
          ExpressionAttributeNames: {
            "#name": "name",
            "#dueDate": "dueDate",
            "#done": "done",
          },
          ExpressionAttributeValues: {
            ":name": updatedTodo.name,
            ":dueDate": updatedTodo.dueDate,
            ":done": updatedTodo.done,
          },
        })
        .promise();
    }
  
  
    async updateTodoImageAttribute(todoId: string, userId: string, attachmentUrl: string) {
      logger.info("Updating todo image index");
  
      await this.docClient
        .update({
          TableName: this.todosTable,
          Key: { todoId: todoId, userId: userId },
          UpdateExpression:
            "set #attachmentUrl = :attachmentUrl",
          ExpressionAttributeNames: {
            "#attachmentUrl": "attachmentUrl",
          },
          ExpressionAttributeValues: {
            ":attachmentUrl": attachmentUrl,
          },
        })
        .promise();
    }
  
    async deleteTodo(
      todoId: string,
      userId: string,
    ) {
      logger.info("Deleting todo item", { todoId });
  
      await this.docClient
        .delete({
          TableName: this.todosTable,
          Key: { todoId: todoId, userId: userId },
        })
        .promise();
    }
}
  
function createDynamoDBClient() {
    return new XAWS.DynamoDB.DocumentClient();
}
  