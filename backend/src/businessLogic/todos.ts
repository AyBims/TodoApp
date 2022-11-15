import { TodoAccess } from '../dataLayer/todosAcess'
// import { AttachmentUtils } from './attachmentUtils';
import { TodoItem } from '../models/TodoItem'
import { CreateTodoRequest } from '../requests/CreateTodoRequest'
import { UpdateTodoRequest } from '../requests/UpdateTodoRequest'
// import { createLogger } from '../utils/logger'
import * as uuid from 'uuid';
// import * as createError from 'http-errors'
import { APIGatewayProxyEvent } from "aws-lambda";
import { getUserId } from "../lambda/utils";
import { getUploadUrl } from "../helpers/attachmentUtils";

// TODO: Implement businessLogic

const todoAccess = new TodoAccess();

export async function createTodo(
    event: APIGatewayProxyEvent,
    newTodo: CreateTodoRequest
  ): Promise<TodoItem> {
    const todoId = uuid.v4();
  
    const userId = getUserId(event);
    const todoItem: TodoItem = {
      todoId,
      userId,
      createdAt: new Date().toISOString(),
      name: newTodo.name,
      dueDate: newTodo.dueDate,
      done: false,
      attachmentUrl: "",
    };
  
    return await todoAccess.createTodo(todoItem);
  }
  
  export async function getAllUserTodos(
    event: APIGatewayProxyEvent
  ): Promise<TodoItem[]> {
    const userId = getUserId(event);
    return await todoAccess.getAllUserTodos(userId);
  }
  
  
  export async function createAttachmentPresignedUrl(todoId: string): Promise<string> {
      return getUploadUrl(todoId)
  }
  
  
  export async function updateTodo(todoId:string, event: APIGatewayProxyEvent): Promise<TodoItem> {
    const userId = getUserId(event);
    const updatedTodo: UpdateTodoRequest = JSON.parse(event.body)
  
    // Check if todo created by user exists
    const todo = await todoAccess.getTodoUsingTodoIdAndUserId(todoId, userId)
  
    if (!todo){
      return null
    }
  
  
    await todoAccess.updateTodo(todoId, userId, updatedTodo)
  }
  
  export async function deleteTodo(todoId: string, event: APIGatewayProxyEvent): Promise<TodoItem> {
    const userId = getUserId(event);
    console.log("Attempting to delete a todo", todoId, userId)
  
    // Check if todo created by user exists
    const todo = await todoAccess.getTodoUsingTodoIdAndUserId(todoId, userId)
  
    if (!todo){
      return null
    }
  
    await todoAccess.deleteTodo(todoId, userId)
  }