import 'source-map-support/register'

import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda'
import * as middy from 'middy'
import { cors, httpErrorHandler } from 'middy/middlewares'

import { TodoAccess } from "../../helpers/dataLayer";
import { createAttachmentPresignedUrl } from '../../businessLogic/todos'
import { getUserId } from '../utils'

const todoAccess = new TodoAccess()

const bucketName = process.env.ATTACHMENT_S3_BUCKET;

export const handler = middy(
  async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
    const todoId = event.pathParameters.todoId
    const userId = getUserId(event)
    // TODO: Return a presigned URL to upload a file for a TODO item with the provided id
    const todoExist = await todoAccess.getTodoUsingTodoIdAndUserId(todoId, userId)

    if(!todoExist){
      return {
        statusCode: 404,
        body: JSON.stringify({
          message: 'Todo not found'
        })
      }
    }
    
    const attachmentUrl: string = `https://${bucketName}.s3.amazonaws.com/${todoId}`

    await todoAccess.updateTodoImageAttribute(todoId, userId, attachmentUrl)

    const url = await createAttachmentPresignedUrl(todoId);


    return {
      statusCode: 201,
      body: JSON.stringify({
        uploadUrl: url,
      }),
    };
  }
)

handler
  .use(httpErrorHandler())
  .use(
    cors({
      credentials: true
    })
  )
