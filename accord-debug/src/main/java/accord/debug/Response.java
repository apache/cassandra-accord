/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package accord.debug;

import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.javalin.http.Context;

public interface Response<T>
{
    public static final Logger LOGGER = LoggerFactory.getLogger(Response.class);
    public static <T> Response<T> compute(Supplier<T> create)
    {
        try
        {
            return Response.success(create.get());
        }
        catch (Throwable t)
        {
            LOGGER.error("Caught an error while computing value", t);
            return failure(t.getMessage());
        }
    }
    boolean isSuccess();
    T getData();
    String getError();
    
    static <T> Response<T> success(T data)
    {
        return new Success<>(data);
    }
    
    static <T> Response<T> failure(String error)
    {
        return new Failure<>(error);
    }
    
    static <T> void sendResponse(Context ctx, Response<T> response)
    {
        if (response.isSuccess())
        {
            ctx.json(response); // TODO: this is not wired through for "debug" project
        }
        else
        {
            ctx.status(500).json(response);
        }
    }
    
    static <T> void sendResponse(Context ctx, Response<T> response, int errorStatus)
    {
        if (response.isSuccess())
        {
            ctx.json(response);
        }
        else
        {
            ctx.status(errorStatus).json(response);
        }
    }
    
    class Success<T> implements Response<T>
    {
        public final T data;
        
        Success(T data)
        {
            this.data = data;
        }
        
        @Override
        public boolean isSuccess()
        {
            return true;
        }
        
        @Override
        public T getData()
        {
            return data;
        }
        
        @Override
        public String getError()
        {
            return null;
        }
    }
    
    class Failure<T> implements Response<T>
    {
        public final String error;
        
        Failure(String error)
        {
            this.error = error;
        }
        
        @Override
        public boolean isSuccess()
        {
            return false;
        }
        
        @Override
        public T getData()
        {
            return null;
        }
        
        @Override
        public String getError()
        {
            return error;
        }
    }
}