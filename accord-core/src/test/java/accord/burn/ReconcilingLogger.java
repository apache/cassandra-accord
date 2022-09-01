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

package accord.burn;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class ReconcilingLogger
{
    public static class Session extends AppenderBase<ILoggingEvent> implements AutoCloseable
    {
        private final int id;
        private final Logger logger;
        private final List<String> messages = new ArrayList<>();

        public Session(int id, Logger logger)
        {
            this.id = id;
            this.logger = logger;
            setName("ReconcilingAppender{" + this.id + '}');
            start();
            logger.addAppender(this);
        }

        @Override
        protected synchronized void append(ILoggingEvent eventObject)
        {
            messages.add(eventObject.getFormattedMessage());
        }

        int size()
        {
            return messages.size();
        }

        @Override
        public void close()
        {
            logger.detachAppender(this);
        }

        String messageAt(int idx)
        {
            return idx < messages.size() ? messages.get(idx) : "<no message>";
        }
    }

    private final Logger logger;
    private final List<Session> sessions = new ArrayList<>();

    public ReconcilingLogger(org.slf4j.Logger logger)
    {
        this.logger = (Logger) logger;
        ((Logger) logger).setLevel(Level.DEBUG);
    }

    public Session nextSession()
    {
        Session session = new Session(sessions.size() + 1, logger);
        sessions.add(session);
        return session;
    }

    public boolean reconcile()
    {
        if (sessions.size() < 2)
        {
            logger.info("can't reconcile {} sessions", sessions.size());
            return true;
        }

        Session check = sessions.get(0);

        List<Session> mismatches = new ArrayList<>();
        for (int i=1; i<sessions.size(); i++)
        {
            Session session = sessions.get(i);
            if (check.messages.equals(session.messages))
                continue;

            if (mismatches.isEmpty())
                mismatches.add(check);

            mismatches.add(session);
        }

        if (mismatches.isEmpty())
        {
            logger.info("All session logs match");
            return true;
        }

        int size = mismatches.stream().max(Comparator.comparing(Session::size)).get().size();

        logger.error("Mismatched session logs");
        for (int i=0; i<size; i++)
        {
            for (int j=0; j<mismatches.size(); j++)
            {
                Session session = mismatches.get(j);
                logger.error("{} Session {}: {}", i, session.id, session.messageAt(i));
            }
        }

        return false;
    }
}
