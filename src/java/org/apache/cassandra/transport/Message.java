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
package org.apache.cassandra.transport;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.transport.messages.*;
import org.apache.cassandra.service.QueryState;

/**
 * A message from the CQL binary protocol.
 */
public abstract class Message
{
    protected static final Logger logger = LoggerFactory.getLogger(Message.class);

    /**
     * When we encounter an unexpected IOException we look for these {@link Throwable#getMessage() messages}
     * (because we have no better way to distinguish) and log them at DEBUG rather than INFO, since they
     * are generally caused by unclean client disconnects rather than an actual problem.
     */
    private static final Set<String> ioExceptionsAtDebugLevel = ImmutableSet.<String>builder().
            add("Connection reset by peer").
            add("Broken pipe").
            add("Connection timed out").
            build();

    public interface Codec<M extends Message> extends CBCodec<M> {}

    public enum Direction
    {
        REQUEST, RESPONSE;

        public static Direction extractFromVersion(int versionWithDirection)
        {
            return (versionWithDirection & 0x80) == 0 ? REQUEST : RESPONSE;
        }

        public int addToVersion(int rawVersion)
        {
            return this == REQUEST ? (rawVersion & 0x7F) : (rawVersion | 0x80);
        }
    }

    public enum Type
    {
        ERROR          (0,  Direction.RESPONSE, ErrorMessage.codec),
        STARTUP        (1,  Direction.REQUEST,  StartupMessage.codec),
        READY          (2,  Direction.RESPONSE, ReadyMessage.codec),
        AUTHENTICATE   (3,  Direction.RESPONSE, AuthenticateMessage.codec),
        CREDENTIALS    (4,  Direction.REQUEST,  CredentialsMessage.codec),
        OPTIONS        (5,  Direction.REQUEST,  OptionsMessage.codec),
        SUPPORTED      (6,  Direction.RESPONSE, SupportedMessage.codec),
        QUERY          (7,  Direction.REQUEST,  QueryMessage.codec),
        RESULT         (8,  Direction.RESPONSE, ResultMessage.codec),
        PREPARE        (9,  Direction.REQUEST,  PrepareMessage.codec),
        EXECUTE        (10, Direction.REQUEST,  ExecuteMessage.codec),
        REGISTER       (11, Direction.REQUEST,  RegisterMessage.codec),
        EVENT          (12, Direction.RESPONSE, EventMessage.codec),
        BATCH          (13, Direction.REQUEST,  BatchMessage.codec),
        AUTH_CHALLENGE (14, Direction.RESPONSE, AuthChallenge.codec),
        AUTH_RESPONSE  (15, Direction.REQUEST,  AuthResponse.codec),
        AUTH_SUCCESS   (16, Direction.RESPONSE, AuthSuccess.codec);

        public final int opcode;
        public final Direction direction;
        public final Codec<?> codec;

        private static final Type[] opcodeIdx;
        static
        {
            int maxOpcode = -1;
            for (Type type : Type.values())
                maxOpcode = Math.max(maxOpcode, type.opcode);
            opcodeIdx = new Type[maxOpcode + 1];
            for (Type type : Type.values())
            {
                if (opcodeIdx[type.opcode] != null)
                    throw new IllegalStateException("Duplicate opcode");
                opcodeIdx[type.opcode] = type;
            }
        }

        private Type(int opcode, Direction direction, Codec<?> codec)
        {
            this.opcode = opcode;
            this.direction = direction;
            this.codec = codec;
        }

        public static Type fromOpcode(int opcode, Direction direction)
        {
            if (opcode >= opcodeIdx.length)
                throw new ProtocolException(String.format("Unknown opcode %d", opcode));
            Type t = opcodeIdx[opcode];
            if (t == null)
                throw new ProtocolException(String.format("Unknown opcode %d", opcode));
            if (t.direction != direction)
                throw new ProtocolException(String.format("Wrong protocol direction (expected %s, got %s) for opcode %d (%s)",
                                                          t.direction,
                                                          direction,
                                                          opcode,
                                                          t));
            return t;
        }
    }

    public final Type type;
    protected volatile Connection connection;
    private volatile int streamId;

    protected Message(Type type)
    {
        this.type = type;
    }

    public void attach(Connection connection)
    {
        this.connection = connection;
    }

    public Connection connection()
    {
        return connection;
    }

    public Message setStreamId(int streamId)
    {
        this.streamId = streamId;
        return this;
    }

    public int getStreamId()
    {
        return streamId;
    }

    public static abstract class Request extends Message
    {
        protected boolean tracingRequested;

        protected Request(Type type)
        {
            super(type);

            if (type.direction != Direction.REQUEST)
                throw new IllegalArgumentException();
        }

        public abstract Response execute(QueryState queryState);

        public void setTracingRequested()
        {
            this.tracingRequested = true;
        }

        public boolean isTracingRequested()
        {
            return tracingRequested;
        }
    }

    public static abstract class Response extends Message
    {
        protected UUID tracingId;

        protected Response(Type type)
        {
            super(type);

            if (type.direction != Direction.RESPONSE)
                throw new IllegalArgumentException();
        }

        public Message setTracingId(UUID tracingId)
        {
            this.tracingId = tracingId;
            return this;
        }

        public UUID getTracingId()
        {
            return tracingId;
        }
    }

    public static class ProtocolDecoder extends OneToOneDecoder
    {
        public Object decode(ChannelHandlerContext ctx, Channel channel, Object msg)
        {
            assert msg instanceof Frame : "Expecting frame, got " + msg;

            Frame frame = (Frame)msg;
            boolean isRequest = frame.header.type.direction == Direction.REQUEST;
            boolean isTracing = frame.header.flags.contains(Frame.Header.Flag.TRACING);

            UUID tracingId = isRequest || !isTracing ? null : CBUtil.readUUID(frame.body);

            try
            {
                Message message = frame.header.type.codec.decode(frame.body, frame.header.version);
                message.setStreamId(frame.header.streamId);

                if (isRequest)
                {
                    assert message instanceof Request;
                    Request req = (Request)message;
                    req.attach((Connection)channel.getAttachment());
                    if (isTracing)
                        req.setTracingRequested();
                }
                else
                {
                    assert message instanceof Response;
                    if (isTracing)
                        ((Response)message).setTracingId(tracingId);
                }

                return message;
            }
            catch (Exception ex)
            {
                // Remember the streamId
                throw ErrorMessage.wrap(ex, frame.header.streamId);
            }
        }
    }

    public static class ProtocolEncoder extends OneToOneEncoder
    {
        public Object encode(ChannelHandlerContext ctx, Channel channel, Object msg)
        {
            assert msg instanceof Message : "Expecting message, got " + msg;

            Message message = (Message)msg;

            Connection connection = (Connection)channel.getAttachment();
            // The only case the connection can be null is when we send the initial STARTUP message (client side thus)
            int version = connection == null ? Server.CURRENT_VERSION : connection.getVersion();

            EnumSet<Frame.Header.Flag> flags = EnumSet.noneOf(Frame.Header.Flag.class);

            Codec<Message> codec = (Codec<Message>)message.type.codec;
            int messageSize = codec.encodedSize(message, version);
            ChannelBuffer body;
            if (message instanceof Response)
            {
                UUID tracingId = ((Response)message).getTracingId();
                if (tracingId != null)
                {
                    body = ChannelBuffers.buffer(CBUtil.sizeOfUUID(tracingId) + messageSize);
                    CBUtil.writeUUID(tracingId, body);
                    flags.add(Frame.Header.Flag.TRACING);
                }
                else
                {
                    body = ChannelBuffers.buffer(messageSize);
                }
            }
            else
            {
                assert message instanceof Request;
                body = ChannelBuffers.buffer(messageSize);
                if (((Request)message).isTracingRequested())
                    flags.add(Frame.Header.Flag.TRACING);
            }

            codec.encode(message, body, version);
            return Frame.create(message.type, message.getStreamId(), version, flags, body);
        }
    }

    public static class Dispatcher extends SimpleChannelUpstreamHandler
    {
        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
        {
            assert e.getMessage() instanceof Message : "Expecting message, got " + e.getMessage();

            if (e.getMessage() instanceof Response)
                throw new ProtocolException("Invalid response message received, expecting requests");

            Request request = (Request)e.getMessage();

            try
            {
                assert request.connection() instanceof ServerConnection;
                ServerConnection connection = (ServerConnection)request.connection();
                QueryState qstate = connection.validateNewMessage(request.type, connection.getVersion(), request.getStreamId());

                logger.debug("Received: {}, v={}", request, connection.getVersion());

                Response response = request.execute(qstate);
                response.setStreamId(request.getStreamId());
                response.attach(connection);
                connection.applyStateTransition(request.type, response.type);

                logger.debug("Responding: {}, v={}", response, connection.getVersion());

                ctx.getChannel().write(response);
            }
            catch (Throwable ex)
            {
                // Don't let the exception propagate to exceptionCaught() if we can help it so that we can assign the right streamID.
                ctx.getChannel().write(ErrorMessage.fromException(ex, new UnexpectedChannelExceptionHandler(ctx.getChannel(), true)).setStreamId(request.getStreamId()));
            }
        }

        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, ExceptionEvent e)
        throws Exception
        {
            if (ctx.getChannel().isOpen())
            {
                ChannelFuture future = ctx.getChannel().write(ErrorMessage.fromException(e.getCause(), new UnexpectedChannelExceptionHandler(ctx.getChannel(), false)));
                // On protocol exception, close the channel as soon as the message have been sent
                if (e.getCause() instanceof ProtocolException)
                {
                    future.addListener(new ChannelFutureListener() {
                        public void operationComplete(ChannelFuture future) {
                            ctx.getChannel().close();
                        }
                    });
                }
            }
        }
    }

    /**
     * Include the channel info in the logged information for unexpected errors, and (if {@link #alwaysLogAtError} is
     * false then choose the log level based on the type of exception (some are clearly client issues and shouldn't be
     * logged at server ERROR level)
     */
    static final class UnexpectedChannelExceptionHandler implements Predicate<Throwable>
    {
        private final Channel channel;
        private final boolean alwaysLogAtError;

        UnexpectedChannelExceptionHandler(Channel channel, boolean alwaysLogAtError)
        {
            this.channel = channel;
            this.alwaysLogAtError = alwaysLogAtError;
        }

        @Override
        public boolean apply(Throwable exception)
        {
            String message;
            try
            {
                message = "Unexpected exception during request; channel = " + channel;
            }
            catch (Exception ignore)
            {
                // We don't want to make things worse if String.valueOf() throws an exception
                message = "Unexpected exception during request; channel = <unprintable>";
            }

            if (!alwaysLogAtError && exception instanceof IOException)
            {
                if (ioExceptionsAtDebugLevel.contains(exception.getMessage()))
                {
                    // Likely unclean client disconnects
                    logger.debug(message, exception);
                }
                else
                {
                    // Generally unhandled IO exceptions are network issues, not actual ERRORS
                    logger.info(message, exception);
                }
            }
            else
            {
                // Anything else is probably a bug in server of client binary protocol handling
                logger.error(message, exception);
            }

            // We handled the exception.
            return true;
        }
    }
}
