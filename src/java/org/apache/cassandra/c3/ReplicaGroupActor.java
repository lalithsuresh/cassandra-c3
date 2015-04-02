package org.apache.cassandra.c3;

import akka.actor.UntypedActorWithStash;
import akka.japi.Procedure;
import org.apache.cassandra.service.AbstractReadExecutor;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;


public class ReplicaGroupActor extends UntypedActorWithStash
{

    private final Procedure<Object> WAITING_STATE = new Procedure<Object>()
    {
        @Override
        public void apply(Object msg) throws Exception
        {
            if (msg instanceof ReplicaGroupActorCommand)
            {
                switch ((ReplicaGroupActorCommand) msg)
                {
                    case UNBLOCK:
                        getContext().unbecome();
                        unstashAll();
                        break;
                    default:
                        throw new AssertionError("Received invalid command " + msg);
                }
            }
            else
            {
                stash();
            }
        }
    };

    @Override
    public void onReceive(Object msg)
    {
        if (msg instanceof AbstractReadExecutor)
        {
            long durationToWait = (long) ((AbstractReadExecutor) msg).pushRead();

            if (durationToWait > 0)
            {
                stash();
                switchToWaiting(durationToWait);
            }
        }
    }

    private void switchToWaiting(final long durationToWait)
    {
        System.out.println("Switching to waiting " + durationToWait);
        getContext().become(WAITING_STATE, false);
        getContext().system().scheduler().scheduleOnce(
                                                      Duration.create(durationToWait, TimeUnit.NANOSECONDS),
                                                      getSelf(),
                                                      ReplicaGroupActorCommand.UNBLOCK,
                                                      getContext().system().dispatcher(),
                                                      null);
    }

    enum ReplicaGroupActorCommand
    {
        BLOCK,
        UNBLOCK,
    }
}
