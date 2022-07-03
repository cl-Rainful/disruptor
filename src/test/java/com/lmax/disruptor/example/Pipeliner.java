package com.lmax.disruptor.example;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;

import java.util.concurrent.TimeUnit;

public class Pipeliner
{
    public static void main(String[] args) throws InterruptedException {
        Disruptor<PipelinerEvent> disruptor = new Disruptor<PipelinerEvent>(
            PipelinerEvent.FACTORY, 1024, DaemonThreadFactory.INSTANCE);

        disruptor.handleEventsWith(
            new ParallelHandler(0, 3),
            new ParallelHandler(1, 3),
            new ParallelHandler(2, 3)
        ).then(new JoiningHandler());

        RingBuffer<PipelinerEvent> ringBuffer = disruptor.start();

        for (int i = 0; i < 10; i++)
        {
            long next = ringBuffer.next();
            try
            {
                PipelinerEvent pipelinerEvent = ringBuffer.get(next);
                pipelinerEvent.input = i;
            }
            finally
            {
                ringBuffer.publish(next);
            }
        }

        TimeUnit.SECONDS.sleep(10);
    }

    private static class ParallelHandler implements EventHandler<PipelinerEvent>
    {
        private final int ordinal;
        private final int totalHandlers;

        ParallelHandler(int ordinal, int totalHandlers)
        {
            this.ordinal = ordinal;
            this.totalHandlers = totalHandlers;
        }

        @Override
        public void onEvent(PipelinerEvent event, long sequence, boolean endOfBatch) throws Exception
        {
            if (sequence % totalHandlers == ordinal)
            {
                event.result = Long.toString(event.input * 10);
            }
        }
    }

    private static class JoiningHandler implements EventHandler<PipelinerEvent>
    {
        private long lastEvent = -1;

        @Override
        public void onEvent(PipelinerEvent event, long sequence, boolean endOfBatch) throws Exception
        {
            if (event.input != lastEvent + 1 || event.result == null)
            {
                System.out.println("Error: " + event);
            }
            System.out.println("event = " + event);
            lastEvent = event.input;
            event.result = null;
        }
    }

    private static class PipelinerEvent
    {
        long input;
        Object result;

        private static final EventFactory<PipelinerEvent> FACTORY = new EventFactory<PipelinerEvent>()
        {
            @Override
            public PipelinerEvent newInstance()
            {
                return new PipelinerEvent();
            }
        };

        @Override
        public String toString()
        {
            return "PipelinerEvent{" +
                "input=" + input +
                ", result=" + result +
                '}';
        }
    }
}
