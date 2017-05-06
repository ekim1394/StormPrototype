package com.ekim.storm.test;

/**
 * Created by simpl on 5/6/2017.
 */

import com.ekim.storm.ExclamationBolt;
import com.ekim.storm.SimpleTopology;
import org.apache.storm.Config;
import org.apache.storm.Testing;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class StormTest {
    private static final Logger logger = LoggerFactory.getLogger(StormTest.class);

    private ExclamationBolt bolt;
    private OutputCollector collector;
    private TopologyContext context;

    @Before
    public void setUp() throws Exception {
        context = mock(TopologyContext.class);
        collector = mock(OutputCollector.class);
        bolt = new ExclamationBolt();
        bolt.prepare(new Config(), context, collector);
    }

    @Test
    public void testBolt() {
        Tuple input = Testing.testTuple(new Values("Hello"));
        bolt.execute(input);
        verify(collector).emit(input, new Values(input.getString(0) + "!!!"));
    }

    @Test
    public void testTopology() throws Exception {
        SimpleTopology topology = new SimpleTopology();
        SimpleTopology.main(null);
    }
}
