package com.shao;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class WordCountTopology {
    public static final Logger log = LoggerFactory.getLogger(WordCountTopology.class);

    public static class RandomSentenceSpout extends BaseRichSpout {

        SpoutOutputCollector _collector;
        // 随机数生产对象
        Random _rand;

        /**
         * 对storm进行初始化
         *
         * @param conf
         * @param context
         * @param collector 用于发射数据的对象
         */
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
            _rand = new Random();
        }

        /**
         * tuple数据封装对象.无限个tuple组成的就是一个流
         * 这个spout会运行在某个work进程的某个executor线程中的某个task中。
         * task会一致调用这个方法获取到数据并封装成tuple发射出去
         */
        @Override
        public void nextTuple() {
            Utils.sleep(100);
            String[] sentences = new String[]{sentence("the cow jumped over the moon"), sentence("an apple a day keeps the doctor away"),
                    sentence("four score and seven years ago"), sentence("snow white and the seven dwarfs"), sentence("i am at two with nature")};
            final String sentence = sentences[_rand.nextInt(sentences.length)];
            log.info("RandomSpout发射句子: " + sentence);
            _collector.emit(new Values(sentence));
        }

        protected String sentence(String input) {
            return input;
        }

        @Override
        public void ack(Object id) {
        }

        @Override
        public void fail(Object id) {
        }

        /**
         * 定义每个tuple中value的field的值是什么，类似于value 的 key
         *
         * @param declarer
         */
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("sentence"));
        }

    }

    /**
     * 是spolt处理的下一层
     * 和spolt一样，会运行在某个worker的某个executor的某个task中运行
     */
    public static class SplitSentence extends BaseRichBolt {
        private OutputCollector collector;

        /**
         * bolt第一层处理 设置value的key
         *
         * @param declarer 发射器
         */
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        /**
         * outputCollector是tuple的一个发射器，通过这个对象将tuple发射出去
         */
        @Override
        public void prepare(Map cnf, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.collector = outputCollector;
        }

        /**
         * 每有一个tuple都会在这execute方法中执行
         *
         * @param tuple 数据对象
         */
        @Override
        public void execute(Tuple tuple) {
            final String sentence = tuple.getStringByField("sentence");
            final String[] splits = sentence.split(" ");
            for (String split : splits) {
                log.info("SplitBolt发射单词: " + split);
                collector.emit(new Values(split));
            }
        }
    }

    public static class WordCount extends BaseRichBolt {
        Map<String, Integer> countMap = new HashMap<String, Integer>();
        OutputCollector collector;

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.collector = outputCollector;
        }

        @Override
        public void execute(Tuple tuple) {
            final String word = tuple.getStringByField("word");
            Integer wordVal = countMap.get(word);
            if (wordVal == null) {
                wordVal = 0;
            }
            ++wordVal;
            countMap.put(word, wordVal);
            log.info("WordCountBolt发射单词组: " + word + "----" + wordVal) ;
            collector.emit(new Values(word, wordVal));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        // 第一个参数设置spout的名字；第二个参数设置spout是什么，第三个参数设置spout的executor有几个
        builder.setSpout("RandomSentence", new RandomSentenceSpout(), 2);

        builder.setBolt("SplitSentence", new SplitSentence(), 5).setNumTasks(10).shuffleGrouping("RandomSentence");
        // new Fields("word")表示根据word字段值进行分组，特定的值从SplitSentence进入到特定的WordCount
        builder.setBolt("WordCount", new WordCount(), 10).setNumTasks(20).fieldsGrouping("SplitSentence", new Fields("word"));

        Config conf = new Config();
        conf.setDebug(true);

        // 打算命令行执行并提交到storm集群上去
        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());

            Thread.sleep(10000);

            cluster.shutdown();
        }
    }
}